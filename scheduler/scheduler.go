package scheduler

import (
	"fmt"
	"sync"
	"time"
)

type tenantState struct {
	quota    Resources
	assigned Resources // 该租户所有在队 + 在跑作业的需求之和
}

// jobEntry 保存作业的全部可变运行时状态，只能在 s.mu 下访问。
type jobEntry struct {
	job       *Job
	phase     Phase
	attempt   int
	fence     uint64
	epoch     uint64 // 每次调度唯一递增（fence 跨重试复用，epoch 不会）
	queuedAt  time.Time
	waitSince time.Time // 当前一轮排队等待的起点（用于防饿死老化）
	startedAt time.Time // 本次启动时间（抢占牺牲者排序用）
	readyAt   time.Time // 零值表示立即可调度
	leasedTo  string
	deadline  time.Time
	heartbeat time.Time
	renew     uint64 // 每次心跳续租自增，通知看门狗重新计时
	leaseTok  uint64 // 每次启动/续租换发；旧定时器回调凭令牌自动失效
	failErr   string
	attempts  []Attempt
	notify    chan struct{} // 终态时关闭
}

func (e *jobEntry) effectivePriority(now time.Time, boostPer time.Duration, maxBoost int32) int32 {
	base := e.job.Priority
	if boostPer <= 0 || e.waitSince.IsZero() {
		return base
	}
	waited := now.Sub(e.waitSince)
	boost := int32(waited / boostPer)
	if boost > maxBoost {
		boost = maxBoost
	}
	return base + boost
}

func (e *jobEntry) status() JobStatus {
	s := JobStatus{
		Job:       e.job,
		Phase:     e.phase,
		Attempt:   e.attempt,
		Fence:     e.fence,
		RunningOn: e.leasedTo,
		FailErr:   e.failErr,
		Attempts:  append([]Attempt(nil), e.attempts...),
	}
	return s
}

// Scheduler 是进程内作业调度器。零值不可用，请用 New 创建。
type Scheduler struct {
	cfg   Config
	exec  Executor
	clock Clock

	mu       sync.Mutex
	stopped  bool
	jobs     map[string]*jobEntry
	tenants  map[string]*tenantState
	used     Resources // 集群已占用（所有 Running 作业需求之和）
	totalReq Resources // 所有在队 + 在跑作业需求之和（用于不变量校验）

	// bgWG 跟踪后台 goroutine（退避计时、租约看门狗）。
	bgWG     sync.WaitGroup
	stopOnce sync.Once
	stopCh   chan struct{}
}

// New 创建调度器。exec 为执行器（本工程可用 executor.NewSimulator），
// clock 为时钟（真实运行传 RealClock{}，测试传 *FakeClock）。
func New(cfg Config, exec Executor, clock Clock) *Scheduler {
	cfg.withDefaults()
	if clock == nil {
		clock = RealClock{}
	}
	return &Scheduler{
		cfg:     cfg,
		exec:    exec,
		clock:   clock,
		jobs:    make(map[string]*jobEntry),
		tenants: make(map[string]*tenantState),
	}
}

// AddTenant 注册租户及其配额。必须在提交该租户作业前调用。
func (s *Scheduler) AddTenant(id string, quota Resources) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenants[id] = &tenantState{quota: quota}
}

// UpdateTenantQuota 修改租户配额，立即重新评估排队与抢占。
// 缩容不会杀掉已在运行的作业：超额部分等作业结束或被高优先级抢占时回收；
// 扩容会立刻让排队作业获得调度机会。
func (s *Scheduler) UpdateTenantQuota(id string, quota Resources) error {
	s.mu.Lock()
	t, ok := s.tenants[id]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("scheduler: tenant %q not found", id)
	}
	old := t.quota
	t.quota = quota
	s.logInfo("tenant quota updated", "tenant", id, "old", old, "new", quota)
	s.scheduleLocked()
	s.mu.Unlock()
	return nil
}

// Submit 提交作业。
// queueIfBusy 为 true 时，租户当前额度不足但请求不超配额上限会排队；
// 为 false 时立即返回原因 RejectTenantQuotaFull 的 *SubmitError。
// 返回非 nil 错误表示作业未被受理（没有 JobID 生效）。
func (s *Scheduler) Submit(j *Job, queueIfBusy bool) error {
	if j == nil || j.ID == "" || j.TenantID == "" {
		return newSubmitError(RejectInvalidRequest, "job id and tenant id are required")
	}
	if j.Request.CPUMillis < 0 || j.Request.MemoryMiB < 0 {
		return newSubmitError(RejectInvalidRequest, "resource request must be non-negative")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return newSubmitError(RejectStopped, "scheduler is stopped")
	}
	if _, exists := s.jobs[j.ID]; exists {
		return newSubmitError(RejectInvalidRequest, "duplicate job id: "+j.ID)
	}
	t, ok := s.tenants[j.TenantID]
	if !ok {
		return newSubmitError(RejectTenantUnknown, "tenant not registered: "+j.TenantID)
	}
	if !Fits(j.Request, s.cfg.Capacity) {
		return newSubmitError(RejectExceedsCluster,
			fmt.Sprintf("request %v exceeds cluster capacity %v", j.Request, s.cfg.Capacity))
	}
	if !Fits(j.Request, t.quota) {
		return newSubmitError(RejectExceedsTenantQuota,
			fmt.Sprintf("request %v exceeds tenant quota %v", j.Request, t.quota))
	}
	if !Fits(t.assigned.Add(j.Request), t.quota) {
		if !queueIfBusy {
			return newSubmitError(RejectTenantQuotaFull,
				fmt.Sprintf("tenant %s quota busy: assigned %v + request %v > quota %v",
					j.TenantID, t.assigned, j.Request, t.quota))
		}
	}

	now := s.clock.Now()
	entry := &jobEntry{
		job:       j,
		phase:     PhasePending,
		queuedAt:  now,
		waitSince: now,
		notify:    make(chan struct{}),
	}
	s.jobs[j.ID] = entry
	t.assigned = t.assigned.Add(j.Request)
	s.totalReq = s.totalReq.Add(j.Request)
	s.logInfo("job accepted", "job", j.ID, "tenant", j.TenantID,
		"priority", j.Priority, "request", j.Request)

	s.scheduleLocked()
	return nil
}

// Cancel 请求取消作业，幂等。
//   - 作业尚在排队：立即进入 Canceled 终态并释放租户额度。
//   - 作业正在运行：向执行方发出撤销信号；作业终态由"取消"与"完成"
//     谁先在调度器锁内落账决定，保证成功/失败/取消只落一个。
//   - 作业已终态或不存在：返回对应错误，不产生任何副作用。
func (s *Scheduler) Cancel(id string) error {
	s.mu.Lock()
	e, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return ErrJobNotFound
	}
	if e.phase.IsTerminal() {
		s.mu.Unlock()
		return ErrAlreadyTerminal
	}

	if e.phase == PhasePending {
		s.finishLocked(e, PhaseCanceled, "", "CANCELED", "")
		s.scheduleLocked()
		s.mu.Unlock()
		return nil
	}

	// Running：撤销执行方。结果与取消在 completeLocked 中原子裁决。
	fence := e.fence
	s.mu.Unlock()
	s.logInfo("cancel requested for running job", "job", id, "fence", fence)
	s.exec.Cancel(id)
	return nil
}

// Status 返回作业状态快照；不存在时返回 ErrJobNotFound。
func (s *Scheduler) Status(id string) (JobStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.jobs[id]
	if !ok {
		return JobStatus{}, ErrJobNotFound
	}
	return e.status(), nil
}

// Done 返回在作业进入终态时关闭的通道；作业不存在时返回 nil。
// 重复调用同一作业返回同一通道，可被多个 goroutine 同时等待。
func (s *Scheduler) Done(id string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.jobs[id]; ok {
		return e.notify
	}
	return nil
}

// Stop 关闭调度器：撤销所有在跑执行方、取消所有排队作业，
// 并等待后台 goroutine 退出。Stop 之后 Submit 返回 RejectStopped。
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.stopOnce.Do(func() { s.stopCh = make(chan struct{}) })
	close(s.stopCh)
	running := make([]string, 0)
	for _, e := range s.jobs {
		if e.phase == PhasePending {
			s.finishLocked(e, PhaseCanceled, "", "CANCELED", "scheduler stopped")
		} else if e.phase == PhaseRunning {
			running = append(running, e.job.ID)
		}
	}
	s.mu.Unlock()

	for _, id := range running {
		s.exec.Cancel(id)
	}
	s.bgWG.Wait()
}

// CheckInvariants 校验资源账本一致性，供测试与诊断使用。
// 任意时刻：s.used == 所有 Running 作业需求之和；
// 每个租户 assigned == 其全部非终态作业需求之和。
func (s *Scheduler) CheckInvariants() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkInvariantsLocked()
}

// Used 返回当前集群已占用资源（即所有 Running 作业需求之和）的快照。
func (s *Scheduler) Used() (Resources, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used, true
}

func (s *Scheduler) checkInvariantsLocked() error {
	var wantUsed Resources
	tenantWant := make(map[string]Resources)
	var totalWant Resources
	for _, e := range s.jobs {
		if e.phase == PhaseRunning {
			wantUsed = wantUsed.Add(e.job.Request)
		}
		if !e.phase.IsTerminal() {
			tenantWant[e.job.TenantID] = tenantWant[e.job.TenantID].Add(e.job.Request)
			totalWant = totalWant.Add(e.job.Request)
		}
	}
	if wantUsed != s.used {
		return fmt.Errorf("cluster ledger mismatch: used=%v want=%v", s.used, wantUsed)
	}
	if totalWant != s.totalReq {
		return fmt.Errorf("total ledger mismatch: totalReq=%v want=%v", s.totalReq, totalWant)
	}
	for id, t := range s.tenants {
		if t.assigned != tenantWant[id] {
			return fmt.Errorf("tenant %s ledger mismatch: assigned=%v want=%v",
				id, t.assigned, tenantWant[id])
		}
	}
	return nil
}

func (s *Scheduler) leaseTTL(j *Job) time.Duration {
	if j.LeaseTTL > 0 {
		return j.LeaseTTL
	}
	return s.cfg.DefaultLeaseTTL
}

func (s *Scheduler) logInfo(msg string, args ...any) {
	if s.cfg.Logger != nil {
		s.cfg.Logger.Info(msg, args...)
	}
}

func (s *Scheduler) logWarn(msg string, args ...any) {
	if s.cfg.Logger != nil {
		s.cfg.Logger.Warn(msg, args...)
	}
}

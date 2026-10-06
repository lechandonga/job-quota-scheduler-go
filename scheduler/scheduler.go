// Package scheduler 实现一个进程内的本地作业调度器：
// 租户配额、优先级抢占、失败重试、租约回收与防饥饿老化。
// 只依赖标准库，时间由手动时钟驱动，保证测试确定性。
package scheduler

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Executor 是调度器对执行方的抽象。Start 必须是非阻塞的：
// 它登记一次执行并立即返回执行方标识，执行结果通过
// Scheduler.ReportResult 异步回报。
type Executor interface {
	Start(job Job, epoch int, leaseDeadline time.Time) (workerID string)
}

// Stopper 是 Executor 的可选接口。调度器在取消/抢占时会调用 Stop
// 通知执行方停止；执行方可以忽略（模拟失联的旧执行方）。
type Stopper interface {
	Stop(jobID string, epoch int)
}

// Config 是调度器配置。
type Config struct {
	// Capacity 是集群总容量。
	Capacity Resources
	// LeaseDuration 是每次派发的租约时长；到期未心跳续约即回收。
	LeaseDuration time.Duration
	// AgingInterval 是防饥饿老化间隔：每等待一个间隔，有效优先级 +1。
	// 为 0 表示关闭老化。
	AgingInterval time.Duration
	// Now 是手动时钟的初始时间；为零值时使用固定的默认起点。
	Now time.Time
}

type jobState struct {
	job           Job
	state         State
	attempts      int
	epoch         int
	worker        string
	leaseDeadline time.Time
	firstQueuedAt time.Time
	reason        string
	seq           int
}

// effectivePriority = 基础优先级 + 老化加成。老化加成只在排队等待时
// 累积（运行中的作业不参与），用于防止低优先级作业被饿死。
func (js *jobState) effectivePriority(now time.Time, aging time.Duration) int {
	p := js.job.Priority
	if aging > 0 && js.state == StatePending {
		p += int(now.Sub(js.firstQueuedAt) / aging)
	}
	return p
}

// Scheduler 是调度器主体。所有导出方法都持有同一把互斥锁，
// 每次状态变更后同步触发一轮调度，因此行为是确定、可解释的。
type Scheduler struct {
	mu     sync.Mutex
	cfg    Config
	now    time.Time
	exec   Executor
	quotas map[string]Resources
	jobs   map[string]*jobState
	queue  []*jobState
	seq    int

	usedCluster  Resources
	usedByTenant map[string]Resources

	events []Event
}

func NewScheduler(cfg Config, exec Executor) *Scheduler {
	now := cfg.Now
	if now.IsZero() {
		now = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return &Scheduler{
		cfg:          cfg,
		now:          now,
		exec:         exec,
		quotas:       map[string]Resources{},
		jobs:         map[string]*jobState{},
		usedByTenant: map[string]Resources{},
	}
}

// SetQuota 设置（或调整）租户配额。配额统计该租户 Pending+Running
// 作业的资源总和。调整后立即触发一轮调度。
func (s *Scheduler) SetQuota(tenant string, limit Resources) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.quotas[tenant] = limit
	s.log("", "quota-set", fmt.Sprintf("tenant=%s limit=%s", tenant, limit))
	s.scheduleLocked()
}

// Submit 提交作业。配额不足时：QueueOnQuota 为 true 则排队等待，
// 否则返回 ErrQuotaExceeded；两种路径可明确区分。
func (s *Scheduler) Submit(job Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job.ID == "" || job.Tenant == "" || job.Need.IsZero() {
		return fmt.Errorf("%w: id/tenant/need required", ErrInvalidJob)
	}
	if _, ok := s.jobs[job.ID]; ok {
		return fmt.Errorf("%w: %s", ErrJobExists, job.ID)
	}
	if !job.Need.FitsIn(s.cfg.Capacity) {
		return fmt.Errorf("%w: need=%s capacity=%s", ErrExceedsCapacity, job.Need, s.cfg.Capacity)
	}
	js := &jobState{job: job, state: StatePending, firstQueuedAt: s.now}
	s.seq++
	js.seq = s.seq
	if limit, ok := s.quotas[job.Tenant]; ok &&
		!s.usedByTenant[job.Tenant].Add(job.Need).FitsIn(limit) {
		if !job.QueueOnQuota {
			return fmt.Errorf("%w: tenant=%s need=%s used=%s limit=%s",
				ErrQuotaExceeded, job.Tenant, job.Need, s.usedByTenant[job.Tenant], limit)
		}
		js.reason = "queued: tenant quota"
	}
	s.jobs[job.ID] = js
	s.queue = append(s.queue, js)
	s.usedByTenant[job.Tenant] = s.usedByTenant[job.Tenant].Add(job.Need)
	s.log(job.ID, "submitted", fmt.Sprintf("tenant=%s prio=%d need=%s", job.Tenant, job.Priority, job.Need))
	s.scheduleLocked()
	return nil
}

// Cancel 幂等取消作业。返回是否本次调用真正改变了状态；
// 对已终态的作业再次取消返回 (false, nil)。
func (s *Scheduler) Cancel(id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	js, ok := s.jobs[id]
	if !ok {
		return false, fmt.Errorf("%w: %s", ErrJobNotFound, id)
	}
	if js.state.Terminal() {
		return false, nil
	}
	if js.state == StateRunning {
		s.stopExecutionLocked(js)
		s.usedCluster = s.usedCluster.Sub(js.job.Need)
	} else {
		s.removeFromQueueLocked(js)
	}
	s.usedByTenant[js.job.Tenant] = s.usedByTenant[js.job.Tenant].Sub(js.job.Need)
	js.state = StateCanceled
	js.reason = "canceled"
	s.log(id, "canceled", "")
	s.scheduleLocked()
	return true, nil
}

// Status 返回作业快照。
func (s *Scheduler) Status(id string) (JobStatus, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	js, ok := s.jobs[id]
	if !ok {
		return JobStatus{}, false
	}
	return s.statusLocked(js), true
}

// Queue 返回当前排队作业的快照（按调度顺序）。
func (s *Scheduler) Queue() []JobStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	ordered := s.sortedQueueLocked()
	out := make([]JobStatus, 0, len(ordered))
	for _, js := range ordered {
		out = append(out, s.statusLocked(js))
	}
	return out
}

// Ledger 返回资源账本：集群已用/总量与各租户已用。
func (s *Scheduler) Ledger() (used, capacity Resources, byTenant map[string]Resources) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := map[string]Resources{}
	for t, u := range s.usedByTenant {
		cp[t] = u
	}
	return s.usedCluster, s.cfg.Capacity, cp
}

// Events 返回调度事件日志的拷贝。
func (s *Scheduler) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...)
}

// Advance 推进手动时钟，回收所有已过期租约并重新调度。
func (s *Scheduler) Advance(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = s.now.Add(d)
	for _, js := range s.jobs {
		if js.state == StateRunning && !js.leaseDeadline.After(s.now) {
			s.log(js.job.ID, "lease-expired",
				fmt.Sprintf("worker=%s epoch=%d", js.worker, js.epoch))
			s.requeueLocked(js, "lease expired")
		}
	}
	s.scheduleLocked()
}

// Heartbeat 续约租约。只有当前持有租约的 (epoch, worker) 才能续约成功。
func (s *Scheduler) Heartbeat(jobID string, epoch int, workerID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	js, ok := s.jobs[jobID]
	if !ok || js.state != StateRunning || js.epoch != epoch || js.worker != workerID {
		return false
	}
	js.leaseDeadline = s.now.Add(s.cfg.LeaseDuration)
	return true
}

// ReportResult 由执行方回报执行结果。只有与当前 (state, epoch, worker)
// 完全匹配的结果才被接受；迟到的旧结果会被忽略并记录事件，
// 不会覆盖新的状态。
func (s *Scheduler) ReportResult(jobID string, epoch int, workerID string, runErr error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	js, ok := s.jobs[jobID]
	if !ok || js.state != StateRunning || js.epoch != epoch || js.worker != workerID {
		s.log(jobID, "stale-result-ignored",
			fmt.Sprintf("worker=%s epoch=%d", workerID, epoch))
		return false
	}
	if runErr == nil {
		s.usedCluster = s.usedCluster.Sub(js.job.Need)
		s.usedByTenant[js.job.Tenant] = s.usedByTenant[js.job.Tenant].Sub(js.job.Need)
		js.state = StateSucceeded
		js.reason = "succeeded"
		s.log(jobID, "succeeded", fmt.Sprintf("attempt=%d", js.attempts))
	} else if js.attempts <= js.job.MaxRetries {
		s.requeueLocked(js, fmt.Sprintf("retry after failure: %v", runErr))
	} else {
		s.usedCluster = s.usedCluster.Sub(js.job.Need)
		s.usedByTenant[js.job.Tenant] = s.usedByTenant[js.job.Tenant].Sub(js.job.Need)
		js.state = StateFailed
		js.reason = fmt.Sprintf("failed: %v", runErr)
		s.log(jobID, "failed", fmt.Sprintf("attempts=%d err=%v", js.attempts, runErr))
	}
	s.scheduleLocked()
	return true
}

// CheckInvariants 从作业状态重算账本并与计数器比对，
// 用于验证"账本与实际执行占用一致、不泄漏、不重复计数"。
func (s *Scheduler) CheckInvariants() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var running Resources
	byTenant := map[string]Resources{}
	seen := map[string]bool{}
	for _, js := range s.queue {
		if js.state != StatePending {
			return fmt.Errorf("invariant: job %s in queue but state=%s", js.job.ID, js.state)
		}
		if seen[js.job.ID] {
			return fmt.Errorf("invariant: job %s duplicated in queue", js.job.ID)
		}
		seen[js.job.ID] = true
	}
	for _, js := range s.jobs {
		switch js.state {
		case StateRunning:
			running = running.Add(js.job.Need)
			byTenant[js.job.Tenant] = byTenant[js.job.Tenant].Add(js.job.Need)
			if js.worker == "" || js.epoch == 0 {
				return fmt.Errorf("invariant: running job %s missing worker/epoch", js.job.ID)
			}
		case StatePending:
			byTenant[js.job.Tenant] = byTenant[js.job.Tenant].Add(js.job.Need)
		}
	}
	if running != s.usedCluster {
		return fmt.Errorf("invariant: cluster ledger %s != running sum %s", s.usedCluster, running)
	}
	for t, u := range byTenant {
		if s.usedByTenant[t] != u {
			return fmt.Errorf("invariant: tenant %s ledger %s != sum %s", t, s.usedByTenant[t], u)
		}
	}
	for t, u := range s.usedByTenant {
		if byTenant[t] != u {
			return fmt.Errorf("invariant: tenant %s ledger %s != sum %s", t, u, byTenant[t])
		}
	}
	return nil
}

// ---- 内部实现（调用方须持有锁）----

func (s *Scheduler) log(jobID, kind, detail string) {
	s.events = append(s.events, Event{At: s.now, Kind: kind, JobID: jobID, Detail: detail})
}

func (s *Scheduler) statusLocked(js *jobState) JobStatus {
	return JobStatus{
		ID: js.job.ID, Tenant: js.job.Tenant, State: js.state,
		Attempt: js.attempts, Epoch: js.epoch, Worker: js.worker, Reason: js.reason,
	}
}

func (s *Scheduler) removeFromQueueLocked(js *jobState) {
	for i, q := range s.queue {
		if q == js {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			return
		}
	}
}

// requeueLocked 把 Running 的作业收回队列：作废当前 epoch、
// 释放集群占用（租户占用不变，因为 Pending 也计入配额）。
func (s *Scheduler) requeueLocked(js *jobState, reason string) {
	s.stopExecutionLocked(js)
	s.usedCluster = s.usedCluster.Sub(js.job.Need)
	js.state = StatePending
	js.reason = "queued: " + reason
	js.worker = ""
	s.queue = append(s.queue, js)
	s.log(js.job.ID, "requeued", reason)
}

func (s *Scheduler) stopExecutionLocked(js *jobState) {
	js.epoch++ // 作废旧 epoch，旧执行方的迟到结果将被忽略
	if st, ok := s.exec.(Stopper); ok {
		st.Stop(js.job.ID, js.epoch-1)
	}
}

func (s *Scheduler) sortedQueueLocked() []*jobState {
	ordered := append([]*jobState(nil), s.queue...)
	sort.SliceStable(ordered, func(i, j int) bool {
		pi := ordered[i].effectivePriority(s.now, s.cfg.AgingInterval)
		pj := ordered[j].effectivePriority(s.now, s.cfg.AgingInterval)
		if pi != pj {
			return pi > pj
		}
		return ordered[i].seq < ordered[j].seq
	})
	return ordered
}

// scheduleLocked 按（有效优先级降序、提交序号升序）尝试派发队列中的作业；
// 资源不足时尝试抢占有效优先级更低的运行中作业。
func (s *Scheduler) scheduleLocked() {
	for _, js := range s.sortedQueueLocked() {
		if limit, ok := s.quotas[js.job.Tenant]; ok &&
			!s.usedByTenant[js.job.Tenant].FitsIn(limit) {
			js.reason = "queued: tenant quota"
			continue
		}
		free := s.cfg.Capacity.Sub(s.usedCluster)
		if !js.job.Need.FitsIn(free) {
			s.preemptLocked(js)
			free = s.cfg.Capacity.Sub(s.usedCluster)
			if !js.job.Need.FitsIn(free) {
				js.reason = "queued: cluster capacity"
				continue
			}
		}
		s.dispatchLocked(js)
	}
}

// preemptLocked 为 pending 作业 js 抢占有效优先级严格更低的运行中作业，
// 直到资源足够或无可抢占。被抢占作业回到队列，不消耗重试次数。
func (s *Scheduler) preemptLocked(js *jobState) {
	prio := js.effectivePriority(s.now, s.cfg.AgingInterval)
	var victims []*jobState
	for _, other := range s.jobs {
		if other.state == StateRunning &&
			other.effectivePriority(s.now, s.cfg.AgingInterval) < prio {
			victims = append(victims, other)
		}
	}
	sort.SliceStable(victims, func(i, j int) bool {
		pi := victims[i].effectivePriority(s.now, s.cfg.AgingInterval)
		pj := victims[j].effectivePriority(s.now, s.cfg.AgingInterval)
		if pi != pj {
			return pi < pj
		}
		return victims[i].seq > victims[j].seq
	})
	free := s.cfg.Capacity.Sub(s.usedCluster)
	for _, v := range victims {
		if js.job.Need.FitsIn(free) {
			return
		}
		s.log(v.job.ID, "preempted",
			fmt.Sprintf("by=%s victimPrio=%d preemptorPrio=%d", js.job.ID,
				v.effectivePriority(s.now, s.cfg.AgingInterval), prio))
		s.requeueLocked(v, "preempted")
		free = s.cfg.Capacity.Sub(s.usedCluster)
	}
}

func (s *Scheduler) dispatchLocked(js *jobState) {
	s.removeFromQueueLocked(js)
	js.state = StateRunning
	js.attempts++
	js.epoch++
	js.leaseDeadline = s.now.Add(s.cfg.LeaseDuration)
	js.reason = ""
	js.worker = s.exec.Start(js.job, js.epoch, js.leaseDeadline)
	s.usedCluster = s.usedCluster.Add(js.job.Need)
	s.log(js.job.ID, "dispatched",
		fmt.Sprintf("worker=%s epoch=%d attempt=%d", js.worker, js.epoch, js.attempts))
}

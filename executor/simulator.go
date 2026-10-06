// Package executor 提供 scheduler.Executor 的进程内确定性模拟实现。
//
// 模拟执行器不启动真实进程或容器：每次尝试是一个 goroutine，
// 行为由可编程的 Spec 决定（成功/失败若干次/挂死丢心跳/运行时撤销）。
// 脚本化尝试通过 OnStart 回调把 *Attempt 交给测试，再由测试显式
// Succeed/Fail 决定结局，因此并发用例不需要 sleep 碰运气。
package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lechandonga/job-quota-scheduler-go/scheduler"
)

// Mode 描述一次尝试的结局方式。
type Mode int

const (
	// ModeAutoSucceed 启动后立即回报成功。
	ModeAutoSucceed Mode = iota
	// ModeAutoFail 启动后立即按 Spec.Err 回报失败。
	ModeAutoFail
	// ModeScript 不自动结束，由测试通过 Attempt.Succeed/Fail 显式驱动；
	// 被撤销时回报 ErrExecutionCanceled。
	ModeScript
	// ModeHang 永不结束、忽略撤销（模拟假死 worker），只能靠租约过期回收。
	ModeHang
)

// Spec 是单个作业的模拟脚本。
type Spec struct {
	// Modes[第N次尝试-1] 决定第 N 次尝试；越界用 Default。
	Modes   []Mode
	Default Mode
	Err     error
	// AutoDelay 自动模式回报前的真实等待，默认 0。
	AutoDelay time.Duration
}

// Attempt 表示一次正在进行的模拟尝试，供测试显式驱动。
type Attempt struct {
	JobID string
	Fence uint64

	sim   *Simulator
	entry *runningAttempt
}

// Worker 返回承载该尝试的模拟 worker 名称。
func (a *Attempt) Worker() string { return a.entry.worker }

type runningAttempt struct {
	jobID   string
	fence   uint64
	epoch   uint64
	worker  string
	cancel  context.CancelFunc
	done    func(scheduler.Result)
	revoked bool

	// settled 保证一次尝试至多回报一个结果，且 Revoke 与显式完成
	// 并发时只有一个赢家。
	settled chan struct{}
}

// Simulator 是 scheduler.Executor 的模拟实现。
type Simulator struct {
	// OnStart 在每次尝试的 goroutine 就绪后同步调用（Run 返回之前）。
	// 测试拿到 *Attempt 后可以精确编排完成/心跳/撤销的顺序。
	OnStart func(*Attempt)

	mu        sync.Mutex
	specs     map[string]Spec
	active    map[string]*runningAttempt
	nextID    int
	hanging   sync.WaitGroup
	closeOnce sync.Once
	closeMu   sync.Mutex
	closed    bool
	closeCond *sync.Cond
}

// New 创建模拟执行器。
func New() *Simulator {
	s := &Simulator{
		specs:  make(map[string]Spec),
		active: make(map[string]*runningAttempt),
	}
	s.closeCond = sync.NewCond(&s.closeMu)
	return s
}

// SetSpec 为作业配置模拟脚本（提交前设置即可）。
func (s *Simulator) SetSpec(jobID string, spec Spec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.specs[jobID] = spec
}

// Run 实现 scheduler.Executor；立即返回，工作在 goroutine 中。
func (s *Simulator) Run(j *scheduler.Job, lease scheduler.Lease,
	deadline time.Time, done func(scheduler.Result)) string {
	s.mu.Lock()
	s.nextID++
	worker := fmt.Sprintf("worker-%d", s.nextID)
	spec := s.specs[j.ID]
	attemptIndex := int(lease.Fence()) - 1
	s.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	ra := &runningAttempt{
		jobID:   j.ID,
		fence:   lease.Fence(),
		epoch:   lease.Epoch(),
		worker:  worker,
		cancel:  cancel,
		done:    done,
		settled: make(chan struct{}),
	}
	a := &Attempt{JobID: j.ID, Fence: lease.Fence(), sim: s, entry: ra}

	s.mu.Lock()
	s.active[key(j.ID, lease.Fence())] = ra
	s.mu.Unlock()

	mode := spec.Default
	if attemptIndex < len(spec.Modes) {
		mode = spec.Modes[attemptIndex]
	}

	go func() {
		if s.OnStart != nil {
			s.OnStart(a)
		}
		switch mode {
		case ModeAutoSucceed, ModeAutoFail:
			if spec.AutoDelay > 0 {
				t := time.NewTimer(spec.AutoDelay)
				select {
				case <-t.C:
				case <-ctx.Done():
					t.Stop()
					s.report(ra, scheduler.ErrExecutionCanceled)
					return
				}
			}
			if mode == ModeAutoSucceed {
				s.report(ra, nil)
			} else if spec.Err != nil {
				s.report(ra, spec.Err)
			} else {
				s.report(ra, fmt.Errorf("simulated failure"))
			}
		case ModeScript:
			<-ctx.Done()
			s.report(ra, scheduler.ErrExecutionCanceled)
		case ModeHang:
			s.hanging.Add(1)
			defer s.hanging.Done()
			// 故意忽略 ctx，模拟撤销信号丢失的假死 worker。
			s.waitClosed()
		}
	}()

	return worker
}

// Revoke 撤销某次尝试：脚本化/等待中的尝试会回报取消；
// 已经在 report 临界区决出结果的尝试不受影响（由调度器 fence 再挡）。
func (s *Simulator) Revoke(jobID string, fence uint64) {
	s.mu.Lock()
	ra := s.active[key(jobID, fence)]
	s.mu.Unlock()
	if ra != nil {
		s.revoke(ra)
	}
}

// cancel 供调度器 Cancel 使用：撤销该作业当前所有活动尝试（正常仅一个）。
func (s *Simulator) Cancel(jobID string) {
	s.mu.Lock()
	var all []*runningAttempt
	for _, ra := range s.active {
		if ra.jobID == jobID {
			all = append(all, ra)
		}
	}
	s.mu.Unlock()
	for _, ra := range all {
		s.revoke(ra)
	}
}

func (s *Simulator) revoke(ra *runningAttempt) {
	s.mu.Lock()
	if !ra.revoked {
		ra.revoked = true
		s.mu.Unlock()
		ra.cancel()
		return
	}
	s.mu.Unlock()
}

// report 是所有自动/撤销结果的唯一出口。一个尝试最多回报一次：
// 若撤销先发生，结果一律改写为 ErrExecutionCanceled；显式成功先发生
// 则成功算数。这与调度器端的 fence/终态裁决构成双保险。
func (s *Simulator) report(ra *runningAttempt, err error) {
	s.mu.Lock()
	select {
	case <-ra.settled:
		s.mu.Unlock()
		return // 已经有结果在途
	default:
		close(ra.settled)
	}
	delete(s.active, key(ra.jobID, ra.fence))
	if ra.revoked && err != scheduler.ErrExecutionCanceled {
		err = scheduler.ErrExecutionCanceled
	}
	done := ra.done
	s.mu.Unlock()

	done(scheduler.Result{Fence: ra.fence, Epoch: ra.epoch, Err: err})
}

// Succeed 让脚本化尝试成功。若撤销先到，实际回报的是取消。
func (a *Attempt) Succeed() { a.sim.report(a.entry, nil) }

// Fail 让脚本化尝试以 err 失败。若撤销先到，实际回报的是取消。
func (a *Attempt) Fail(err error) {
	if err == nil {
		err = fmt.Errorf("simulated failure")
	}
	a.sim.report(a.entry, err)
}

// ActiveAttempt 返回作业当前活动尝试（正常最多一个），没有则 nil。
func (s *Simulator) ActiveAttempt(jobID string) *Attempt {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best *runningAttempt
	for _, ra := range s.active {
		if ra.jobID == jobID && (best == nil || ra.fence > best.fence) {
			best = ra
		}
	}
	if best == nil {
		return nil
	}
	return &Attempt{JobID: best.jobID, Fence: best.fence, sim: s, entry: best}
}

// Close 唤醒所有 ModeHang 的 goroutine，应在测试结束时调用，可重复调用。
func (s *Simulator) Close() {
	s.closeOnce.Do(func() {
		s.closeMu.Lock()
		s.closed = true
		s.closeCond.Broadcast()
		s.closeMu.Unlock()
	})
	s.hanging.Wait()
}

func (s *Simulator) waitClosed() {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	for !s.closed {
		s.closeCond.Wait()
	}
}

func key(jobID string, fence uint64) string {
	return fmt.Sprintf("%s#%d", jobID, fence)
}

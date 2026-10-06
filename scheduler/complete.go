package scheduler

import (
	"errors"
	"time"
)

// ErrExecutionCanceled 由执行方在被撤销后回报，调度器据此把作业判为
// Canceled 而非 Failed。它必须携带发起执行时的 fence。
var ErrExecutionCanceled = errors.New("execution revoked")

// complete 是所有执行尝试结果的唯一入口，在 Executor 的 goroutine 中调用，
// 内部用同一把锁与取消/抢占/租约回收串行化，因此终态裁决是原子的。
func (s *Scheduler) complete(jobID string, r Result) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.jobs[jobID]
	if !ok {
		return
	}
	// 终态已落，或这是旧执行方的迟到结果（fence 已易主）：丢弃。
	if e.phase.IsTerminal() || r.Fence != e.fence || r.Epoch != e.epoch {
		s.logWarn("stale result dropped", "job", jobID,
			"resultFence", r.Fence, "currentFence", e.fence,
			"resultEpoch", r.Epoch, "currentEpoch", e.epoch, "phase", e.phase.String())
		return
	}
	if e.phase != PhaseRunning {
		s.logWarn("result for non-running job dropped", "job", jobID, "phase", e.phase.String())
		return
	}

	switch {
	case r.Err == nil:
		s.finishLocked(e, PhaseSucceeded, "", outcomeSucceeded, "")
	case errors.Is(r.Err, ErrExecutionCanceled):
		// 取消与完成同时发生时，谁先在锁内落账谁生效。
		s.finishLocked(e, PhaseCanceled, "", outcomeCanceled, "canceled while running")
	default:
		s.handleFailureLocked(e, r.Err.Error(), outcomeFailed)
	}
	s.scheduleLocked()
}

// handleFailureLocked 处理一次失败/租约丢失：
// 未超预算则退避后重新排队；超过上限判 Failed（唯一失败终态）。
func (s *Scheduler) handleFailureLocked(e *jobEntry, errText, outcome string) {
	now := s.clock.Now()
	maxAttempts := e.job.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	s.recordAttemptEndLocked(e, now, outcome, "", errText)
	s.used = s.used.Sub(e.job.Request)
	e.leasedTo = ""
	e.deadline = time.Time{}

	if e.attempt >= maxAttempts {
		e.phase = PhaseFailed
		e.failErr = errText
		t := s.tenants[e.job.TenantID]
		t.assigned = t.assigned.Sub(e.job.Request)
		s.totalReq = s.totalReq.Sub(e.job.Request)
		close(e.notify)
		s.logWarn("job terminal", "job", e.job.ID, "phase", PhaseFailed.String(),
			"attempts", e.attempt, "err", errText)
		return
	}

	backoff := RetryPolicy{Initial: e.job.InitialBackoff, Max: e.job.MaxBackoff}.
		Backoff(e.attempt)
	e.phase = PhasePending
	e.readyAt = now.Add(backoff)
	e.waitSince = now.Add(backoff)
	// 旧执行方的迟到结果由 epoch 丢弃；fence 在下次 dispatch 时递增。
	s.logInfo("job retry scheduled", "job", e.job.ID,
		"attempt", e.attempt, "backoff", backoff, "err", errText)

	epoch := e.epoch
	// 注册在持锁状态完成；FakeClock 下回调在后续 Advance 内同步执行。
	s.clockWait(backoff, func() { s.onRetryWake(e.job.ID, epoch) })
}

func (s *Scheduler) onRetryWake(jobID string, epoch uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.jobs[jobID]
	if !ok || e.phase.IsTerminal() || e.epoch != epoch {
		return
	}
	if !e.readyAt.IsZero() && s.clock.Now().Before(e.readyAt) {
		return
	}
	e.readyAt = time.Time{}
	s.scheduleLocked()
}

// Heartbeat 由执行方周期性调用以续租。
// 返回 false 表示租约已失效（作业不存在、已终态、fence 过期），
// 执行方必须立刻停止，其之后的任何结果都会被丢弃。
func (s *Scheduler) Heartbeat(jobID string, fence uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.jobs[jobID]
	if !ok || e.phase != PhaseRunning || fence != e.fence {
		return false
	}
	grace := e.heartbeat.Add(s.leaseTTL(e.job)).Add(s.cfg.HeartbeatTimeout)
	if s.clock.Now().After(grace) {
		return false
	}
	e.heartbeat = s.clock.Now()
	e.deadline = e.heartbeat.Add(s.leaseTTL(e.job))
	e.renew++
	e.leaseTok++
	// 心跳即续租：让在途看门狗通过 renew 变化感知，同时确保即使
	// 在定时器到期边界心跳，也有一个按新 deadline 的回调在等待。
	s.armLeaseWatchdogLocked(e)
	return true
}

// armLeaseWatchdogLocked 在 s.mu 下为当前代次注册租约到期回调。
// 心跳续租会推进 deadline 并使 renew 自增；到期回调发现 renew 已变
// 就按新 deadline 重新注册，否则回收租约。
// 旧代次的回调凭 epoch 识别，绝不作用于新代次。
func (s *Scheduler) armLeaseWatchdogLocked(e *jobEntry) {
	epoch := e.epoch
	token := e.leaseTok
	expiry := e.deadline.Add(s.cfg.HeartbeatTimeout)
	wait := expiry.Sub(s.clock.Now())
	if wait <= 0 {
		wait = time.Nanosecond
	}

	var onTick func()
	onTick = func() {
		s.mu.Lock()
		cur, ok := s.jobs[e.job.ID]
		// 只对登记时的 (epoch, token) 负责；心跳/重新调度/抢占/终态
		// 都会让该回调自动失效，绝不作用于新一代尝试。
		if !ok || cur.phase.IsTerminal() || cur.epoch != epoch ||
			cur.leaseTok != token || cur.phase != PhaseRunning {
			s.mu.Unlock()
			return
		}
		now := s.clock.Now()
		if now.Before(expiry) {
			s.clockWait(expiry.Sub(now), onTick)
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		s.reapLease(e.job.ID, epoch)
	}
	s.clockWait(wait, onTick)
}

// reapLease 尝试回收 epoch 对应的租约；返回 true 表示作业已终态。
func (s *Scheduler) reapLease(jobID string, epoch uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.jobs[jobID]
	if !ok {
		return true
	}
	if e.phase.IsTerminal() {
		return true
	}
	if e.phase != PhaseRunning || e.epoch != epoch {
		return false
	}
	if s.clock.Now().Before(e.deadline.Add(s.cfg.HeartbeatTimeout)) {
		return false
	}
	s.logWarn("lease expired, reclaiming", "job", jobID, "epoch", epoch)
	s.handleFailureLocked(e, "lease expired: worker lost heartbeat", outcomeLeaseLost)
	s.scheduleLocked()
	return false
}

// stopCh 在 Stop 时关闭，用于唤醒真实时钟下的后台等待。懒初始化。
func (s *Scheduler) stoppingChan() <-chan struct{} {
	s.stopOnce.Do(func() { s.stopCh = make(chan struct{}) })
	return s.stopCh
}

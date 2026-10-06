package scheduler

import "time"

// outcome 常量用于 Attempt.Outcome。
const (
	outcomeSucceeded = "SUCCEEDED"
	outcomeFailed    = "FAILED"
	outcomePreempted = "PREEMPTED"
	outcomeCanceled  = "CANCELED"
	outcomeLeaseLost = "LEASE_LOST"
)

// scheduleLocked 是唯一改变"资源分配"的地方，在 s.mu 下运行。
// 它反复执行：选出最高优先级的就绪作业，能直接跑就跑，跑不下就尝试抢占，
// 直到一轮中没有任何作业可以再启动。
func (s *Scheduler) scheduleLocked() {
	if s.stopped {
		return
	}
	for {
		cand := s.pickReadyLocked()
		if cand == nil {
			return
		}
		if s.canRunLocked(cand) {
			s.dispatchLocked(cand)
			continue
		}
		if !s.tryPreemptForLocked(cand) {
			// 高优先级也腾不出资源（例如整个租户配额已被同优先级作业占满）：
			// 留在队列，等待老化加成或资源释放。
			return
		}
		if s.canRunLocked(cand) {
			s.dispatchLocked(cand)
			continue
		}
		return
	}
}

// pickReadyLocked 选出当前有效优先级最高的就绪作业。
// 有效优先级 = 静态优先级 + 等待老化加成，因此任何作业的优先级
// 都会随等待时间持续提高，不会被无限期饿死。
func (s *Scheduler) pickReadyLocked() *jobEntry {
	now := s.clock.Now()
	var best *jobEntry
	for _, e := range s.jobs {
		if e.phase != PhasePending || !e.readyAt.IsZero() && e.readyAt.After(now) {
			continue
		}
		if best == nil || s.lessForSchedule(e, best, now) {
			best = e
		}
	}
	return best
}

// lessForSchedule 定义队列序：a 应排在 b 前面返回 true。
// 键依次为：有效优先级降序、排队时间升序（FIFO）、ID 升序（确定性兜底）。
func (s *Scheduler) lessForSchedule(a, b *jobEntry, now time.Time) bool {
	pa := a.effectivePriority(now, s.cfg.StarvationBoost, s.cfg.MaxBoost)
	pb := b.effectivePriority(now, s.cfg.StarvationBoost, s.cfg.MaxBoost)
	if pa != pb {
		return pa > pb
	}
	if !a.queuedAt.Equal(b.queuedAt) {
		return a.queuedAt.Before(b.queuedAt)
	}
	return a.job.ID < b.job.ID
}

func (s *Scheduler) canRunLocked(e *jobEntry) bool {
	t := s.tenants[e.job.TenantID]
	// 租户维度：缩容后可能暂时 assigned > quota，此时连"维持"都做不到，
	// 更不能再启动新作业。
	runningByTenant := s.runningByTenantLocked(e.job.TenantID)
	if !Fits(runningByTenant.Add(e.job.Request), t.quota) {
		return false
	}
	return Fits(s.used.Add(e.job.Request), s.cfg.Capacity)
}

func (s *Scheduler) runningByTenantLocked(tenant string) Resources {
	var r Resources
	for _, e := range s.jobs {
		if e.phase == PhaseRunning && e.job.TenantID == tenant {
			r = r.Add(e.job.Request)
		}
	}
	return r
}

// tryPreemptForLocked 为 cand 选择并撤销一组牺牲者，使得撤销后 cand 能启动。
// 牺牲者必须：正在运行、有效优先级严格低于 cand。按"优先级最低、
// 资源最大、启动最晚、ID 最大"贪心选择，保证选择确定可解释。
// 同租户的可抢占作业优先考虑，因为租户配额是常见瓶颈。
func (s *Scheduler) tryPreemptForLocked(cand *jobEntry) bool {
	now := s.clock.Now()
	candP := cand.effectivePriority(now, s.cfg.StarvationBoost, s.cfg.MaxBoost)

	clusterFree := s.cfg.Capacity.Sub(s.used)
	tenantRunning := s.runningByTenantLocked(cand.job.TenantID)
	tenantQuota := s.tenants[cand.job.TenantID].quota

	type deficit struct {
		cpu, mem int64
	}
	need := deficit{
		cpu: max64(cand.job.Request.CPUMillis-clusterFree.CPUMillis, 0),
		mem: max64(cand.job.Request.MemoryMiB-clusterFree.MemoryMiB, 0),
	}
	tenantRoom := tenantQuota.Sub(tenantRunning)
	needTenant := deficit{
		cpu: max64(cand.job.Request.CPUMillis-tenantRoom.CPUMillis, 0),
		mem: max64(cand.job.Request.MemoryMiB-tenantRoom.MemoryMiB, 0),
	}

	victims := make([]*jobEntry, 0)
	used := make(map[string]bool)
	// 第一轮只选同租户牺牲者，且只需把"租户配额缺口"补齐
	//（同租户牺牲者同时也会释放集群资源）。
	for needTenant.cpu > 0 || needTenant.mem > 0 {
		v := s.pickVictimLocked(candP, now, used, cand.job.TenantID, true)
		if v == nil {
			return false
		}
		used[v.job.ID] = true
		victims = append(victims, v)
		need.cpu -= v.job.Request.CPUMillis
		need.mem -= v.job.Request.MemoryMiB
		needTenant.cpu -= v.job.Request.CPUMillis
		needTenant.mem -= v.job.Request.MemoryMiB
	}
	// 第二轮解决集群容量瓶颈：允许选择任意租户中优先级更低的牺牲者。
	for need.cpu > 0 || need.mem > 0 {
		v := s.pickVictimAnyLocked(candP, now, used)
		if v == nil {
			return false
		}
		used[v.job.ID] = true
		victims = append(victims, v)
		need.cpu -= v.job.Request.CPUMillis
		need.mem -= v.job.Request.MemoryMiB
	}

	for _, v := range victims {
		s.preemptLocked(v, now)
	}
	return true
}

// pickVictimAnyLocked 在所有租户中选择牺牲者（含发起者租户）。
func (s *Scheduler) pickVictimAnyLocked(candP int32, now time.Time,
	used map[string]bool) *jobEntry {
	var best *jobEntry
	for _, e := range s.jobs {
		if e.phase != PhaseRunning || used[e.job.ID] {
			continue
		}
		if e.effectivePriority(now, s.cfg.StarvationBoost, s.cfg.MaxBoost) >= candP {
			continue
		}
		if best == nil || s.lessAsVictim(e, best, now) {
			best = e
		}
	}
	return best
}

func (s *Scheduler) pickVictimLocked(candP int32, now time.Time,
	used map[string]bool, candTenant string, sameTenant bool) *jobEntry {
	var best *jobEntry
	for _, e := range s.jobs {
		if e.phase != PhaseRunning || used[e.job.ID] {
			continue
		}
		isSame := e.job.TenantID == candTenant
		if sameTenant != isSame {
			continue
		}
		if e.effectivePriority(now, s.cfg.StarvationBoost, s.cfg.MaxBoost) >= candP {
			continue
		}
		if best == nil || s.lessAsVictim(e, best, now) {
			best = e
		}
	}
	return best
}

// lessAsVictim：a 比 b 更适合被牺牲（优先级更低、资源更大、启动更晚、ID 更大）。
func (s *Scheduler) lessAsVictim(a, b *jobEntry, now time.Time) bool {
	pa := a.effectivePriority(now, s.cfg.StarvationBoost, s.cfg.MaxBoost)
	pb := b.effectivePriority(now, s.cfg.StarvationBoost, s.cfg.MaxBoost)
	if pa != pb {
		return pa < pb
	}
	sa := a.job.Request.CPUMillis + a.job.Request.MemoryMiB
	sb := b.job.Request.CPUMillis + b.job.Request.MemoryMiB
	if sa != sb {
		return sa > sb
	}
	if !a.startedAt.Equal(b.startedAt) {
		return a.startedAt.After(b.startedAt)
	}
	return a.job.ID > b.job.ID
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// dispatchLocked 启动一次尝试并登记账本。
func (s *Scheduler) dispatchLocked(e *jobEntry) {
	now := s.clock.Now()
	e.fence++ // 首次为 1；重排队时也已递增，因此每次真实执行 fence 唯一
	e.epoch++
	e.attempt++
	e.phase = PhaseRunning
	e.readyAt = time.Time{}
	e.deadline = now.Add(s.leaseTTL(e.job))
	e.heartbeat = now
	e.startedAt = now
	e.leaseTok++

	s.used = s.used.Add(e.job.Request)

	fence := e.fence
	lease := Lease{JobID: e.job.ID, fence: fence, epoch: e.epoch}
	worker := s.exec.Run(e.job, lease, e.deadline, func(r Result) {
		s.complete(e.job.ID, r)
	})
	e.leasedTo = worker

	e.attempts = append(e.attempts, Attempt{
		Fence:   fence,
		Started: now,
		Outcome: "RUNNING",
	})
	s.armLeaseWatchdogLocked(e)
	s.logInfo("job dispatched", "job", e.job.ID, "tenant", e.job.TenantID,
		"fence", fence, "attempt", e.attempt, "worker", worker)
	if err := s.checkInvariantsLocked(); err != nil {
		panic("scheduler invariant broken at dispatch: " + err.Error())
	}
}

// preemptLocked 撤销牺牲者：旧 fence 立即失效，作业回到 Pending 队列，
// 资源立刻归还账本（不等执行方真正停下）。执行方的迟到结果会被
// fence 检查丢弃，因此不会双重记账。
func (s *Scheduler) preemptLocked(e *jobEntry, now time.Time) {
	fence := e.fence
	s.recordAttemptEndLocked(e, now, outcomePreempted, "preempted by higher priority job", "")
	s.used = s.used.Sub(e.job.Request)
	e.phase = PhasePending
	e.readyAt = time.Time{} // 抢占立即重新排队，不消耗重试预算，不退避
	e.waitSince = now
	e.leasedTo = ""
	e.deadline = time.Time{}
	s.logInfo("job preempted", "job", e.job.ID, "fence", fence)
	s.exec.Revoke(e.job.ID, fence)
}

// finishLocked 让作业进入终态：关闭通知通道、归还租户额度与集群占用、
// 从调度集合移除。幂等性由调用前的 phase 检查保证。
func (s *Scheduler) finishLocked(e *jobEntry, phase Phase, failErr, outcome, note string) {
	now := s.clock.Now()
	if e.phase == PhaseRunning {
		s.recordAttemptEndLocked(e, now, outcome, note, failErr)
		s.used = s.used.Sub(e.job.Request)
	} else {
		s.recordAttemptEndLocked(e, now, outcome, note, failErr)
	}
	e.phase = phase
	e.failErr = failErr
	e.leasedTo = ""
	e.deadline = time.Time{}

	t := s.tenants[e.job.TenantID]
	t.assigned = t.assigned.Sub(e.job.Request)
	s.totalReq = s.totalReq.Sub(e.job.Request)
	close(e.notify)
	s.logInfo("job terminal", "job", e.job.ID, "phase", phase.String(),
		"attempts", e.attempt)
}

func (s *Scheduler) recordAttemptEndLocked(e *jobEntry, at time.Time, outcome, note, errText string) {
	for i := len(e.attempts) - 1; i >= 0; i-- {
		if e.attempts[i].Outcome == "RUNNING" {
			e.attempts[i].Ended = at
			e.attempts[i].Outcome = outcome
			e.attempts[i].Err = errText
			return
		}
	}
	_ = note
}

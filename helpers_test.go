package bootstrap_test

import (
	"testing"
	"time"

	"github.com/lechandonga/job-quota-scheduler-go/executor"
	"github.com/lechandonga/job-quota-scheduler-go/scheduler"
	"github.com/lechandonga/job-quota-scheduler-go/testutil"
)

func res(cpu, mem int64) scheduler.Resources {
	return scheduler.Resources{CPUMillis: cpu, MemoryMiB: mem}
}

type harness struct {
	t     *testing.T
	sched *scheduler.Scheduler
	sim   *executor.Simulator
	clock *scheduler.FakeClock
	log   *testutil.ScenarioLogger
}

func newHarness(t *testing.T, capacity scheduler.Resources, log *testutil.ScenarioLogger) *harness {
	t.Helper()
	start := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	clock := scheduler.NewFakeClock(start)
	sim := executor.New()
	cfg := scheduler.Config{
		Capacity:        capacity,
		DefaultLeaseTTL: 10 * time.Second,
		StarvationBoost: 10 * time.Second,
		Logger:          log,
	}
	s := scheduler.New(cfg, sim, clock)
	t.Cleanup(func() {
		s.Stop()
		sim.Close()
	})
	return &harness{t: t, sched: s, sim: sim, clock: clock, log: log}
}

func (h *harness) submit(j *scheduler.Job, queue bool) error {
	h.t.Helper()
	err := h.sched.Submit(j, queue)
	if err != nil {
		h.log.Step("提交作业 %s 被拒绝: %v", j.ID, err)
	} else {
		h.log.Step("提交作业 %s（租户=%s 优先级=%d 需求=%v 排队=%t）",
			j.ID, j.TenantID, j.Priority, j.Request, queue)
	}
	return err
}

func (h *harness) phase(id string) scheduler.Phase {
	h.t.Helper()
	st, err := h.sched.Status(id)
	if err != nil {
		h.t.Fatalf("Status(%s): %v", id, err)
	}
	return st.Phase
}

func (h *harness) expectPhase(id string, want scheduler.Phase) {
	h.t.Helper()
	got := h.phase(id)
	h.log.Check(got == want, "作业 %s 终态/阶段 = %s，期望 %s", id, got, want)
	if got != want {
		h.t.Errorf("job %s phase = %s, want %s", id, got, want)
	}
}

func (h *harness) expectAttempts(id string, want int) {
	h.t.Helper()
	st, _ := h.sched.Status(id)
	h.log.Check(st.Attempt == want, "作业 %s 发起尝试次数 = %d，期望 %d", id, st.Attempt, want)
	if st.Attempt != want {
		h.t.Errorf("job %s attempts = %d, want %d", id, st.Attempt, want)
	}
}

func (h *harness) expectRejectReason(err error, want scheduler.RejectReason) {
	h.t.Helper()
	se, ok := scheduler.IsSubmitError(err)
	got := scheduler.RejectReason("")
	if ok {
		got = se.Reason
	}
	h.log.Check(ok && got == want, "拒绝原因 = %s，期望 %s（err=%v）", got, want, err)
	if !ok || got != want {
		h.t.Errorf("reject reason = %v, want %s (err=%v)", got, want, err)
	}
}

func (h *harness) checkLedger(where string) {
	h.t.Helper()
	if err := h.sched.CheckInvariants(); err != nil {
		h.log.Check(false, "资源账本一致 @%s: %v", where, err)
		h.t.Fatalf("invariants broken @%s: %v", where, err)
	}
	used := h.usedView()
	h.log.Check(true, "资源账本一致 @%s（集群占用 cpu=%dm mem=%dMi）",
		where, used.CPUMillis, used.MemoryMiB)
}

func (h *harness) usedView() scheduler.Resources {
	u, _ := h.sched.Used()
	return u
}

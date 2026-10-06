package bootstrap_test

import (
	"testing"
	"time"

	"github.com/lechandonga/job-quota-scheduler-go/executor"
	"github.com/lechandonga/job-quota-scheduler-go/scheduler"
	"github.com/lechandonga/job-quota-scheduler-go/testutil"
)

// TestRetryExhaustion 验证失败重试语义：
// 前两次尝试失败按指数退避重新排队，第三次成功；
// 退避期间作业为 PENDING；时钟推进到退避点后自动重试；
// 始终失败则在 MaxAttempts 用尽后进入唯一失败终态 Failed。
func TestRetryExhaustion(t *testing.T) {
	testutil.Banner(t, "失败重试与退避",
		"作业配置最多 3 次尝试、初始退避 5s（封顶 10s）：\n"+
			"失败后进入退避 PENDING，假时钟推进后重试；第三次成功。\n"+
			"另有作业用尽全部尝试后判 Failed，且账本释放。")
	log := testutil.NewLogger(t, true)
	h := newHarness(t, res(100, 100), log)
	h.sched.AddTenant("t", res(100, 100))

	starts := make(chan *executor.Attempt, 16)
	h.sim.OnStart = func(a *executor.Attempt) { starts <- a }
	h.sim.SetSpec("flaky", executor.Spec{Default: executor.ModeScript})
	if err := h.submit(&scheduler.Job{
		ID: "flaky", TenantID: "t", Request: res(100, 100),
		MaxAttempts: 3, InitialBackoff: 5 * time.Second, MaxBackoff: 10 * time.Second,
	}, false); err != nil {
		t.Fatal(err)
	}
	waitStart := func(id string) {
		for {
			a := <-starts
			if a.JobID == id {
				return
			}
		}
	}
	waitStart("flaky")
	h.expectAttempts("flaky", 1)

	backoffs := []time.Duration{5 * time.Second, 10 * time.Second}
	for attempt, expectedBackoff := range backoffs {
		h.log.Step("让第 %d 次尝试失败", attempt+1)
		h.sim.ActiveAttempt("flaky").Fail(nil)
		waitFor(h, func() bool { return h.phase("flaky") == scheduler.PhasePending })
		h.expectPhase("flaky", scheduler.PhasePending)
		h.log.Step("推进假时钟 %s（第 %d 次失败的退避）", expectedBackoff, attempt+1)
		h.clock.Advance(expectedBackoff)
		waitStart("flaky") // 下一次尝试确定性启动
		h.expectAttempts("flaky", attempt+2)
		h.expectPhase("flaky", scheduler.PhaseRunning)
	}

	h.log.Step("让第 3 次尝试成功")
	h.sim.ActiveAttempt("flaky").Succeed()
	<-h.sched.Done("flaky")
	h.expectPhase("flaky", scheduler.PhaseSucceeded)
	h.expectAttempts("flaky", 3)
	st, _ := h.sched.Status("flaky")
	for i, a := range st.Attempts {
		if i < 2 && a.Outcome != "FAILED" {
			t.Fatalf("attempt %d outcome = %s, want FAILED", i+1, a.Outcome)
		}
	}
	h.log.Check(true, "前两次尝试记录 FAILED，第三次 SUCCEEDED")
	h.checkLedger("重试成功后")

	// 用尽预算 -> Failed
	h.sim.SetSpec("doomed", executor.Spec{Default: executor.ModeScript})
	if err := h.submit(&scheduler.Job{
		ID: "doomed", TenantID: "t", Request: res(100, 100),
		MaxAttempts: 2, InitialBackoff: time.Second,
	}, false); err != nil {
		t.Fatal(err)
	}
	waitStart("doomed")
	h.sim.ActiveAttempt("doomed").Fail(nil)
	waitFor(h, func() bool { return h.phase("doomed") == scheduler.PhasePending })
	h.clock.Advance(time.Second)
	waitStart("doomed")
	h.sim.ActiveAttempt("doomed").Fail(nil)
	<-h.sched.Done("doomed")
	h.expectPhase("doomed", scheduler.PhaseFailed)
	h.expectAttempts("doomed", 2)
	st2, _ := h.sched.Status("doomed")
	if st2.FailErr == "" {
		t.Fatal("failed job must carry failure reason")
	}
	h.log.Check(true, "重试预算耗尽后判 Failed 并携带失败原因: %s", st2.FailErr)
	h.checkLedger("失败终态后")
}

func waitFor(h *harness, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	h.t.Fatal("condition was not satisfied in time")
}

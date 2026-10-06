package bootstrap_test

import (
	"testing"
	"time"

	"github.com/lechandonga/job-quota-scheduler-go/executor"
	"github.com/lechandonga/job-quota-scheduler-go/scheduler"
	"github.com/lechandonga/job-quota-scheduler-go/testutil"
)

// TestPreemption 覆盖高优先级抢占：被抢占作业回到队列不判失败；
// 抢占不消耗重试预算；高优先级结束后被抢占者重新运行；
// 同优先级不得互相抢占。
func TestPreemption(t *testing.T) {
	testutil.Banner(t, "优先级抢占",
		"低优先级占满集群；同优先级新来者只能排队；高优先级到来后抢占，\n"+
			"牺牲者回到 PENDING 且尝试记录为 PREEMPTED；高优先级结束后按 FIFO 恢复。")
	log := testutil.NewLogger(t, true)
	h := newHarness(t, res(1000, 1000), log)
	h.sched.AddTenant("team-a", res(1000, 1000))
	h.sim.SetSpec("low", executor.Spec{Default: executor.ModeScript})

	if err := h.submit(&scheduler.Job{ID: "low", TenantID: "team-a",
		Priority: 1, Request: res(1000, 1000)}, true); err != nil {
		t.Fatal(err)
	}
	h.expectPhase("low", scheduler.PhaseRunning)

	h.sim.SetSpec("same", executor.Spec{Default: executor.ModeScript})
	if err := h.submit(&scheduler.Job{ID: "same", TenantID: "team-a",
		Priority: 1, Request: res(1000, 1000)}, true); err != nil {
		t.Fatal(err)
	}
	h.expectPhase("same", scheduler.PhasePending)
	h.expectPhase("low", scheduler.PhaseRunning)

	h.sim.SetSpec("high", executor.Spec{Default: executor.ModeScript})
	if err := h.submit(&scheduler.Job{ID: "high", TenantID: "team-a",
		Priority: 10, Request: res(1000, 1000)}, true); err != nil {
		t.Fatal(err)
	}
	h.expectPhase("high", scheduler.PhaseRunning)
	h.expectPhase("low", scheduler.PhasePending)
	h.expectAttempts("low", 1)
	st, _ := h.sched.Status("low")
	if len(st.Attempts) != 1 || st.Attempts[0].Outcome != "PREEMPTED" {
		t.Fatalf("low attempts = %+v, want one PREEMPTED record", st.Attempts)
	}
	h.log.Check(true, "被抢占作业回到 PENDING，尝试记录为 PREEMPTED，且不消耗重试预算")
	h.checkLedger("抢占后：账本只计 high")

	// low 与 same 同优先级，low 排队更早，high 释放资源后应先恢复 low。
	h.log.Step("完成 high，low 应先于 same 重新运行（同优先级 FIFO）")
	h.sim.ActiveAttempt("high").Succeed()
	<-h.sched.Done("high")
	h.expectPhase("low", scheduler.PhaseRunning)
	h.expectPhase("same", scheduler.PhasePending)
	h.expectAttempts("low", 2)

	h.log.Step("完成 low，same 运行并成功")
	h.sim.ActiveAttempt("low").Succeed()
	<-h.sched.Done("low")
	h.expectPhase("low", scheduler.PhaseSucceeded)
	h.expectPhase("same", scheduler.PhaseRunning)

	h.sim.ActiveAttempt("same").Succeed()
	<-h.sched.Done("same")
	h.checkLedger("全部结束")
}

// TestPreemptCrossTenant 覆盖跨租户抢占：租户 A 的作业占满集群，
// 租户 B 的高优先级作业可以抢占 A 的低优先级作业。
func TestPreemptCrossTenant(t *testing.T) {
	testutil.Banner(t, "跨租户抢占",
		"集群容量是共享瓶颈时，高优先级作业允许抢占其他租户的低优先级作业；\n"+
			"同租户牺牲者优先，保证抢占最小化波及范围。")
	log := testutil.NewLogger(t, true)
	h := newHarness(t, res(100, 100), log)
	h.sched.AddTenant("a", res(100, 100))
	h.sched.AddTenant("b", res(100, 100))
	h.sim.SetSpec("a-low", executor.Spec{Default: executor.ModeScript})
	h.sim.SetSpec("b-high", executor.Spec{Default: executor.ModeScript})

	if err := h.submit(&scheduler.Job{ID: "a-low", TenantID: "a",
		Priority: 1, Request: res(100, 100)}, true); err != nil {
		t.Fatal(err)
	}
	if err := h.submit(&scheduler.Job{ID: "b-high", TenantID: "b",
		Priority: 9, Request: res(100, 100)}, true); err != nil {
		t.Fatal(err)
	}
	h.expectPhase("a-low", scheduler.PhasePending)
	h.expectPhase("b-high", scheduler.PhaseRunning)
	h.checkLedger("跨租户抢占后")

	h.sim.ActiveAttempt("b-high").Succeed()
	<-h.sched.Done("b-high")
	h.expectPhase("a-low", scheduler.PhaseRunning)
	h.sim.ActiveAttempt("a-low").Succeed()
	<-h.sched.Done("a-low")
	h.checkLedger("全部结束")
}

// TestNoStarvation 验证老化机制：低优先级作业等待足够久后，
// 其有效优先级会超过后到的静态高优先级作业，从而先被调度，
// 不存在被持续高优先级流量无限饿死的可能。
func TestNoStarvation(t *testing.T) {
	testutil.Banner(t, "防饿死（优先级老化）",
		"低优先级作业排队等待超过 StarvationBoost 后获得 +1 加成；\n"+
			"推进假时钟模拟长时间等待，老化作业先于新来的高优先级作业运行。")
	log := testutil.NewLogger(t, true)
	h := newHarness(t, res(100, 100), log)
	h.sched.AddTenant("t", res(100, 100))
	h.sim.SetSpec("hold", executor.Spec{Default: executor.ModeScript})
	h.sim.SetSpec("low", executor.Spec{Default: executor.ModeScript})
	h.sim.SetSpec("new-high", executor.Spec{Default: executor.ModeScript})

	if err := h.submit(&scheduler.Job{ID: "hold", TenantID: "t",
		Priority: 5, Request: res(100, 100)}, true); err != nil {
		t.Fatal(err)
	}
	// low 静态优先级 1，排队等待。
	if err := h.submit(&scheduler.Job{ID: "low", TenantID: "t",
		Priority: 1, Request: res(100, 100)}, true); err != nil {
		t.Fatal(err)
	}
	// 推进时钟 100s（StarvationBoost=10s），low 的有效优先级变为 11。
	h.log.Step("假时钟推进 100s，low 等待老化（1 + 10 = 11）")
	h.clock.Advance(100 * time.Second)

	// 释放 hold；此时再提交静态优先级 5 的新作业，老化后的 low 应先跑。
	h.sim.ActiveAttempt("hold").Succeed()
	<-h.sched.Done("hold")
	if err := h.submit(&scheduler.Job{ID: "new-high", TenantID: "t",
		Priority: 5, Request: res(100, 100)}, true); err != nil {
		t.Fatal(err)
	}
	h.expectPhase("low", scheduler.PhaseRunning)
	h.expectPhase("new-high", scheduler.PhasePending)
	h.log.Check(true, "老化后的低优先级作业先于静态优先级更高的新来者运行")

	h.sim.ActiveAttempt("low").Succeed()
	<-h.sched.Done("low")
	h.expectPhase("new-high", scheduler.PhaseRunning)
	h.sim.ActiveAttempt("new-high").Succeed()
	<-h.sched.Done("new-high")
	h.checkLedger("全部结束")
}

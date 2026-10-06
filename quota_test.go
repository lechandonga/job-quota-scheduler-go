package bootstrap_test

import (
	"testing"

	"github.com/lechandonga/job-quota-scheduler-go/executor"
	"github.com/lechandonga/job-quota-scheduler-go/scheduler"
	"github.com/lechandonga/job-quota-scheduler-go/testutil"
)

// TestQuotaRejection 覆盖提交阶段的配额判定：
// 未知租户、超过租户配额上限、超过集群容量、当前额度不足拒绝/排队，
// 以及排队作业在额度释放后自动调度。
func TestQuotaRejection(t *testing.T) {
	testutil.Banner(t, "配额拒绝与排队",
		"验证不同提交结果有可区分原因；超过上限直接拒绝，额度暂时不足可排队；\n"+
			"在跑作业结束后，排队作业按规则自动出队。")
	log := testutil.NewLogger(t, true)
	h := newHarness(t, res(1000, 1024), log)
	h.sched.AddTenant("team-a", res(600, 512))

	h.sim.SetSpec("j-run", executor.Spec{Default: executor.ModeScript})

	err := h.submit(&scheduler.Job{ID: "j-unknown", TenantID: "ghost",
		Priority: 0, Request: res(1, 1)}, true)
	h.expectRejectReason(err, scheduler.RejectTenantUnknown)

	err = h.submit(&scheduler.Job{ID: "j-too-big", TenantID: "team-a",
		Priority: 0, Request: res(700, 100)}, true)
	h.expectRejectReason(err, scheduler.RejectExceedsTenantQuota)

	err = h.submit(&scheduler.Job{ID: "j-cluster", TenantID: "team-a",
		Priority: 0, Request: res(2000, 100)}, true)
	h.expectRejectReason(err, scheduler.RejectExceedsCluster)

	err = h.submit(&scheduler.Job{ID: "j-run", TenantID: "team-a",
		Priority: 0, Request: res(600, 512)}, true)
	if err != nil {
		t.Fatalf("j-run submit: %v", err)
	}
	h.expectPhase("j-run", scheduler.PhaseRunning)

	err = h.submit(&scheduler.Job{ID: "j-busy-deny", TenantID: "team-a",
		Priority: 0, Request: res(100, 100)}, false)
	h.expectRejectReason(err, scheduler.RejectTenantQuotaFull)

	err = h.submit(&scheduler.Job{ID: "j-queued", TenantID: "team-a",
		Priority: 0, Request: res(100, 100)}, true)
	if err != nil {
		t.Fatalf("j-queued submit: %v", err)
	}
	h.expectPhase("j-queued", scheduler.PhasePending)
	h.checkLedger("排队后")

	h.sim.SetSpec("j-queued", executor.Spec{Default: executor.ModeScript})
	h.log.Step("完成 j-run，释放租户额度")
	h.sim.ActiveAttempt("j-run").Succeed()
	<-h.sched.Done("j-run")
	h.expectPhase("j-run", scheduler.PhaseSucceeded)
	h.expectPhase("j-queued", scheduler.PhaseRunning)
	h.checkLedger("排队作业出队后")

	h.log.Step("完成 j-queued")
	h.sim.ActiveAttempt("j-queued").Succeed()
	<-h.sched.Done("j-queued")
	h.checkLedger("全部结束")
}

// TestTenantQuotaChange 覆盖运行中配额变化：扩容立即放行排队作业；
// 缩容不杀在跑作业，但阻止新调度，直到占用自然降到新额度以下。
func TestTenantQuotaChange(t *testing.T) {
	testutil.Banner(t, "租户配额动态变化",
		"扩容：排队作业立即获得调度；\n缩容：在跑作业继续运行，新作业与排队作业等待额度回落。")
	log := testutil.NewLogger(t, true)
	h := newHarness(t, res(2000, 2048), log)
	h.sched.AddTenant("team-a", res(400, 400))
	h.sim.SetSpec("q1", executor.Spec{Default: executor.ModeScript})
	h.sim.SetSpec("q2", executor.Spec{Default: executor.ModeScript})

	if err := h.submit(&scheduler.Job{ID: "q1", TenantID: "team-a",
		Request: res(400, 400)}, true); err != nil {
		t.Fatal(err)
	}
	if err := h.submit(&scheduler.Job{ID: "q2", TenantID: "team-a",
		Request: res(200, 200)}, true); err != nil {
		t.Fatal(err)
	}
	h.expectPhase("q1", scheduler.PhaseRunning)
	h.expectPhase("q2", scheduler.PhasePending)

	h.log.Step("租户额度扩容到 600/600")
	if err := h.sched.UpdateTenantQuota("team-a", res(600, 600)); err != nil {
		t.Fatal(err)
	}
	h.expectPhase("q2", scheduler.PhaseRunning)
	h.checkLedger("扩容后两作业在跑")

	h.log.Step("租户额度缩容到 300/300（不杀在跑作业），新作业 q3 必须等待")
	if err := h.sched.UpdateTenantQuota("team-a", res(300, 300)); err != nil {
		t.Fatal(err)
	}
	h.sim.SetSpec("q3", executor.Spec{Default: executor.ModeScript})
	if err := h.submit(&scheduler.Job{ID: "q3", TenantID: "team-a",
		Request: res(100, 100)}, true); err != nil {
		t.Fatal(err)
	}
	h.expectPhase("q3", scheduler.PhasePending)

	h.log.Step("结束 q1（释放 400/400），占用降到 q2 的 200/200，q3 放行")
	h.sim.ActiveAttempt("q1").Succeed()
	<-h.sched.Done("q1")
	h.expectPhase("q3", scheduler.PhaseRunning)
	h.checkLedger("缩容后额度回落")

	h.sim.ActiveAttempt("q2").Succeed()
	h.sim.ActiveAttempt("q3").Succeed()
	<-h.sched.Done("q2")
	<-h.sched.Done("q3")
	h.checkLedger("全部结束")
}

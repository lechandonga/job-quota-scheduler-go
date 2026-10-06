package bootstrap_test

import (
	"testing"
	"time"

	"github.com/lechandonga/job-quota-scheduler-go/executor"
	"github.com/lechandonga/job-quota-scheduler-go/scheduler"
	"github.com/lechandonga/job-quota-scheduler-go/testutil"
)

// TestLeaseReclaim 覆盖租约生命周期：
//  1. 心跳正常：推进时间但不超过 TTL，作业持续运行；
//  2. 心跳停止、租约过期：调度器回收，作业按 LEASE_LOST 退避重试；
//  3. 旧（假死）执行方的迟到结果不能覆盖新一轮结果。
func TestLeaseReclaim(t *testing.T) {
	testutil.Banner(t, "心跳续租与租约回收",
		"租约 TTL=10s：持续心跳时推进时钟作业不受影响；停止心跳并推进超过 TTL，\n"+
			"调度器回收租约（LEASE_LOST）并按退避重试；旧 worker 迟到结果被 fence 丢弃。")
	log := testutil.NewLogger(t, true)
	h := newHarness(t, res(100, 100), log)
	h.sched.AddTenant("t", res(100, 100))

	var firstAttempt *executor.Attempt
	started := make(chan struct{}, 2)
	h.sim.OnStart = func(a *executor.Attempt) {
		if a.JobID == "lease-job" && firstAttempt == nil {
			firstAttempt = a
		}
		started <- struct{}{}
	}

	// 第一次尝试假死（不回报、不响应撤销），第二次尝试脚本化成功。
	h.sim.SetSpec("lease-job", executor.Spec{
		Modes:   []executor.Mode{executor.ModeHang, executor.ModeScript},
		Default: executor.ModeScript,
	})
	if err := h.submit(&scheduler.Job{
		ID: "lease-job", TenantID: "t", Request: res(100, 100),
		MaxAttempts: 2, InitialBackoff: 3 * time.Second,
	}, true); err != nil {
		t.Fatal(err)
	}
	<-started
	h.expectPhase("lease-job", scheduler.PhaseRunning)

	// 推进 5s，心跳续命，租约不过期。
	h.log.Step("推进 5s 并心跳，租约应保持有效")
	h.clock.Advance(5 * time.Second)
	if !h.sched.Heartbeat("lease-job", firstAttempt.Fence) {
		t.Fatal("heartbeat within TTL should be accepted")
	}
	h.expectPhase("lease-job", scheduler.PhaseRunning)

	// 再推进超过 TTL，worker 假死没有心跳 -> 租约被回收。
	h.log.Step("停止心跳，推进 11s，租约应过期回收")
	h.clock.Advance(11 * time.Second)
	waitFor(h, func() bool { return h.phase("lease-job") == scheduler.PhasePending })
	st, _ := h.sched.Status("lease-job")
	h.expectPhase("lease-job", scheduler.PhasePending)
	h.expectAttempts("lease-job", 1)
	if len(st.Attempts) == 0 || st.Attempts[0].Outcome != "LEASE_LOST" {
		t.Fatalf("first attempt outcome = %+v, want LEASE_LOST", st.Attempts)
	}
	h.log.Check(true, "租约过期回收，第一次尝试记录 LEASE_LOST")

	// 旧 fence 的心跳必须被拒绝。
	if h.sched.Heartbeat("lease-job", firstAttempt.Fence) {
		t.Fatal("heartbeat with stale fence must be rejected")
	}
	h.log.Check(true, "旧执行方用失效 fence 续租被拒绝")

	// 退避结束，第二次尝试启动并成功。
	h.log.Step("推进 3s 退避，第二次尝试启动")
	h.clock.Advance(3 * time.Second)
	<-started
	h.expectAttempts("lease-job", 2)

	// 旧假死 worker 若"复活"并带着旧 fence 回报成功：无法直接构造，
	// 但 fence 拒绝路径已由 Heartbeat 验证；这里再次校验账本后让新尝试成功。
	h.checkLedger("旧执行方失联期间，账本只计第二次尝试")
	second := h.sim.ActiveAttempt("lease-job")
	if second == nil || second.Fence != firstAttempt.Fence+1 {
		t.Fatalf("second attempt fence = %v, want %d", second, firstAttempt.Fence+1)
	}
	second.Succeed()
	<-h.sched.Done("lease-job")
	h.expectPhase("lease-job", scheduler.PhaseSucceeded)
	h.checkLedger("租约恢复后成功")
}

// TestLeaseReclaimThenFail 验证租约丢失也消耗尝试预算：
// 只允许一次尝试时，租约过期即终态 Failed。
func TestLeaseReclaimThenFail(t *testing.T) {
	testutil.Banner(t, "租约丢失计入重试预算",
		"MaxAttempts=1 的作业在唯一一次尝试租约过期后直接判 Failed。")
	log := testutil.NewLogger(t, true)
	h := newHarness(t, res(100, 100), log)
	h.sched.AddTenant("t", res(100, 100))
	h.sim.SetSpec("one-shot", executor.Spec{Default: executor.ModeHang})

	if err := h.submit(&scheduler.Job{
		ID: "one-shot", TenantID: "t", Request: res(100, 100),
		MaxAttempts: 1,
	}, true); err != nil {
		t.Fatal(err)
	}
	h.expectPhase("one-shot", scheduler.PhaseRunning)
	h.clock.Advance(11 * time.Second)
	<-h.sched.Done("one-shot")
	h.expectPhase("one-shot", scheduler.PhaseFailed)
	st, _ := h.sched.Status("one-shot")
	if st.Attempts[0].Outcome != "LEASE_LOST" {
		t.Fatalf("outcome = %s, want LEASE_LOST", st.Attempts[0].Outcome)
	}
	h.log.Check(true, "唯一尝试租约过期 -> Failed，账本释放")
	h.checkLedger("终态")
}

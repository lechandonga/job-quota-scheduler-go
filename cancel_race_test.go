package bootstrap_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/lechandonga/job-quota-scheduler-go/executor"
	"github.com/lechandonga/job-quota-scheduler-go/scheduler"
	"github.com/lechandonga/job-quota-scheduler-go/testutil"
)

// TestCancelQueued 验证排队作业取消立即生效、释放租户额度，
// 且重复取消幂等。
func TestCancelQueued(t *testing.T) {
	testutil.Banner(t, "排队取消幂等",
		"排队中的作业取消后立即 Canceled 并释放租户额度，后续作业可顶上；\n"+
			"对同一作业重复取消只返回 ErrAlreadyTerminal，不产生副作用。")
	log := testutil.NewLogger(t, true)
	h := newHarness(t, res(100, 100), log)
	h.sched.AddTenant("t", res(100, 100))
	h.sim.SetSpec("a", executor.Spec{Default: executor.ModeScript})
	h.sim.SetSpec("b", executor.Spec{Default: executor.ModeScript})

	if err := h.submit(&scheduler.Job{ID: "a", TenantID: "t",
		Request: res(100, 100)}, true); err != nil {
		t.Fatal(err)
	}
	if err := h.submit(&scheduler.Job{ID: "b", TenantID: "t",
		Request: res(100, 100)}, true); err != nil {
		t.Fatal(err)
	}
	h.expectPhase("b", scheduler.PhasePending)

	h.log.Step("取消排队作业 b")
	if err := h.sched.Cancel("b"); err != nil {
		t.Fatalf("first cancel: %v", err)
	}
	h.expectPhase("b", scheduler.PhaseCanceled)
	<-h.sched.Done("b")

	if err := h.sched.Cancel("b"); !errors.Is(err, scheduler.ErrAlreadyTerminal) {
		t.Fatalf("second cancel err = %v, want ErrAlreadyTerminal", err)
	}
	if err := h.sched.Cancel("b"); !errors.Is(err, scheduler.ErrAlreadyTerminal) {
		t.Fatalf("third cancel err = %v, want ErrAlreadyTerminal", err)
	}
	h.log.Check(true, "重复取消幂等，返回 ErrAlreadyTerminal")

	if err := h.sched.Cancel("missing"); !errors.Is(err, scheduler.ErrJobNotFound) {
		t.Fatalf("cancel missing err = %v, want ErrJobNotFound", err)
	}
	h.checkLedger("排队取消后")

	h.sim.ActiveAttempt("a").Succeed()
	<-h.sched.Done("a")
	h.checkLedger("a 完成后")
}

// TestConcurrentCancelCompletion 高并发压测"取消幂等 + 取消/完成竞争"。
// 所有 goroutine 用同一个 channel barrier 同时放行，竞争确定发生；
// 结果只能是 Succeeded 或 Canceled 二者之一，且尝试恰好一次。
func TestConcurrentCancelCompletion(t *testing.T) {
	testutil.Banner(t, "并发取消与取消/完成竞争",
		"50 个 goroutine 同时取消同一作业，执行方同时回报成功；\n"+
			"重复 60 轮（-race 下同样稳定）：终态唯一、作业只执行一次、账本一致。")
	log := testutil.NewLogger(t, false)

	const rounds = 60
	for round := 0; round < rounds; round++ {
		h := newHarness(t, res(100, 100), log)
		h.sched.AddTenant("t", res(100, 100))
		id := fmt.Sprintf("race-%d", round)
		h.sim.SetSpec(id, executor.Spec{Default: executor.ModeScript})

		var attemptSeen sync.WaitGroup
		attemptSeen.Add(1)
		var gotAttempt *executor.Attempt
		h.sim.OnStart = func(a *executor.Attempt) {
			gotAttempt = a
			attemptSeen.Done()
		}
		if err := h.sched.Submit(&scheduler.Job{ID: id, TenantID: "t",
			Request: res(100, 100)}, false); err != nil {
			t.Fatalf("round %d submit: %v", round, err)
		}
		attemptSeen.Wait()
		h.sim.OnStart = nil

		var wg sync.WaitGroup
		barrier := make(chan struct{})
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-barrier
				_ = h.sched.Cancel(id)
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-barrier
			gotAttempt.Succeed()
		}()
		close(barrier)
		wg.Wait()

		<-h.sched.Done(id)
		st, err := h.sched.Status(id)
		if err != nil {
			t.Fatalf("round %d status: %v", round, err)
		}
		if st.Phase != scheduler.PhaseSucceeded && st.Phase != scheduler.PhaseCanceled {
			t.Fatalf("round %d: unexpected terminal phase %s", round, st.Phase)
		}
		if st.Attempt != 1 {
			t.Fatalf("round %d: expected exactly 1 attempt, got %d", round, st.Attempt)
		}
		if err := h.sched.Cancel(id); !errors.Is(err, scheduler.ErrAlreadyTerminal) {
			t.Fatalf("round %d: repeat cancel err=%v", round, err)
		}
		if err := h.sched.CheckInvariants(); err != nil {
			t.Fatalf("round %d invariants: %v", round, err)
		}
	}
	log.Check(true, "%d 轮并发取消/完成竞争：终态唯一、尝试恰好一次、账本一致", rounds)
}

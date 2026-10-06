package scheduler

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

func newEnv(capacity Resources, lease, aging time.Duration) (*Scheduler, *SimExecutor) {
	exec := NewSimExecutor(nil)
	s := NewScheduler(Config{Capacity: capacity, LeaseDuration: lease, AgingInterval: aging}, exec)
	exec.Bind(s)
	return s, exec
}

func mustInvariants(t *testing.T, s *Scheduler) {
	t.Helper()
	if err := s.CheckInvariants(); err != nil {
		t.Fatalf("invariant violated: %v", err)
	}
}

func mustState(t *testing.T, s *Scheduler, id string, want State) JobStatus {
	t.Helper()
	st, ok := s.Status(id)
	if !ok {
		t.Fatalf("job %s not found", id)
	}
	if st.State != want {
		t.Fatalf("job %s state=%s, want %s (reason=%q)", id, st.State, want, st.Reason)
	}
	return st
}

func dumpEvents(t *testing.T, s *Scheduler) {
	t.Helper()
	for _, e := range s.Events() {
		t.Logf("  event: %s", e)
	}
}

func hasEvent(s *Scheduler, kind, jobID string) bool {
	for _, e := range s.Events() {
		if e.Kind == kind && (jobID == "" || e.JobID == jobID) {
			return true
		}
	}
	return false
}

// 场景 1：租户配额——超配拒绝、排队等待、配额释放/上调后放行。
func TestQuotaRejectAndQueue(t *testing.T) {
	t.Log("scenario: quota reject vs queue-on-quota")
	s, exec := newEnv(Resources{CPU: 8, Memory: 64}, time.Minute, 0)
	s.SetQuota("t1", Resources{CPU: 4, Memory: 32})

	gate := make(chan struct{})
	exec.SetHandler("a", func(ec ExecContext) error { <-gate; return nil })

	if err := s.Submit(Job{ID: "a", Tenant: "t1", Priority: 1, Need: Resources{CPU: 4, Memory: 32}}); err != nil {
		t.Fatalf("submit a: %v", err)
	}
	mustState(t, s, "a", StateRunning)

	err := s.Submit(Job{ID: "b", Tenant: "t1", Priority: 1, Need: Resources{CPU: 1, Memory: 1}})
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("submit b err=%v, want ErrQuotaExceeded", err)
	}
	t.Logf("case reject: b rejected with %v", err)

	if err := s.Submit(Job{ID: "c", Tenant: "t1", Priority: 1, Need: Resources{CPU: 2, Memory: 2}, QueueOnQuota: true}); err != nil {
		t.Fatalf("submit c: %v", err)
	}
	st := mustState(t, s, "c", StatePending)
	t.Logf("case queue: c queued, reason=%q", st.Reason)

	// 配额上调后排队的 c 被放行。
	s.SetQuota("t1", Resources{CPU: 8, Memory: 64})
	mustState(t, s, "c", StateRunning)
	t.Log("case quota-increase: c dispatched after quota raised")

	// 超出集群容量的作业直接拒绝。
	err = s.Submit(Job{ID: "huge", Tenant: "t1", Priority: 1, Need: Resources{CPU: 100, Memory: 1}})
	if !errors.Is(err, ErrExceedsCapacity) {
		t.Fatalf("submit huge err=%v, want ErrExceedsCapacity", err)
	}

	close(gate)
	exec.Wait()
	mustState(t, s, "a", StateSucceeded)
	mustInvariants(t, s)
	dumpEvents(t, s)
}

// 场景 2：并发提交与并发取消——终态唯一、账本一致。
func TestConcurrentSubmitAndCancel(t *testing.T) {
	t.Log("scenario: concurrent submit/cancel, single terminal state")
	s, exec := newEnv(Resources{CPU: 16, Memory: 128}, time.Minute, 0)
	s.SetQuota("t1", Resources{CPU: 64, Memory: 512})
	exec.SetHandler("", nil) // 默认 handler 立即成功

	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("j%02d", i)
			if err := s.Submit(Job{ID: id, Tenant: "t1", Priority: i % 4, Need: Resources{CPU: 1, Memory: 1}}); err != nil {
			}
			if i%2 == 0 {
				s.Cancel(id) // 与执行完成并发竞争
			}
		}(i)
	}
	wg.Wait()
	exec.Wait()

	counts := map[State]int{}
	for i := 0; i < n; i++ {
		st, ok := s.Status(fmt.Sprintf("j%02d", i))
		if !ok {
			t.Fatalf("job j%02d missing", i)
		}
		if !st.State.Terminal() {
			t.Fatalf("job %s not terminal: %s", st.ID, st.State)
		}
		counts[st.State]++
	}
	t.Logf("terminal states: %v", counts)
	mustInvariants(t, s)

	// 取消幂等：重复取消返回 false 且无错误。
	for i := 0; i < n; i += 2 {
		changed, err := s.Cancel(fmt.Sprintf("j%02d", i))
		if err != nil || changed {
			t.Fatalf("re-cancel j%02d: changed=%v err=%v", i, changed, err)
		}
	}
	t.Log("case idempotent-cancel: repeated cancel is a no-op")
	mustInvariants(t, s)
}

// 场景 3：取消与完成同时发生——只允许一个终态。
func TestCancelRacesCompletion(t *testing.T) {
	t.Log("scenario: cancel races completion")
	for round := 0; round < 50; round++ {
		s, exec := newEnv(Resources{CPU: 4, Memory: 4}, time.Minute, 0)
		gate := make(chan struct{})
		exec.SetHandler("x", func(ec ExecContext) error { <-gate; return nil })
		if err := s.Submit(Job{ID: "x", Tenant: "t1", Priority: 1, Need: Resources{CPU: 1, Memory: 1}}); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); close(gate) }()
		go func() { defer wg.Done(); s.Cancel("x") }()
		wg.Wait()
		exec.Wait()
		st := mustState(t, s, "x", st0(s))
		if st.State != StateSucceeded && st.State != StateCanceled {
			t.Fatalf("round %d: unexpected state %s", round, st.State)
		}
		if err := s.CheckInvariants(); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
	}
	t.Log("case race: 50 rounds, exactly one terminal state each")
}

func st0(s *Scheduler) State {
	st, _ := s.Status("x")
	return st.State
}

// 场景 4：优先级抢占——高优先级抢占低优先级，被抢占者回队列后恢复。
func TestPreemption(t *testing.T) {
	t.Log("scenario: preemption and requeue")
	s, exec := newEnv(Resources{CPU: 4, Memory: 4}, time.Minute, 0)

	lowGate := make(chan struct{})
	lowStarted := make(chan struct{}, 2)
	highGate := make(chan struct{})
	exec.SetHandler("low", func(ec ExecContext) error {
		lowStarted <- struct{}{}
		select {
		case <-ec.Done(): // 被抢占时首次执行退出
			return ec.Err()
		case <-lowGate:
			return nil
		}
	})
	exec.SetHandler("high", func(ec ExecContext) error {
		<-highGate
		return nil
	})

	mustSubmit(t, s, Job{ID: "low", Tenant: "t1", Priority: 1, Need: Resources{CPU: 4, Memory: 4}})
	mustState(t, s, "low", StateRunning)
	<-lowStarted

	mustSubmit(t, s, Job{ID: "high", Tenant: "t1", Priority: 9, Need: Resources{CPU: 4, Memory: 4}})
	mustState(t, s, "high", StateRunning)
	st := mustState(t, s, "low", StatePending)
	t.Logf("case preempt: low preempted back to queue, reason=%q", st.Reason)
	if !hasEvent(s, "preempted", "low") {
		t.Fatal("missing preempted event for low")
	}
	mustInvariants(t, s)

	close(highGate)
	<-lowStarted // low 被重新派发（第二次执行），此时 high 的结果已回报
	mustState(t, s, "high", StateSucceeded)
	st = mustState(t, s, "low", StateRunning)
	t.Logf("case resume: low re-dispatched, attempt=%d", st.Attempt)

	close(lowGate)
	exec.Wait()
	mustState(t, s, "low", StateSucceeded)
	mustInvariants(t, s)
	dumpEvents(t, s)
}

// 场景 5：失败重试——上限内重试成功；超限判失败。
func TestRetry(t *testing.T) {
	t.Log("scenario: retry with backoff-free policy")
	s, exec := newEnv(Resources{CPU: 4, Memory: 4}, time.Minute, 0)

	var flakyCalls atomic.Int32
	exec.SetHandler("flaky", func(ec ExecContext) error {
		if flakyCalls.Add(1) <= 2 {
			return errBoom
		}
		return nil
	})
	exec.SetHandler("hopeless", func(ec ExecContext) error { return errBoom })

	mustSubmit(t, s, Job{ID: "flaky", Tenant: "t1", Priority: 1, Need: Resources{CPU: 1, Memory: 1}, MaxRetries: 2})
	mustSubmit(t, s, Job{ID: "hopeless", Tenant: "t1", Priority: 1, Need: Resources{CPU: 1, Memory: 1}, MaxRetries: 1})
	exec.Wait()

	st := mustState(t, s, "flaky", StateSucceeded)
	t.Logf("case retry-success: flaky succeeded after %d attempts", st.Attempt)
	if st.Attempt != 3 {
		t.Fatalf("flaky attempts=%d, want 3", st.Attempt)
	}
	st = mustState(t, s, "hopeless", StateFailed)
	t.Logf("case retry-exhausted: hopeless failed after %d attempts, reason=%q", st.Attempt, st.Reason)
	if st.Attempt != 2 {
		t.Fatalf("hopeless attempts=%d, want 2", st.Attempt)
	}
	mustInvariants(t, s)
	dumpEvents(t, s)
}

// 场景 6：租约回收与迟到结果——心跳续约有效；失联后重新调度；
// 旧执行方的迟到结果被忽略，不覆盖新结果。
func TestLeaseExpiryAndStaleResult(t *testing.T) {
	t.Log("scenario: lease heartbeat, expiry, stale result ignored")
	s, exec := newEnv(Resources{CPU: 4, Memory: 4}, 10*time.Second, 0)

	gate := make(chan struct{})
	exec.SetHandler("job", func(ec ExecContext) error {
		if ec.Epoch == 1 {
			<-gate // 第一次执行“卡住”，模拟失联
			return errBoom
		}
		return nil // 第二次执行成功
	})

	mustSubmit(t, s, Job{ID: "job", Tenant: "t1", Priority: 1, Need: Resources{CPU: 1, Memory: 1}, MaxRetries: 3})
	st := mustState(t, s, "job", StateRunning)

	// 心跳续约：推进 8s 后续约，再推进 8s 租约仍有效。
	s.Advance(8 * time.Second)
	if !exec.Heartbeat("job", st.Epoch, st.Worker) {
		t.Fatal("heartbeat rejected for live lease")
	}
	s.Advance(8 * time.Second)
	mustState(t, s, "job", StateRunning)
	t.Log("case heartbeat: lease renewed, still running at t=16s")

	// 不再心跳，租约在 t=20s 过期，作业被回收并重新调度。
	s.Advance(5 * time.Second)
	st = mustState(t, s, "job", StateRunning)
	if st.Attempt != 2 {
		t.Fatalf("attempt=%d, want 2 after lease expiry", st.Attempt)
	}
	t.Logf("case lease-expiry: re-dispatched as attempt=%d epoch=%d worker=%s", st.Attempt, st.Epoch, st.Worker)
	if !hasEvent(s, "lease-expired", "job") {
		t.Fatal("missing lease-expired event")
	}

	// 旧执行方迟到回报失败：必须被忽略，不能覆盖新执行。
	close(gate)
	exec.Wait()
	st = mustState(t, s, "job", StateSucceeded)
	if st.Attempt != 2 {
		t.Fatalf("final attempt=%d, want 2", st.Attempt)
	}
	if !hasEvent(s, "stale-result-ignored", "job") {
		t.Fatal("missing stale-result-ignored event")
	}
	t.Log("case stale-result: late failure from old worker ignored, job succeeded")
	mustInvariants(t, s)
	dumpEvents(t, s)
}

// 场景 7：防饥饿老化——低优先级作业等待足够久后有效优先级提升，
// 可以抢占长期占用资源的高优先级作业。
func TestAgingPreventsStarvation(t *testing.T) {
	t.Log("scenario: aging prevents starvation")
	s, exec := newEnv(Resources{CPU: 4, Memory: 4}, time.Minute, 10*time.Second)

	gate := make(chan struct{})
	meekReported := make(chan struct{}, 1)
	exec.SetHandler("hog", func(ec ExecContext) error {
		select {
		case <-ec.Done(): // 被抢占时首次执行退出
			return ec.Err()
		case <-gate:
			return nil
		}
	})
	exec.SetHandler("meek", func(ec ExecContext) error { return nil })
	exec.OnReported = func(jobID string) {
		if jobID == "meek" {
			meekReported <- struct{}{}
		}
	}

	mustSubmit(t, s, Job{ID: "hog", Tenant: "t1", Priority: 5, Need: Resources{CPU: 4, Memory: 4}})
	mustSubmit(t, s, Job{ID: "meek", Tenant: "t1", Priority: 0, Need: Resources{CPU: 4, Memory: 4}})
	mustState(t, s, "meek", StatePending)

	// 等待 6 个老化间隔后，meek 有效优先级 0+6=6 > 5，触发抢占。
	s.Advance(60 * time.Second)
	mustState(t, s, "meek", StateRunning)
	mustState(t, s, "hog", StatePending)
	t.Log("case aging: meek preempted hog after aging boost")
	if !hasEvent(s, "preempted", "hog") {
		t.Fatal("missing preempted event for hog")
	}
	<-meekReported
	mustState(t, s, "meek", StateSucceeded)
	// meek 完成后 hog 被重新派发。
	mustState(t, s, "hog", StateRunning)
	close(gate)
	exec.Wait()
	mustState(t, s, "hog", StateSucceeded)
	mustInvariants(t, s)
	dumpEvents(t, s)
}

func mustSubmit(t *testing.T, s *Scheduler, j Job) {
	t.Helper()
	if err := s.Submit(j); err != nil {
		t.Fatalf("submit %s: %v", j.ID, err)
	}
}

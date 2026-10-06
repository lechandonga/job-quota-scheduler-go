package scheduler

import "time"

// timerScheduler 由 FakeClock 实现：回调在 Advance 内同步执行，
// 因此测试推进时钟后所有到期动作都已完成，无需等待 goroutine 调度。
type timerScheduler interface {
	waitTimer(d time.Duration, fn func(now time.Time)) (cancel func())
}

// clockWait 安排 d 之后执行 fn。
//   - FakeClock：fn 在 Advance 调用栈内同步执行，结果确定；
//   - RealClock：新 goroutine 用 time.After 等待，Stop 时可被打断。
//
// 该方法可在持有 s.mu 时调用（FakeClock 路径不会在注册点立即执行 fn；
// 调用方负责在 Advance 前完成状态转换与注册）。
func (s *Scheduler) clockWait(d time.Duration, fn func()) {
	if fc, ok := s.clock.(*FakeClock); ok {
		fc.waitTimer(d, func(time.Time) { fn() })
		return
	}
	s.bgWG.Add(1)
	go func() {
		defer s.bgWG.Done()
		ch := s.clock.After(d)
		select {
		case <-ch:
			fn()
		case <-s.stoppingChan():
		}
	}()
}

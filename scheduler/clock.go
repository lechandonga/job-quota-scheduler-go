package scheduler

import (
	"sync"
	"time"
)

// Clock 抽象时间来源，使租约到期与重试退避在测试中完全可控。
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// RealClock 对接真实时间。
type RealClock struct{}

// Now 返回真实当前时间。
func (RealClock) Now() time.Time { return time.Now() }

// After 等价于 time.After。
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// FakeClock 是手动推进的确定性时钟。
//
// 并发约定（避免锁环）：
//   - c.mu 只保护时钟自身数据，绝不在持有它时执行用户回调；
//   - 调度器在持有 s.mu 时注册定时器（锁序 s.mu -> c.mu）；
//   - Advance 先在 c.mu 内收集到期项，释放 c.mu 后再执行回调，
//     回调获取 s.mu 时不形成环路。
//
// 确定性：回调可能在执行期间（在 FakeClock 看来"同步"地）注册新的、
// 已经到期的定时器（例如心跳续租后看门狗以 1ns 重新挂表）。这类定时器
// 进入"即发队列"，Advance 会在同一次调用中循环排空，直到没有新的到期项。
// 因此一次 Advance 返回后，所有因此产生的调度动作都已完成。
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	pending []*fakeTimer // 已注册、尚未到期
	immed   []*fakeTimer // 回调期间新登记且已到期，需在本轮立即触发
	nextID  uint64
	inFire  bool
}

type fakeTimer struct {
	id    uint64
	at    time.Time
	ch    chan time.Time
	fn    func(now time.Time)
	alive bool
}

// NewFakeClock 从给定起点创建假时钟。
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now 返回假时钟当前时间。
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After 注册通道定时器（标准兼容接口）。
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	return c.add(d, nil).ch
}

// waitTimer 注册一个到期时在 Advance 内同步执行的回调，返回取消函数。
func (c *FakeClock) waitTimer(d time.Duration, fn func(time.Time)) (cancel func()) {
	t := c.add(d, fn)
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			t.alive = false
			c.mu.Unlock()
		})
	}
}

func (c *FakeClock) add(d time.Duration, fn func(time.Time)) *fakeTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	t := &fakeTimer{
		id:    c.nextID,
		at:    c.now.Add(d),
		ch:    make(chan time.Time, 1),
		fn:    fn,
		alive: true,
	}
	if t.at.After(c.now) {
		c.pending = append(c.pending, t)
	} else {
		// 已到期：进入即发队列。若正处于 Advance 的回调链中，本轮就会
		// 被排空；否则等待下一次 Advance。无论哪种情况，回调都不会在
		// 注册者的调用栈里同步执行，避免跨组件锁重入。
		c.immed = append(c.immed, t)
	}
	return t
}

// Advance 推进时钟并同步排空所有到期回调（含回调链中新产生的到期项）。
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)

	for {
		due, now := c.collectLocked()
		c.mu.Unlock()

		for _, t := range due {
			c.fire(t, now)
		}

		c.mu.Lock()
		if len(c.immed) == 0 {
			c.inFire = false
			c.mu.Unlock()
			return
		}
		// immed 中的定时器在锁内被标记 inFire 消费；循环继续。
	}
}

// collectLocked 取出当前所有应触发的定时器（常规到期 + 即发队列）。
func (c *FakeClock) collectLocked() ([]*fakeTimer, time.Time) {
	due := make([]*fakeTimer, 0)

	kept := c.pending[:0]
	for _, t := range c.pending {
		if t.alive && !t.at.After(c.now) {
			due = append(due, t)
		} else if t.alive {
			kept = append(kept, t)
		}
	}
	c.pending = kept

	due = append(due, c.immed...)
	c.immed = nil
	c.inFire = len(due) > 0
	return due, c.now
}

func (c *FakeClock) fire(t *fakeTimer, now time.Time) {
	c.mu.Lock()
	alive := t.alive
	fn := t.fn
	t.alive = false
	c.mu.Unlock()

	select {
	case t.ch <- now:
	default:
	}
	if alive && fn != nil {
		fn(now)
	}
}

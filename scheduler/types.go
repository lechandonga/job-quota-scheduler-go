package scheduler

import (
	"errors"
	"fmt"
	"time"
)

// Resources 表示一份可比较、可加减的资源量（CPU 核数、内存等）。
type Resources struct {
	CPU    int
	Memory int
}

func (r Resources) Add(o Resources) Resources {
	return Resources{CPU: r.CPU + o.CPU, Memory: r.Memory + o.Memory}
}

func (r Resources) Sub(o Resources) Resources {
	return Resources{CPU: r.CPU - o.CPU, Memory: r.Memory - o.Memory}
}

// FitsIn 报告 r 的每个维度都不超过 limit。
func (r Resources) FitsIn(limit Resources) bool {
	return r.CPU <= limit.CPU && r.Memory <= limit.Memory
}

func (r Resources) IsZero() bool { return r.CPU == 0 && r.Memory == 0 }

func (r Resources) String() string {
	return fmt.Sprintf("{cpu:%d mem:%d}", r.CPU, r.Memory)
}

// State 是作业的生命周期状态。Succeeded/Failed/Canceled 为终态，
// 每个作业只能落入其中一个，且落入后不再变化。
type State int

const (
	StatePending State = iota
	StateRunning
	StateSucceeded
	StateFailed
	StateCanceled
)

func (s State) Terminal() bool {
	return s == StateSucceeded || s == StateFailed || s == StateCanceled
}

func (s State) String() string {
	switch s {
	case StatePending:
		return "Pending"
	case StateRunning:
		return "Running"
	case StateSucceeded:
		return "Succeeded"
	case StateFailed:
		return "Failed"
	case StateCanceled:
		return "Canceled"
	}
	return "Unknown"
}

var (
	// ErrQuotaExceeded 表示提交会超出租户配额（且未选择排队等待）。
	ErrQuotaExceeded = errors.New("tenant quota exceeded")
	// ErrExceedsCapacity 表示作业需求超过集群总容量，永远不可能被调度。
	ErrExceedsCapacity = errors.New("job exceeds cluster capacity")
	// ErrJobExists 表示同 ID 作业已存在。
	ErrJobExists = errors.New("job already exists")
	// ErrJobNotFound 表示作业不存在。
	ErrJobNotFound = errors.New("job not found")
	// ErrInvalidJob 表示作业参数非法。
	ErrInvalidJob = errors.New("invalid job")
)

// Job 是一次作业提交。MaxRetries 表示失败/租约丢失后允许的重试次数
// （不含首次执行），即最多执行 MaxRetries+1 次。
type Job struct {
	ID         string
	Tenant     string
	Priority   int
	Need       Resources
	MaxRetries int
	// QueueOnQuota 为 true 时，配额不足的提交进入排队而不是被拒绝。
	QueueOnQuota bool
}

// JobStatus 是作业某一时刻的对外快照。
type JobStatus struct {
	ID      string
	Tenant  string
	State   State
	Attempt int
	Epoch   int
	Worker  string
	// Reason 解释当前状态的原因（排队原因、终态原因等）。
	Reason string
}

// Event 是调度器内部发生的可解释事件，按发生顺序记录。
type Event struct {
	At     time.Time
	Kind   string
	JobID  string
	Detail string
}

func (e Event) String() string {
	return fmt.Sprintf("%s [%s] job=%s %s", e.At.Format("15:04:05.000"), e.Kind, e.JobID, e.Detail)
}

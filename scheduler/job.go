package scheduler

import "time"

// Job 是作业的不可变描述（提交后字段不再修改）。
// 可变的运行时状态保存在调度器内部的 jobEntry 中。
type Job struct {
	ID       string
	TenantID string
	Priority int32 // 数值越大优先级越高
	Request  Resources

	// MaxAttempts 最大尝试次数（含首次）。<=0 表示执行 1 次、不重试。
	MaxAttempts int
	// InitialBackoff 首次失败后的基础退避；之后按 2 倍指数增长。
	// 0 表示失败后立即重新可调度。
	InitialBackoff time.Duration
	// MaxBackoff 退避上限。0 表示不封顶。
	MaxBackoff time.Duration
	// LeaseTTL 租约时长。执行方必须在此期间心跳续约，
	// 否则调度器会判定租约失效并重新调度。0 使用调度器默认值。
	LeaseTTL time.Duration
}

// Attempt 记录一次执行尝试的可观测信息。
type Attempt struct {
	Fence   uint64
	Started time.Time
	Ended   time.Time
	Outcome string // SUCCEEDED / FAILED / PREEMPTED / CANCELED / LEASE_LOST
	Err     string
}

// JobStatus 是作业状态的只读快照。
type JobStatus struct {
	Job       *Job
	Phase     Phase
	Attempt   int    // 已经发起的尝试次数
	Fence     uint64 // 当前代次
	RunningOn string // 执行方标识（模拟执行器中为 worker 名称）
	FailErr   string // 终态失败原因
	Attempts  []Attempt
}

// RetryPolicy 计算第 failCount 次失败后的退避时长（failCount 从 1 开始）。
type RetryPolicy struct {
	Initial time.Duration
	Max     time.Duration
}

// Backoff 返回指数退避：initial * 2^(failCount-1)，封顶 max。
func (p RetryPolicy) Backoff(failCount int) time.Duration {
	if p.Initial <= 0 {
		return 0
	}
	d := p.Initial
	for i := 1; i < failCount; i++ {
		d *= 2
		if p.Max > 0 && d >= p.Max {
			return p.Max
		}
	}
	if p.Max > 0 && d > p.Max {
		d = p.Max
	}
	return d
}

// Config 是调度器配置。零值可用（New 中会补默认值）。
type Config struct {
	// Capacity 集群总资源容量。
	Capacity Resources
	// DefaultLeaseTTL 作业未单独指定租约时的默认时长。
	DefaultLeaseTTL time.Duration
	// HeartbeatTimeout 心跳宽限：租约到期后额外容忍时间，
	// 避免时钟边界抖动导致误判。0 表示不额外容忍。
	HeartbeatTimeout time.Duration
	// StarvationBoost 每等待这么久，作业的有效优先级 +1，
	// 保证低优先级作业不会被无限期饿死。0 使用默认值。
	StarvationBoost time.Duration
	// MaxBoost 等待带来的优先级加成上限。0 使用默认值。
	MaxBoost int32
	// Logger 可选日志。
	Logger Logger
}

func (c *Config) withDefaults() {
	if c.DefaultLeaseTTL <= 0 {
		c.DefaultLeaseTTL = 10 * time.Second
	}
	if c.StarvationBoost <= 0 {
		c.StarvationBoost = time.Second
	}
	if c.MaxBoost <= 0 {
		c.MaxBoost = 1_000_000
	}
}

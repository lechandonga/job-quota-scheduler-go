// Package scheduler 实现一个纯进程内的本地作业调度器。
//
// 调度器只用标准库，不依赖任何外部消息队列、数据库或真实集群：
// 作业由注入的 Executor 模拟执行，时钟由注入的 Clock 控制，
// 因此所有行为（超时、退避、并发）都可以在测试中被确定性地复现。
package scheduler

import (
	"errors"
	"time"
)

// Resources 描述一份资源向量。当前只建模 CPU 与内存，
// 但所有账本逻辑都按"维度独立比较、逐维累加"的方式编写，便于扩展。
type Resources struct {
	CPUMillis int64
	MemoryMiB int64
}

// Add 返回 r+x（不修改接收者）。
func (r Resources) Add(x Resources) Resources {
	return Resources{
		CPUMillis: r.CPUMillis + x.CPUMillis,
		MemoryMiB: r.MemoryMiB + x.MemoryMiB,
	}
}

// Sub 返回 r-x（不修改接收者）。
func (r Resources) Sub(x Resources) Resources {
	return Resources{
		CPUMillis: r.CPUMillis - x.CPUMillis,
		MemoryMiB: r.MemoryMiB - x.MemoryMiB,
	}
}

// Fits 判断资源需求 need 是否能放入容量 limit。
func Fits(need, limit Resources) bool {
	return need.CPUMillis <= limit.CPUMillis && need.MemoryMiB <= limit.MemoryMiB
}

// Phase 是作业生命周期阶段。
type Phase int

const (
	// PhasePending 已受理但尚未执行（包括在队列中等待、重试退避中）。
	PhasePending Phase = iota
	// PhaseRunning 至少有一个执行方持有当前代租约。
	PhaseRunning
	// PhaseSucceeded 终态：成功。
	PhaseSucceeded
	// PhaseFailed 终态：重试耗尽后失败。
	PhaseFailed
	// PhaseCanceled 终态：被用户取消。
	PhaseCanceled
)

func (p Phase) String() string {
	switch p {
	case PhasePending:
		return "PENDING"
	case PhaseRunning:
		return "RUNNING"
	case PhaseSucceeded:
		return "SUCCEEDED"
	case PhaseFailed:
		return "FAILED"
	case PhaseCanceled:
		return "CANCELED"
	default:
		return "UNKNOWN"
	}
}

// IsTerminal 报告阶段是否为终态。成功、失败、取消三种终态互斥，
// 作业一旦进入终态就不可再变。
func (p Phase) IsTerminal() bool {
	return p == PhaseSucceeded || p == PhaseFailed || p == PhaseCanceled
}

// RejectReason 是提交未被受理时的机器可读原因。
type RejectReason string

const (
	// RejectInvalidRequest 请求字段非法（空租户、非法资源或优先级）。
	RejectInvalidRequest RejectReason = "INVALID_REQUEST"
	// RejectTenantUnknown 租户不存在。
	RejectTenantUnknown RejectReason = "TENANT_UNKNOWN"
	// RejectExceedsTenantQuota 需求超过租户配额上限，排队也永远无法满足。
	RejectExceedsTenantQuota RejectReason = "EXCEEDS_TENANT_QUOTA"
	// RejectTenantQuotaFull 租户当前已分配额度加上本次需求会超限，
	// 且请求要求不排队（queueIfBusy=false）。
	RejectTenantQuotaFull RejectReason = "TENANT_QUOTA_FULL"
	// RejectExceedsCluster 需求超过集群总容量，任何时候都无法调度。
	RejectExceedsCluster RejectReason = "EXCEEDS_CLUSTER_CAPACITY"
	// RejectStopped 调度器已关闭。
	RejectStopped RejectReason = "SCHEDULER_STOPPED"
)

// SubmitError 在作业被拒绝（而非排队）时返回，携带可区分的原因。
type SubmitError struct {
	Reason RejectReason
	msg    string
}

func (e *SubmitError) Error() string { return string(e.Reason) + ": " + e.msg }

func newSubmitError(reason RejectReason, msg string) *SubmitError {
	return &SubmitError{Reason: reason, msg: msg}
}

// IsSubmitError 判断 err 是否为 *SubmitError，并返回它。
func IsSubmitError(err error) (*SubmitError, bool) {
	var se *SubmitError
	if errors.As(err, &se) {
		return se, true
	}
	return nil, false
}

// ErrJobNotFound 表示操作的作业不存在。
var ErrJobNotFound = errors.New("scheduler: job not found")

// ErrAlreadyTerminal 表示取消时作业已经处于终态；取消仍是幂等的，
// 调用方可以忽略此错误，也可以借此区分"本次取消是否生效"。
var ErrAlreadyTerminal = errors.New("scheduler: job already terminal")

// Result 是一次执行尝试的结果。
type Result struct {
	Err error
	// Fence 必须是发起执行时 Executor 收到的围栏令牌；
	// 旧执行方无法伪造新令牌，因此迟到结果会被丢弃。
	Fence uint64
	// Epoch 是该次尝试的调度代次，每次 dispatch 唯一。
	Epoch uint64
}

// Lease 是执行方持有的租约句柄。
type Lease struct {
	JobID string
	fence uint64
	epoch uint64
}

// Fence 返回租约的围栏令牌（代次）。
func (l Lease) Fence() uint64 { return l.fence }

// Epoch 返回租约的调度代次。
func (l Lease) Epoch() uint64 { return l.epoch }

// Executor 由执行环境实现。生产中可以对接真实集群；
// 本工程提供进程内确定性的模拟执行器 executor/simulator.go。
type Executor interface {
	// Run 启动一次尝试，必须"立即返回"：真正的工作在新 goroutine 中进行，
	// 完成后调用 done。若执行被撤销（抢占/运行中取消/租约易主），
	// 执行方应尽快放弃；迟到的结果不会被接受。
	// fence 是本次尝试的代次，回报结果时必须原样带回。
	// 返回值 worker 是执行方自描述标识（用于状态观测）。
	Run(j *Job, lease Lease, deadline time.Time, done func(Result)) (worker string)
	// Revoke 撤销某次尝试：执行方应尽快放弃，且不得再让该作业产生副作用。
	// 调度器在调用前已让该 fence 失效，撤销本身不影响账本。
	Revoke(jobID string, fence uint64)
	// Cancel 撤销某作业当前所有活动尝试（正常至多一个），用于用户取消。
	Cancel(jobID string)
}

// Logger 是调度器接受的最小日志接口（slog.Logger 天然满足）。
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
}

package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ExecContext 是一次执行的上下文：Context 在调度器要求停止
// （取消/抢占）时被取消；handler 可以忽略它来模拟失联的执行方。
type ExecContext struct {
	context.Context
	JobID  string
	Epoch  int
	Worker string
}

// Handler 是模拟执行器运行作业体的方式。
type Handler func(ec ExecContext) error

// ResultSink 是执行器回报结果的接口，由 Scheduler 实现。
type ResultSink interface {
	ReportResult(jobID string, epoch int, workerID string, runErr error) bool
	Heartbeat(jobID string, epoch int, workerID string) bool
}

// Started 记录一次派发的执行，供测试断言。
type Started struct {
	JobID  string
	Epoch  int
	Worker string
}

// SimExecutor 是进程内的模拟执行器：每次 Start 起一个 goroutine 跑
// handler，结束后同步回报结果。测试通过阻塞型 handler（channel 门闩）
// 和 Wait() 获得确定性，无需 sleep。
type SimExecutor struct {
	mu sync.Mutex
	// OnReported 若非空，在每次结果回报（ReportResult 返回）后调用，
	// 供测试做确定性同步。
	OnReported func(jobID string)
	sink       ResultSink
	handlers   map[string]Handler
	def        Handler
	workerN    int
	wg         sync.WaitGroup
	started    []Started
	cancels    map[string]context.CancelFunc // "jobID/epoch" -> cancel
}

func NewSimExecutor(def Handler) *SimExecutor {
	return &SimExecutor{
		handlers: map[string]Handler{},
		def:      def,
		cancels:  map[string]context.CancelFunc{},
	}
}

// Bind 把执行器绑定到结果回报方（通常是 Scheduler）。
func (e *SimExecutor) Bind(sink ResultSink) { e.sink = sink }

// SetHandler 为指定作业注册 handler，覆盖默认 handler。
func (e *SimExecutor) SetHandler(jobID string, h Handler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[jobID] = h
}

// Start 实现 Executor 接口。
func (e *SimExecutor) Start(job Job, epoch int, _ time.Time) string {
	e.mu.Lock()
	e.workerN++
	workerID := fmt.Sprintf("worker-%d", e.workerN)
	h := e.def
	if specific, ok := e.handlers[job.ID]; ok {
		h = specific
	}
	ctx, cancel := context.WithCancel(context.Background())
	key := fmt.Sprintf("%s/%d", job.ID, epoch)
	e.cancels[key] = cancel
	e.started = append(e.started, Started{JobID: job.ID, Epoch: epoch, Worker: workerID})
	e.wg.Add(1)
	e.mu.Unlock()

	go func() {
		defer e.wg.Done()
		var err error
		if h != nil {
			err = h(ExecContext{Context: ctx, JobID: job.ID, Epoch: epoch, Worker: workerID})
		}
		e.sink.ReportResult(job.ID, epoch, workerID, err)
		if cb := e.OnReported; cb != nil {
			cb(job.ID)
		}
	}()
	return workerID
}

// Stop 实现 Stopper 接口：取消对应执行的 ctx。
func (e *SimExecutor) Stop(jobID string, epoch int) {
	e.mu.Lock()
	cancel, ok := e.cancels[fmt.Sprintf("%s/%d", jobID, epoch)]
	e.mu.Unlock()
	if ok {
		cancel()
	}
}

// Heartbeat 代表某个执行方实例为作业续约租约。
func (e *SimExecutor) Heartbeat(jobID string, epoch int, workerID string) bool {
	return e.sink.Heartbeat(jobID, epoch, workerID)
}

// Wait 等待所有已启动的执行 goroutine 结束（结果已回报完毕）。
func (e *SimExecutor) Wait() { e.wg.Wait() }

// Started 返回所有派发记录的拷贝。
func (e *SimExecutor) Started() []Started {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]Started(nil), e.started...)
}

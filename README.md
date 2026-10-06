# job-quota-scheduler-go

一个纯进程内、只依赖标准库的本地作业调度器。使用模拟执行器（goroutine）
运行作业，不依赖外部消息队列、数据库或真实集群；时间由手动时钟驱动，
保证并发提交、配额变化、优先级抢占、故障重试等场景都有确定、可解释的结果。

## 结构

- `scheduler/types.go` — 资源量、作业、状态机、事件等基础类型
- `scheduler/scheduler.go` — 调度器核心：配额、调度、抢占、重试、租约、账本
- `scheduler/executor.go` — 进程内模拟执行器 `SimExecutor`
- `scheduler/scheduler_test.go` — 场景化测试（`go test -v` 可看到每个场景的用例与结果）

## 调度与配额规则

- 作业携带 `Tenant`、`Priority`、`Need`（资源需求）、`MaxRetries`。
- 集群有总容量 `Config.Capacity`；每个租户可通过 `SetQuota` 设置配额。
  配额统计该租户 **Pending + Running** 作业的资源总和；未设置配额的租户不受限。
- 提交时配额不足：
  - `QueueOnQuota=false`：拒绝并返回 `ErrQuotaExceeded`；
  - `QueueOnQuota=true`：进入排队，`Status.Reason` 为 `queued: tenant quota`，
    配额上调（`SetQuota`）或其他作业结束释放配额后自动放行。
  两种路径可明确区分（错误 vs. 排队原因）。
- 需求超过集群总容量的作业直接拒绝（`ErrExceedsCapacity`，永不可调度）。
- 调度顺序：有效优先级降序，相同则按提交序号升序（先到先得）。
- 账本一致性：`CheckInvariants()` 从作业状态重算集群/租户占用并与计数器
  比对——运行中作业占用总和必须等于账本，不泄漏、不重复计数。所有测试在
  关键步骤后都会校验。

## 生命周期与取消

- 状态机：`Pending → Running → Succeeded/Failed/Canceled`。
  三个终态只能落一个，落入后不再变化。
- `Cancel` 幂等：对已终态作业重复取消返回 `(false, nil)`。
- 取消运行中的作业会作废其 epoch 并通知执行器停止（`Stopper` 接口）；
  取消与完成并发发生时，由互斥锁串行化，先到者决定终态，迟到结果被忽略。

## 抢占与防饥饿

- 资源紧张时，待调度作业会抢占**有效优先级严格更低**的运行中作业
  （从最低优先级开始），被抢占作业回到队列（`queued: preempted`），
  **不消耗重试次数**，资源释放后会被重新调度。
- 防饥饿：`Config.AgingInterval > 0` 时，作业每排队等待一个间隔，
  有效优先级 +1（仅排队期间累积）。等待足够久的低优先级作业最终能
  反超并抢占长期占用资源的作业。

## 失败重试

- 执行失败时，若已执行次数 `attempts <= MaxRetries` 则回队列重试，
  否则落入 `Failed`。即最多执行 `MaxRetries + 1` 次。

## 租约与重复执行

- 每次派发生成递增的 `epoch` 和租约（`Config.LeaseDuration`）。
- 执行方通过 `Heartbeat(jobID, epoch, workerID)` 续约；只有当前持有
  租约的 (epoch, worker) 才能续约成功。
- `Advance(d)` 推进手动时钟并回收过期租约：作业回队列重新调度
  （计入重试次数上限），旧 epoch 作废。
- 因此同一作业可能被重复派发（旧执行方失联但仍在跑）。重复执行是
  **可控**的：只有与当前 `(state, epoch, worker)` 完全匹配的结果才被
  接受；旧执行方的迟到结果会被忽略并记录 `stale-result-ignored` 事件，
  不会覆盖新结果。

## 可解释性

调度器记录完整事件流（`Events()`）：提交、派发、抢占、重试、租约过期、
迟到结果忽略、终态等，每条事件带手动时钟时间戳与原因说明。

## 本地验证

```sh
go build ./...
go test ./scheduler/ -v          # 场景与用例日志
go test ./scheduler/ -race       # 如环境支持 cgo
```

测试不依赖 sleep：执行器 handler 用 channel 门闩阻塞，时间由
`Scheduler.Advance` 手动推进，结果回报用 `SimExecutor.Wait` /
`OnReported` 同步，因此并发用例可稳定复现。

覆盖场景：

| 测试 | 场景 |
| --- | --- |
| `TestQuotaRejectAndQueue` | 配额拒绝 / 排队 / 配额上调放行 / 超容量拒绝 |
| `TestConcurrentSubmitAndCancel` | 并发提交+取消，终态唯一、取消幂等、账本一致 |
| `TestCancelRacesCompletion` | 取消与完成竞争 50 轮，只落一个终态 |
| `TestPreemption` | 高优先级抢占低优先级，被抢占者回队列后恢复 |
| `TestRetry` | 上限内重试成功 / 超限判失败 |
| `TestLeaseExpiryAndStaleResult` | 心跳续约、租约回收重调度、迟到结果忽略 |
| `TestAgingPreventsStarvation` | 老化提升有效优先级，防止饿死 |

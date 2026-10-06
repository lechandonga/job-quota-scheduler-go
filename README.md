# 本地作业配额调度器（Go，仅标准库）

一个**纯进程内**的作业调度器：用可注入的模拟执行器跑作业，用可注入的时钟
驱动租约与退避。不依赖外部消息队列、数据库或真实集群，本地离线即可
`go build` / `go test`。

目标是在四类棘手情况下都给出**确定、可解释**的结果：

1. 作业并发提交、租户配额动态变化；
2. 高优先级抢占低优先级、且低优先级不被饿死；
3. 作业失败重试、重试预算耗尽；
4. 执行方心跳中断、租约过期后的回收与重复执行控制。

## 目录结构

| 路径 | 说明 |
| --- | --- |
| `scheduler/` | 调度器核心：提交/取消、配额账本、调度循环、抢占、重试、租约 |
| `scheduler/types.go` | 资源、作业、阶段、提交原因、`Executor`/`Logger` 接口 |
| `scheduler/schedule.go` | 调度循环、就绪选择、抢占牺牲者选择、派发/抢占落账 |
| `scheduler/complete.go` | 结果裁决、失败重试、心跳续租、租约回收 |
| `scheduler/clock.go` | `RealClock` 与确定性的 `FakeClock`（`Advance` 同步触发回调） |
| `executor/simulator.go` | 进程内模拟执行器：自动成功/失败、脚本化、假死丢心跳三种模式 |
| `testutil/` | 场景化测试日志（场景标题、动作、PASS/FAIL 检查点） |
| `*_test.go` | 六个场景的覆盖用例，另含一个端到端 `Example` |

## 快速开始

```go
sim := executor.New()
sched := scheduler.New(scheduler.Config{
    Capacity: scheduler.Resources{CPUMillis: 1000, MemoryMiB: 1024},
}, sim, scheduler.RealClock{})
sched.AddTenant("team-a", scheduler.Resources{CPUMillis: 600, MemoryMiB: 512})

err := sched.Submit(&scheduler.Job{
    ID: "j1", TenantID: "team-a",
    Priority: 5,
    Request:  scheduler.Resources{CPUMillis: 200, MemoryMiB: 128},
    MaxAttempts: 3,
    InitialBackoff: time.Second,
}, true /* queueIfBusy */)
```

## 提交与配额规则

提交按以下顺序校验，任一不满足即**拒绝**，返回带机器可读 `RejectReason` 的
`*scheduler.SubmitError`（可通过 `scheduler.IsSubmitError` 识别）：

| 原因 | 含义 |
| --- | --- |
| `INVALID_REQUEST` | 缺 ID/租户、资源为负、JobID 重复 |
| `TENANT_UNKNOWN` | 租户未注册 |
| `EXCEEDS_CLUSTER_CAPACITY` | 需求超过集群总容量，排队也永远无法满足 |
| `EXCEEDS_TENANT_QUOTA` | 需求超过该租户配额上限 |
| `TENANT_QUOTA_FULL` | 租户当前已分配额 + 本次需求会超限，且 `queueIfBusy=false` |
| `SCHEDULER_STOPPED` | 调度器已 `Stop` |

关键点：

- **“超上限”与“暂时繁忙”分开**。前者任何时刻都无法满足，直接拒绝；
  后者在 `queueIfBusy=true` 时受理为 `PENDING`，额度释放后自动出队。
- **租户额度（assigned）** = 该租户所有非终态（在队 + 在跑）作业需求之和；
  **集群占用（used）** = 所有 `RUNNING` 作业需求之和。两者是两本独立账。
- **配额可热更新**（`UpdateTenantQuota`）：
  - 扩容：立即重新调度，排队作业立刻获得机会；
  - 缩容：**不杀正在运行的作业**（避免抖动），只阻止新调度；
    已占额度随作业结束/被抢占自然回落到新配额以内。
- 每次派发后调度器内部都会运行账本不变量校验（测试中可随时调用
  `Scheduler.CheckInvariants()`）：
  `used == Σ running.需求`，且每个租户 `assigned == Σ 非终态.需求`。

## 生命周期与取消

阶段机：`PENDING → RUNNING → 终态`，终态有且只有三种，互斥不可变：
`SUCCEEDED` / `FAILED` / `CANCELED`。

- 所有状态变更在**同一把调度器锁**内完成，执行结果、取消、抢占、租约回收
  都走同一临界区，因此“取消与完成同时发生”由锁裁决，谁先落账谁生效，
  不可能两个终态并存，也不可能执行两次。
- `Cancel` 幂等：
  - 作业在队：立即 `CANCELED`，同步释放租户额度；
  - 作业在跑：向执行器发撤销信号，终态由结果裁决（成功先到则成功，
    否则取消）；
  - 已终态：返回 `ErrAlreadyTerminal`（无副作用）；
  - 不存在：返回 `ErrJobNotFound`。
- 可通过 `Scheduler.Done(id)` 等待终态通道（同一作业多个等待者共享通道）。

## 优先级与抢占

调度循环反复选取**有效优先级最高**的就绪作业尝试启动；直接放不下时，
尝试为它抢占一组牺牲者。

- **有效优先级** = 静态优先级 + 等待老化加成
  （每 `StarvationBoost` 等待时长 +1，封顶 `MaxBoost`）。
- 排序键：有效优先级降序 → 排队时间升序（FIFO）→ JobID 升序，
  保证选择确定、可解释、无随机。
- **牺牲者条件**：必须正在运行，且其有效优先级**严格低于**候选作业；
  同优先级不互抢。牺牲者按“优先级最低 → 占用资源最大 → 启动最晚 →
  ID 最大”贪心选出，能释放最少且足够的资源。
- 抢占优先选**同租户**牺牲者以补齐租户配额缺口，只有集群容量是瓶颈时
  才跨租户抢占，尽量缩小波及面。
- 被抢占作业：**回到 `PENDING` 队列**，不判失败、**不消耗重试预算**，
  尝试记录标记为 `PREEMPTED`；资源在撤销的同时立即归还账本（不等执行方
  真正停下）。
- **防饿死**：老化加成随排队时间持续增长，任何低优先级作业最终都会在
  有效优先级上超过后来者而被调度（见 `TestNoStarvation`）。

## 失败重试

- 每次作业可配 `MaxAttempts`（含首次，默认 1，即不重试）。
- 失败后按指数退避：`InitialBackoff * 2^(n-1)`，封顶 `MaxBackoff`。
  退避期间作业为 `PENDING`，到期后重新参与调度。
- 尝试次数达到上限才进入唯一失败终态 `FAILED`，并携带失败原因。
- **抢占不计失败、不扣预算**；只有执行失败与租约丢失计入尝试次数。

## 租约、心跳与重复执行约定

- 每次派发都会产生：
  - `fence`：每次真实执行递增的围栏令牌；
  - `epoch`：每次调度唯一递增的代次（重排队期间也能区分新旧）；
  - `leaseTok`：每次启动/心跳续租换发，使旧看门狗定时器自动失效。
- 执行方周期性调用 `Heartbeat(jobID, fence)` 续租：
  - 成功则推进 deadline，作业继续运行；
  - 返回 `false` 表示 fence 已失效或已终态，执行方必须立刻停止。
- 超过 `LeaseTTL (+ HeartbeatTimeout 宽限)` 仍未续租，看门狗回收租约：
  作业按 `LEASE_LOST` 计入一次尝试并走退避重试；预算耗尽则 `FAILED`。
- **旧执行方的迟到结果不能覆盖新结果**：结果回报必须带回 `fence` 与
  `epoch`，与当前作业不符一律丢弃并记录 `stale result dropped`。
  模拟执行器内部还有一层“单次尝试只允许一个结果落账”的保护，
  与调度器构成双保险。
- 因此重复执行的窗口被严格限制在“旧 worker 失联、租约尚未到期”这段
  租约 TTL 内，且其输出不可能污染调度器状态——这是一种有界、
  可解释的 at-least-once 语义。

## 模拟执行器

`executor.Simulator` 每次尝试是一个 goroutine，行为由 `Spec` 决定：

- `ModeAutoSucceed` / `ModeAutoFail`：启动后立即回报（可配 `AutoDelay`）；
- `ModeScript`：不自动结束，测试通过 `OnStart` 拿到 `*Attempt`，
  再显式调用 `Succeed()` / `Fail(err)` 编排时序；
- `ModeHang`：永不结束且忽略撤销，模拟假死 worker，用于租约回收测试。

## 确定性时钟（测试为何不需要 sleep 碰运气）

测试不使用真实时间等待，而使用 `scheduler.FakeClock`：

- 定时器在状态转换的同一把锁临界区内登记，不存在“先推进时钟、
  后注册定时器导致丢事件”的窗口；
- `FakeClock.Advance(d)` 推进时钟后，会在**同一次调用内同步执行并排空**
  所有到期回调（包括回调链中新注册且已到期的定时器，例如续租后的看门狗），
  返回时调度结果已经落账；
- 并发用例（`TestConcurrentCancelCompletion`）用 channel barrier 让
  50 个取消 goroutine 与执行方回报**同时放行**，竞争必然发生；
  重复 60 轮，并用 `-race` 验证。

## 本地验证

只依赖标准库，离线可编译：

```bash
go build ./...
go vet ./...
go test -v -count=1 ./...              # 查看六个场景与每步 PASS/FAIL
CGO_ENABLED=1 go test -race ./...      # 竞态检测（需要 cgo/gcc）
go test -run Example -v .              # 端到端示例
```

### 测试覆盖的场景

| 测试 | 场景 |
| --- | --- |
| `TestQuotaRejection` | 五类拒绝原因、配额不足拒绝与排队、释放后自动出队 |
| `TestTenantQuotaChange` | 租户配额扩容立即放行、缩容不杀在跑作业 |
| `TestCancelQueued` | 排队取消立即生效、重复取消幂等、未知作业报错 |
| `TestConcurrentCancelCompletion` | 60 轮 × 50 goroutine 取消与成功同时落账，终态唯一、只执行一次 |
| `TestPreemption` | 同优先级不互抢、高优先级抢占、被抢占者回队不扣预算、FIFO 恢复 |
| `TestPreemptCrossTenant` | 集群瓶颈下的跨租户抢占 |
| `TestNoStarvation` | 等待老化让低优先级作业最终先于后来者运行 |
| `TestRetryExhaustion` | 指数退避重试成功、预算耗尽判 `FAILED` |
| `TestLeaseReclaim` | 心跳续租、断心跳租约回收、旧 fence 续租被拒、恢复后成功 |
| `TestLeaseReclaimThenFail` | 唯一尝试租约丢失即 `FAILED` |

`go test -v` 输出会先打印 `=== 场景: ... ===` 标题与说明，随后是
`-> 动作` 与 `[PASS]/[FAIL] 检查点`，可以直接看出每个场景覆盖了哪些用例、
结果如何；每个关键节点都会校验资源账本一致性。

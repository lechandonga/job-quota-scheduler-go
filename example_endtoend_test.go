package bootstrap_test

import (
	"fmt"

	"github.com/lechandonga/job-quota-scheduler-go/executor"
	"github.com/lechandonga/job-quota-scheduler-go/scheduler"
)

// ExampleEndToEnd 演示一次"低优先级运行 -> 高优先级抢占 -> 被抢占者恢复"
// 的完整流程。使用真实时钟与自动成功执行器，无需任何外部依赖。
func Example() {
	sim := executor.New()
	sched := scheduler.New(scheduler.Config{
		Capacity:        res(100, 100),
		DefaultLeaseTTL: 0,
	}, sim, scheduler.RealClock{})
	defer func() {
		sched.Stop()
		sim.Close()
	}()

	sched.AddTenant("team-a", res(100, 100))

	// 两个作业都用脚本化执行，由测试/调用方显式控制完成时机。
	sim.SetSpec("low", executor.Spec{Default: executor.ModeScript})
	sim.SetSpec("high", executor.Spec{Default: executor.ModeScript})

	_ = sched.Submit(&scheduler.Job{
		ID: "low", TenantID: "team-a", Priority: 1, Request: res(100, 100),
	}, true)
	_ = sched.Submit(&scheduler.Job{
		ID: "high", TenantID: "team-a", Priority: 9, Request: res(100, 100),
	}, true)

	st, _ := sched.Status("low")
	fmt.Println("low after high submitted:", st.Phase)

	// 高优先级结束后，低优先级恢复。
	if a := sim.ActiveAttempt("high"); a != nil {
		a.Succeed()
	}
	<-sched.Done("high")

	// 此时 low 已重新运行，把它跑完。
	if a := sim.ActiveAttempt("low"); a != nil {
		a.Succeed()
	}
	<-sched.Done("low")
	st, _ = sched.Status("low")
	fmt.Println("low final:", st.Phase)
	fmt.Println("low attempts:", st.Attempt)

	// Output:
	// low after high submitted: PENDING
	// low final: SUCCEEDED
	// low attempts: 2
}

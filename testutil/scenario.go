// Package testutil 提供调度器测试共用的可观测工具。
package testutil

import (
	"fmt"
	"sync"
	"testing"
)

// ScenarioLogger 实现 scheduler.Logger，把日志按"场景/步骤"打印，
// 让 go test -v 的输出能直接看出覆盖了哪些场景、每步发生了什么。
type ScenarioLogger struct {
	t       *testing.T
	mu      sync.Mutex
	verbose bool
}

// NewLogger 创建场景日志器。verbose 为 false 时只记录、不逐条打印，
// 但步骤与检查点仍会通过 Step/Check 输出。
func NewLogger(t *testing.T, verbose bool) *ScenarioLogger {
	t.Helper()
	return &ScenarioLogger{t: t, verbose: verbose}
}

// Step 打印场景中的一个动作。
func (l *ScenarioLogger) Step(format string, args ...any) {
	l.t.Helper()
	l.t.Logf("    -> "+format, args...)
}

// Check 打印一个断言/检查点及其结果。
func (l *ScenarioLogger) Check(ok bool, desc string, args ...any) {
	l.t.Helper()
	tag := "PASS"
	if !ok {
		tag = "FAIL"
	}
	l.t.Logf("    [%s] %s", tag, fmt.Sprintf(desc, args...))
}

// Info 实现 scheduler.Logger。
func (l *ScenarioLogger) Info(msg string, args ...any) {
	l.t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.verbose {
		l.t.Logf("       scheduler: %s %s", msg, kv(args))
	}
}

// Warn 实现 scheduler.Logger；警告总是打印。
func (l *ScenarioLogger) Warn(msg string, args ...any) {
	l.t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.t.Logf("       scheduler[WARN]: %s %s", msg, kv(args))
}

func kv(args []any) string {
	s := ""
	for i := 0; i+1 < len(args); i += 2 {
		s += fmt.Sprintf("%v=%v ", args[i], args[i+1])
	}
	return s
}

// Banner 输出场景标题，方便在 -v 日志里定位覆盖范围。
func Banner(t *testing.T, name, desc string) {
	t.Helper()
	t.Logf("")
	t.Logf("=== 场景: %s ===", name)
	for _, line := range splitLines(desc) {
		t.Logf("    %s", line)
	}
}

func splitLines(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == '\n' {
			out = append(out, cur)
			cur = ""
		} else {
			cur += string(r)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

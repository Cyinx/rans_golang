// Package log 是 rans 库的日志门面：库包不直接调用 log/slog 的包级函数、
// 也不持有私有 logger 实例，全部输出统一经本门面。宿主用 SetLogger 全局
// 注入自己的结构化 logger（默认透传 slog.Default()，跟随 slog.SetDefault），
// 测试用它注入捕获 logger 做日志断言。决策出处：docs/decisions/2026-09-30-phase6-security.md。
package log

import (
	"log/slog"
	"sync/atomic"
)

// current 持当前 logger；nil 表示默认态（每次调用动态取 slog.Default()，
// 宿主换 slog.SetDefault 亦被跟随）。热路径仅原子读，无锁。
var current atomic.Pointer[slog.Logger]

// SetLogger 换全局 logger；nil 恢复默认透传 slog.Default()。并发安全
// （原子写），不阻塞，构造期与运行期皆可调。
//
// 本函数是 Set* 门禁 grep（'func .*Set[A-Z]'）的唯一保留例外——日志入口
// 是 spec §4.2 保留的进程级横切装置，非节点配置面；豁免理由见 rans.SetLogger。
func SetLogger(l *slog.Logger) {
	if l == nil {
		current.Store(nil)
		return
	}
	current.Store(l)
}

// Logger 返回当前 logger（宿主需要结构化 handler 或级别控制时用）。
func Logger() *slog.Logger {
	if l := current.Load(); l != nil {
		return l
	}
	return slog.Default()
}

// Debug 经门面输出 DEBUG 级日志。
func Debug(msg string, args ...any) { Logger().Debug(msg, args...) }

// Info 经门面输出 INFO 级日志。
func Info(msg string, args ...any) { Logger().Info(msg, args...) }

// Warn 经门面输出 WARN 级日志。
func Warn(msg string, args ...any) { Logger().Warn(msg, args...) }

// Error 经门面输出 ERROR 级日志。
func Error(msg string, args ...any) { Logger().Error(msg, args...) }

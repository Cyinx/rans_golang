package rans

import (
	"log/slog"

	"github.com/cyinx/rans_golang/internal/log"
)

// SetLogger 设置运行时日志器（nil 恢复默认透传 slog.Default）。宿主用它
// 注入自己的结构化 logger；库内输出统一经该门面。并发安全（原子写），
// 不阻塞，构造期与运行期皆可调。
//
// 本函数是仓库「删除全部 Set*」门禁（grep -rn 'func .*Set[A-Z]'）的唯一
// 保留例外：日志门面是 spec §4.2 保留的进程级横切注入入口，语义即「换
// 全局 logger」，与节点配置面（构造时一次给全、运行中不可变）无关。
func SetLogger(l *slog.Logger) { log.SetLogger(l) }

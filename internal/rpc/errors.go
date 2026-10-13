package rpc

// 统一错误模型：错误形态正本是 rans.Error——调用方
// 判错一律经 rans.CodeOf/ReasonOf 与 rans.* 哨兵（errors.Is），本包不持有
// 其他错误形态。

import (
	"github.com/cyinx/rans_golang"
)

// 网关准入拒绝面（*rans.Error 形态）。
var (
	// ErrPendingCapacity pending 表满载拒绝（附录 A NET-8）：网关在途请求
	// 达容量上限，新请求不入表不发送。属节点过载信号（Unavailable+
	// Overloaded）：分类唯一表不自动重试（过载保护——立即重试大概率再次
	// 满载，调用方应退避或降载）。
	ErrPendingCapacity = rans.NewError(rans.CodeUnavailable, rans.ReasonOverloaded,
		"rans: gateway: pending table at capacity")
	// ErrDuplicateMessageID 重复 MessageID 入表拒绝（Error+Internal，编程
	// 错误）：关联键在途复用属调用方编程错误（MessageID 必须经
	// NextMessageID 预填且在途不复用，陷阱 7）。
	ErrDuplicateMessageID = rans.NewError(rans.CodeError, rans.ReasonInternal,
		"rans: gateway: duplicate message id in flight")
)

// errSendFailed 连接类发送失败（Unavailable+SendFailed，确定未投递）：
// 端点发送重试耗尽（resolve 失败/拨号失败/连接未就绪/写出错误经重试仍
// 失败）或请求预算在发送段耗尽时，以本形态包装末次错误返回（%w 保留根因
// 链）。语义＝「确定未投递」——消息未送出连接，分类唯一表归
// RetryUnconditional，调用方可无条件重建重试。ctx 取消不归本形态
// （调用方主动放弃，非连接失败）。
var errSendFailed = rans.NewError(rans.CodeUnavailable, rans.ReasonSendFailed,
	"rans: invoke: send failed")

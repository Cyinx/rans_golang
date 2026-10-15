package rans

import (
	"context"
	"errors"
	"fmt"
)

// Reason 是调用错误的具体原因，过线（结构化 Error 帧体的 varint 字段，
// 数值即契约、只追加不复用）；与 Code 一起决定重试分类（约定 spec §5.3）。
type Reason uint16

const (
	ReasonNone Reason = iota
	ReasonSendFailed
	ReasonBusy
	ReasonOverloaded
	ReasonDeactivating
	ReasonDraining
	ReasonLeaseLost
	ReasonActivationFailed
	ReasonArbitrationLost
	ReasonNoEligibleNode
	ReasonStaleLocation
	ReasonUnknownKind
	ReasonNotLocal
	ReasonUnsupported
	ReasonMigrationLost
	ReasonInternal
	ReasonHandler
	ReasonExpired
	ReasonCycle
	ReasonDeactivated
)

// String 返回 reason 的诊断名（snake_case，供日志与测试可读性）；线格式
// 是 Error 帧体的 varint 数值，本文本不是线契约。
func (r Reason) String() string {
	switch r {
	case ReasonNone:
		return "none"
	case ReasonSendFailed:
		return "send_failed"
	case ReasonBusy:
		return "busy"
	case ReasonOverloaded:
		return "overloaded"
	case ReasonDeactivating:
		return "deactivating"
	case ReasonDraining:
		return "draining"
	case ReasonLeaseLost:
		return "lease_lost"
	case ReasonActivationFailed:
		return "activation_failed"
	case ReasonArbitrationLost:
		return "arbitration_lost"
	case ReasonNoEligibleNode:
		return "no_eligible_node"
	case ReasonStaleLocation:
		return "stale_location"
	case ReasonUnknownKind:
		return "unknown_kind"
	case ReasonNotLocal:
		return "not_local"
	case ReasonUnsupported:
		return "unsupported"
	case ReasonMigrationLost:
		return "migration_lost"
	case ReasonInternal:
		return "internal"
	case ReasonHandler:
		return "handler"
	case ReasonExpired:
		return "expired"
	case ReasonCycle:
		return "cycle"
	case ReasonDeactivated:
		return "deactivated"
	default:
		return fmt.Sprintf("reason(%d)", uint16(r))
	}
}

// Error 是调用错误的统一形态：本地与远端同构（位置透明）。Call/Tell 返回
// 的错误一律不可 Unwrap 到实体原始错误——Handle 错误在投递边界已折叠为
// CodeError + ReasonHandler。
type Error struct {
	Code    Code
	Reason  Reason
	Message string
	Node    NodeID // 产生错误的节点；0 表示本地（与匿名源哨兵同义）
	AppCode int64  // 实体返回 AppError 时携带
	AppData []byte // 实体返回 AppDataError 时携带，不透明，≤ 64 KiB

	// OwnerNode/OwnerEpoch 是重定向提示（H13 决策 24，Error 帧字段 6）：
	// StaleLocation 类错误携带调用方已知的正确归属（epoch=0 表示代际未
	// 知），调用方据此立即改写本地位置缓存（不等正 TTL 到期）。OwnerNode
	// =0 表示无提示（字段不上线）。
	OwnerNode  NodeID
	OwnerEpoch uint64
}

// NewError 构造统一错误（字段全部导出，内部包与消费方都可构造）。
func NewError(code Code, reason Reason, msg string) *Error {
	return &Error{Code: code, Reason: reason, Message: msg}
}

// Error 实现 error：文本供日志与排障；跨节点判定用 errors.Is / CodeOf。
func (e *Error) Error() string {
	if e.Reason == ReasonNone {
		return fmt.Sprintf("rans: code=%d: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("rans: code=%d reason=%d: %s", e.Code, e.Reason, e.Message)
}

// Is 按维度匹配：target 为 *Error 时比 Code；target.Reason 非 0 时同时比
// Reason；target.AppCode 非 0 时同时比 AppCode。target 为标准库
// context.DeadlineExceeded 时超时类（Code==Timeout）匹配（票 13：标准 Go
// 判错写法对超时/取消两半都成立——取消经包装链携带 ctx.Err() 本就可达）。
func (e *Error) Is(target error) bool {
	if target == context.DeadlineExceeded {
		return e.Code == CodeTimeout
	}
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	if e.Code != t.Code {
		return false
	}
	if t.Reason != ReasonNone && e.Reason != t.Reason {
		return false
	}
	if t.AppCode != 0 && e.AppCode != t.AppCode {
		return false
	}
	return true
}

// AppError 是实体返回的业务错误：以 CodeError + ReasonHandler + AppCode +
// Message 过线，调用方得到同构的 *Error。
type AppError interface {
	error
	AppCode() int64
}

// AppDataError 可额外携带不透明载荷（如消费方的 HTTP 状态），原样过线。
type AppDataError interface {
	AppError
	AppData() []byte
}

// CodeOf 取错误的调用状态码：nil → CodeOK；链上首个 *Error → 其 Code；
// 其余（非 rans 错误）→ CodeError。
func CodeOf(err error) Code {
	if err == nil {
		return CodeOK
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return CodeError
}

// ReasonOf 取错误的具体原因：nil / 非 rans 错误 → ReasonNone。
func ReasonOf(err error) Reason {
	var e *Error
	if errors.As(err, &e) {
		return e.Reason
	}
	return ReasonNone
}

// 调用错误哨兵族：零 Reason 形态，供 errors.Is 判类（只比 Code；具体
// reason 经 ReasonOf 取）。CodeCanceled/CodeConnectionLost 只在本地产生。
var (
	ErrTimeout        = NewError(CodeTimeout, ReasonNone, "timeout")
	ErrNotFound       = NewError(CodeNotFound, ReasonNone, "not found")
	ErrUnavailable    = NewError(CodeUnavailable, ReasonNone, "unavailable")
	ErrCanceled       = NewError(CodeCanceled, ReasonNone, "canceled")
	ErrConnectionLost = NewError(CodeConnectionLost, ReasonNone, "connection lost")
	ErrSerialization  = NewError(CodeSerialization, ReasonNone, "serialization failed")
	ErrDecode         = NewError(CodeDecode, ReasonNone, "decode failed")
)

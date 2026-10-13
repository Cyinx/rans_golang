// Package routing 实现失败策略的错误分类与调用选项（规格 §9.2）：
// Classify 把错误归类为重试决策四类；InvokeOptions 携带调用级幂等标注
// （决策出处：docs/decisions/2026-09-30-phase4-idempotency.md）。重试环
// 本体（上限/退避/载荷工厂契约）在 retry.go；位置缓存在 location.go。
// 错误形态正本是 rans.Error（统一错误模型）：分类只看
// Code/Reason（分类唯一表见约定 spec §5.3 与本文件 Classify 判据）。
package routing

import (
	"errors"

	"github.com/cyinx/rans_golang"
)

// Class 重试决策分类（规格 §9.2）。
type Class int

const (
	// NoRetry 不重试：确定性失败——对端/本端明确回错（Error/Serialization/
	// Decode/Handler/Internal/Unsupported/MigrationLost 等）、Canceled（调用
	// 方放弃）、Unavailable+Busy/Overloaded（过载保护：确定未执行但无条件
	// 重试会在热点实体上放大流量）与
	// NotFound+NotLocal（管理操作）、未知/非 rans 错误。
	NoRetry Class = iota
	// RetryUnconditional 无条件重试：确定未执行——Unavailable（Busy/
	// Overloaded 除外）回绝。全部 Unavailable 发射点都在 admission 之前
	// （收件箱满/销毁中/排空门闸/激活失败/仲裁落败未转发/发送失败
	// ReasonSendFailed）——决策出处：
	// docs/decisions/2026-10-09-hr2-unavailable-retry.md。重建重试不破坏
	// 恰好一次语义，仍受环上限与退避约束。
	RetryUnconditional
	// RetryNotFound 位置陈旧：NotFound + StaleLocation/UnknownKind——目录
	// 位置过期或目标 kind 未知（迁移落位竞态），重新解析位置后无条件重试；
	// 仍无可用节点则终止。
	RetryNotFound
	// RetryAmbiguous 模糊投递：请求可能已送达（Timeout/ConnectionLost）——
	// 仅幂等标注的调用可重试（InvokeOptions.Idempotent）。
	RetryAmbiguous
)

// Classify 把错误归类为重试决策。判据＝分类唯一表（约定 spec §5.3；
// **只看链上首个 *rans.Error 的 Code/Reason**——本地与远端同构）：
//
//	Unavailable（Busy/Overloaded 除外）   → RetryUnconditional（确定未执行）
//	Unavailable + Busy/Overloaded        → NoRetry（过载保护）
//	NotFound + StaleLocation/UnknownKind → RetryNotFound（重解析后重试）
//	NotFound + NotLocal（与其余）        → NoRetry（管理操作）
//	Timeout / ConnectionLost             → RetryAmbiguous（仅 Idempotent）
//	Canceled / 其余（Error/Serialization/Decode/…）/ 非 rans 错误 → NoRetry
//
// nil → NoRetry。链上首个 *rans.Error 即产生方（errors.As 语义），链上
// 更深处的 rans.Error 不改写判定；Handle 错误已在投递边界折叠为
// CodeError+ReasonHandler（不可 Unwrap 到嵌套运行时形态），天然 NoRetry。
func Classify(err error) Class {
	if err == nil {
		return NoRetry
	}
	var e *rans.Error
	if !errors.As(err, &e) {
		return NoRetry
	}
	switch e.Code {
	case rans.CodeUnavailable:
		switch e.Reason {
		case rans.ReasonBusy, rans.ReasonOverloaded:
			// 过载保护：确定未执行，
			// 但无条件重试会在热点实体上放大流量。
			return NoRetry
		default:
			return RetryUnconditional
		}
	case rans.CodeNotFound:
		switch e.Reason {
		case rans.ReasonStaleLocation, rans.ReasonUnknownKind:
			return RetryNotFound
		default:
			// NotLocal 与其余 NotFound：管理操作目标面，不重试。
			return NoRetry
		}
	case rans.CodeTimeout, rans.CodeConnectionLost:
		return RetryAmbiguous
	default:
		// Canceled / CodeError / Serialization / Decode / 未知码。
		return NoRetry
	}
}

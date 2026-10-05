package reconcile

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
)

// 渠道对账：把渠道侧交易明细（对账单）与账本凭证按业务请求标识（RequestID）双边配对。
// 只读、只分类、不修复；与账本内部核对共用同一时间窗口口径 [from, to)。

// ChannelDirection 渠道明细的资金方向（从渠道视角）。
type ChannelDirection string

const (
	ChannelDebit  ChannelDirection = "DEBIT"  // 支出：付款/代付，对应账本 TRANSFER 凭证
	ChannelCredit ChannelDirection = "CREDIT" // 收入：退款/冲正回流，对应账本 REVERSAL 凭证
)

// ChannelStatus 渠道明细状态。
type ChannelStatus string

const (
	ChannelSuccess ChannelStatus = "SUCCESS" // 渠道确认成功，参与配对
	ChannelFailed  ChannelStatus = "FAILED"  // 渠道失败：不应有账务，不参与配对（计数披露）
	ChannelPending ChannelStatus = "PENDING" // 渠道处理中：不参与配对（计数披露）
)

// ChannelTxn 渠道对账单上的一条交易明细。
type ChannelTxn struct {
	ChannelTxnID string           // 渠道流水号（渠道侧唯一）
	RequestID    string           // 对应的业务请求标识（幂等键）
	Amount       int64            // 最小货币单位，非负
	Direction    ChannelDirection // 资金方向
	Status       ChannelStatus    // 渠道侧状态
	OccurredAt   int64            // 发生时间，UnixNano
}

// contentKey 渠道明细的内容指纹：用于识别“同一 RequestID 多条且内容不一致”。
// 完全相同的重复记录（重发/重复导入）折叠为一条，保证重跑不重复计数。
func (t ChannelTxn) contentKey() string {
	return strings.Join([]string{
		fmt.Sprintf("%d", t.Amount),
		string(t.Direction),
		string(t.Status),
	}, "|")
}

// ChannelDiffKind 渠道配对差异分类。
type ChannelDiffKind string

const (
	DiffLedgerOnly       ChannelDiffKind = "LEDGER_ONLY"       // 账本有而渠道没有（渠道未达/漏记）
	DiffChannelOnly      ChannelDiffKind = "CHANNEL_ONLY"      // 渠道有而账本没有（渠道多记/未入账）
	DiffContentMismatch  ChannelDiffKind = "CONTENT_MISMATCH"  // 两边都有但金额或方向对不上
	DiffChannelDuplicate ChannelDiffKind = "CHANNEL_DUPLICATE" // 渠道自身重复：同一 RequestID 多条且内容不一致
)

// ChannelDiff 单条配对差异及其双边判定依据。
type ChannelDiff struct {
	Kind      ChannelDiffKind
	RequestID string // 业务请求标识，定位差异

	// 账本侧判定依据（无账本记录时为零值）。
	LedgerVoucherID string
	LedgerAmount    int64
	LedgerDirection ChannelDirection
	LedgerAt        int64

	// 渠道侧判定依据（无渠道记录时为零值）。
	ChannelTxnIDs    []string
	ChannelAmount    int64
	ChannelDirection ChannelDirection
	ChannelAt        int64

	Evidence string
}

func (d ChannelDiff) String() string {
	s := "channel-diff kind=" + string(d.Kind) + " request_id=" + d.RequestID
	if d.LedgerVoucherID != "" {
		s += fmt.Sprintf(" | ledger{voucher=%s amount=%d dir=%s at=%d}",
			d.LedgerVoucherID, d.LedgerAmount, d.LedgerDirection, d.LedgerAt)
	}
	if len(d.ChannelTxnIDs) > 0 {
		s += fmt.Sprintf(" | channel{txns=%s amount=%d dir=%s at=%d}",
			strings.Join(d.ChannelTxnIDs, ","), d.ChannelAmount, d.ChannelDirection, d.ChannelAt)
	}
	if d.Evidence != "" {
		s += " | evidence: " + d.Evidence
	}
	return s
}

// ChannelReport 渠道对账报告：保留账本自身核对结论，并给出双边配对结论。
type ChannelReport struct {
	WindowFrom int64
	WindowTo   int64

	// Internal 为同一时间窗口、同一快照下的账本自身核对结论（凭证/流水/余额）。
	Internal Report

	ChannelDiffs []ChannelDiff
	// 按类型统计，便于断言与汇总。
	CountByKind map[ChannelDiffKind]int

	// Matched 双边一致的业务笔数。
	Matched int
	// SkippedChannelNonSuccess 窗口内状态非 SUCCESS 的渠道明细条数：
	// 失败/处理中的渠道记录本身不构成差异，但如实计数、不静默丢弃。
	SkippedChannelNonSuccess int

	// Balanced 为 true 当且仅当账本自身核对无差异且渠道配对无差异。
	Balanced bool
}

// ReconcileWithChannel 在 [from,to) 窗口内先做账本自身核对，
// 再把窗口内账本凭证与渠道对账单明细按 RequestID 双边配对。
// 只读：建议在 ledger.Service.View 内调用以获得快照一致性。
// 对同一份对账单重复调用结果一致（幂等），内容完全相同的渠道重复记录会被折叠。
func (r *Reconciler) ReconcileWithChannel(ctx context.Context, from, to int64, txns []ChannelTxn, opts *Options) (ChannelReport, error) {
	rep := ChannelReport{
		WindowFrom:  from,
		WindowTo:    to,
		CountByKind: map[ChannelDiffKind]int{},
	}

	// 1) 账本自身核对：同一窗口、同一快照，结论完整保留在报告中。
	internal, err := r.Reconcile(ctx, from, to, opts)
	if err != nil {
		return rep, err
	}
	rep.Internal = internal

	// 2) 窗口内账本凭证按 RequestID 建索引（RequestID 在账本侧唯一）。
	vouchers, err := r.store.ListVouchers(ctx, from, to)
	if err != nil {
		return rep, err
	}
	ledgerByReq := map[string]domain.Voucher{}
	for _, v := range vouchers {
		ledgerByReq[v.RequestID] = v
	}

	// 3) 渠道明细：窗口过滤 + 状态过滤 + 按 RequestID 分组、内容去重。
	//    同一 RequestID 去重后仍有多种内容 → 渠道自身重复，整组不参与配对。
	channelByReq := map[string]ChannelTxn{} // 可配对池：每个 RequestID 恰好一条
	dupGroups := map[string][]ChannelTxn{}  // 内容冲突组：整组记为渠道自身重复
	for _, t := range txns {
		if t.OccurredAt < from || t.OccurredAt >= to {
			continue // 窗口外：半开区间 [from,to)，边界只算一次
		}
		if t.Status != ChannelSuccess {
			rep.SkippedChannelNonSuccess++
			continue
		}
		if t.RequestID == "" {
			// 无业务请求标识无法配对，逐条记为渠道单边，不合并、不丢弃。
			rep.addChannelDiff(ChannelDiff{
				Kind:             DiffChannelOnly,
				RequestID:        "",
				ChannelTxnIDs:    []string{t.ChannelTxnID},
				ChannelAmount:    t.Amount,
				ChannelDirection: t.Direction,
				ChannelAt:        t.OccurredAt,
				Evidence:         "channel record has empty request id, cannot be paired",
			})
			continue
		}
		if group, dup := dupGroups[t.RequestID]; dup {
			dupGroups[t.RequestID] = append(group, t) // 已判定冲突的组：继续归集，便于完整举证
			continue
		}
		if prev, ok := channelByReq[t.RequestID]; ok {
			if prev.contentKey() == t.contentKey() {
				continue // 内容完全相同的重复记录：折叠，保证重复导入/重跑不重复计数
			}
			// 内容不一致的重复：整组移出配对池，按渠道自身重复处理。
			delete(channelByReq, t.RequestID)
			dupGroups[t.RequestID] = []ChannelTxn{prev, t}
			continue
		}
		channelByReq[t.RequestID] = t
	}
	for reqID, group := range dupGroups {
		ids := make([]string, 0, len(group))
		contents := make([]string, 0, len(group))
		seen := map[string]bool{}
		for _, t := range group {
			ids = append(ids, t.ChannelTxnID)
			if ck := t.contentKey(); !seen[ck] {
				seen[ck] = true
				contents = append(contents, "{"+ck+"}")
			}
		}
		sort.Strings(ids)
		rep.addChannelDiff(ChannelDiff{
			Kind:          DiffChannelDuplicate,
			RequestID:     reqID,
			ChannelTxnIDs: ids,
			Evidence: fmt.Sprintf("channel statement has %d conflicting records for same request id: %s",
				len(group), strings.Join(contents, " vs ")),
		})
	}

	// 4) 双边配对：每个 RequestID 只落入一个类别，不混合、不遗漏。
	for reqID, v := range ledgerByReq {
		ct, ok := channelByReq[reqID]
		if !ok {
			if _, dup := dupGroups[reqID]; dup {
				continue // 该请求已在“渠道自身重复”类中交代，不再重复计数
			}
			rep.addChannelDiff(ChannelDiff{
				Kind:            DiffLedgerOnly,
				RequestID:       reqID,
				LedgerVoucherID: v.ID,
				LedgerAmount:    v.Amount,
				LedgerDirection: ledgerDirectionOf(v),
				LedgerAt:        v.CreatedAt.UnixNano(),
				Evidence:        "ledger voucher has no matching channel record in window",
			})
			continue
		}
		ld := ledgerDirectionOf(v)
		if v.Amount != ct.Amount || ld != ct.Direction {
			rep.addChannelDiff(ChannelDiff{
				Kind:             DiffContentMismatch,
				RequestID:        reqID,
				LedgerVoucherID:  v.ID,
				LedgerAmount:     v.Amount,
				LedgerDirection:  ld,
				LedgerAt:         v.CreatedAt.UnixNano(),
				ChannelTxnIDs:    []string{ct.ChannelTxnID},
				ChannelAmount:    ct.Amount,
				ChannelDirection: ct.Direction,
				ChannelAt:        ct.OccurredAt,
				Evidence: fmt.Sprintf("paired by request id but content differs: ledger amount=%d dir=%s vs channel amount=%d dir=%s",
					v.Amount, ld, ct.Amount, ct.Direction),
			})
			delete(channelByReq, reqID) // 已交代，不再落入渠道单边
			continue
		}
		rep.Matched++
		delete(channelByReq, reqID)
	}
	// 剩余渠道记录：账本侧没有对应凭证。
	for reqID, ct := range channelByReq {
		rep.addChannelDiff(ChannelDiff{
			Kind:             DiffChannelOnly,
			RequestID:        reqID,
			ChannelTxnIDs:    []string{ct.ChannelTxnID},
			ChannelAmount:    ct.Amount,
			ChannelDirection: ct.Direction,
			ChannelAt:        ct.OccurredAt,
			Evidence:         "channel record has no matching ledger voucher in window",
		})
	}

	sortChannelDiffs(rep.ChannelDiffs)
	rep.Balanced = internal.Balanced && len(rep.ChannelDiffs) == 0
	return rep, nil
}

// ledgerDirectionOf 把账本凭证类型映射为渠道视角的资金方向。
func ledgerDirectionOf(v domain.Voucher) ChannelDirection {
	switch v.Type {
	case domain.VoucherTransfer:
		return ChannelDebit
	case domain.VoucherReversal:
		return ChannelCredit
	default:
		return ""
	}
}

func (rep *ChannelReport) addChannelDiff(d ChannelDiff) {
	rep.ChannelDiffs = append(rep.ChannelDiffs, d)
	rep.CountByKind[d.Kind]++
}

// sortChannelDiffs 让报告输出确定性排序（按类别再按 RequestID），便于阅读与断言。
func sortChannelDiffs(diffs []ChannelDiff) {
	sort.Slice(diffs, func(i, j int) bool {
		if diffs[i].Kind != diffs[j].Kind {
			return diffs[i].Kind < diffs[j].Kind
		}
		return diffs[i].RequestID < diffs[j].RequestID
	})
}

package channelrecon

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/reconcile"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/store"
)

// ChannelDiffKind 渠道对账差异分类。
type ChannelDiffKind string

const (
	DiffLedgerOnly    ChannelDiffKind = "LEDGER_ONLY"       // 账本有、渠道没有（渠道未达/漏记）
	DiffChannelOnly   ChannelDiffKind = "CHANNEL_ONLY"      // 渠道有、账本没有（渠道多记/账本未入账）
	DiffFieldMismatch ChannelDiffKind = "FIELD_MISMATCH"    // 两边都有但金额或方向对不上
	DiffChannelDup    ChannelDiffKind = "CHANNEL_DUPLICATE" // 渠道自身重复：同一业务请求标识多条
)

// ChannelDiff 单条渠道对账差异及其判定依据（账本侧与渠道侧各自证据）。
type ChannelDiff struct {
	Kind          ChannelDiffKind
	RequestID     string
	VoucherID     string // 账本侧凭证（若有）
	ChannelTxnID  string // 渠道流水号（若可定位到单条）
	LedgerAmount  int64
	ChannelAmount int64
	LedgerTime    int64
	ChannelTime   int64
	Evidence      string
}

func (d ChannelDiff) String() string {
	s := "channel-diff kind=" + string(d.Kind) + " request_id=" + d.RequestID
	if d.VoucherID != "" {
		s += " voucher_id=" + d.VoucherID
	}
	if d.ChannelTxnID != "" {
		s += " channel_txn_id=" + d.ChannelTxnID
	}
	s += fmt.Sprintf(" ledger_amount=%d channel_amount=%d ledger_time=%d channel_time=%d",
		d.LedgerAmount, d.ChannelAmount, d.LedgerTime, d.ChannelTime)
	if d.Evidence != "" {
		s += " | evidence: " + d.Evidence
	}
	return s
}

// Report 渠道对账报告：保留账本自身核对结论，并给出双边配对结论。
type Report struct {
	WindowFrom  int64
	WindowTo    int64
	Internal    reconcile.Report // 账本自身核对结论（原样保留）
	Diffs       []ChannelDiff
	CountByKind map[ChannelDiffKind]int
	Matched     int // 双边一致的对数
	LedgerTxns  int // 窗口内账本凭证数
	ChannelTxns int // 窗口内参与配对的渠道明细数
	SkippedChannel int // 非 SUCCESS 状态被跳过（计数报告，不静默）
	Balanced    bool // 账本自身核对与双边配对均无差异
}

// Reconciler 渠道对账器：只读核对，不改动账本数据。
type Reconciler struct {
	store    store.Store
	internal *reconcile.Reconciler
}

func NewReconciler(st store.Store) *Reconciler {
	return &Reconciler{store: st, internal: reconcile.NewReconciler(st)}
}

// Reconcile 在 [from,to) 窗口内先做账本自身核对，再与渠道对账单双边配对。
// 窗口口径与账本侧一致（左闭右开，边界只算一次）。
// 建议在 ledger.Service.View 内调用以获得一致时点快照；
// 整个过程只读，不改动账本数据。
func (r *Reconciler) Reconcile(ctx context.Context, stmt *Statement, from, to int64, opts *reconcile.Options) (Report, error) {
	rep := Report{WindowFrom: from, WindowTo: to, CountByKind: map[ChannelDiffKind]int{}}

	// 1) 账本自身核对结论原样保留。
	internal, err := r.internal.Reconcile(ctx, from, to, opts)
	if err != nil {
		return rep, err
	}
	rep.Internal = internal

	// 2) 取窗口内账本凭证（与内部核对同一快照、同一窗口口径）。
	vouchers, err := r.store.ListVouchers(ctx, from, to)
	if err != nil {
		return rep, err
	}
	rep.LedgerTxns = len(vouchers)

	// 3) 渠道侧：窗口内、仅 SUCCESS 参与配对；其它状态计数报告。
	var channelTxns []ChannelTxn
	for _, t := range stmt.Txns(from, to) {
		if t.Status == StatusSuccess {
			channelTxns = append(channelTxns, t)
		} else {
			rep.SkippedChannel++
		}
	}
	rep.ChannelTxns = len(channelTxns)

	// 4) 渠道明细按业务请求标识归组，先检出渠道自身重复。
	byReq := map[string][]ChannelTxn{}
	for _, t := range channelTxns {
		byReq[t.RequestID] = append(byReq[t.RequestID], t)
	}
	dupReqs := map[string]bool{}
	for reqID, group := range byReq {
		if len(group) > 1 {
			dupReqs[reqID] = true
			rep.add(channelDupDiff(reqID, group, vouchers))
		}
	}

	// 5) 双边配对：以业务请求标识为键。
	voucherByReq := map[string]domain.Voucher{}
	for _, v := range vouchers {
		voucherByReq[v.RequestID] = v
	}
	matchedReqs := map[string]bool{}
	for reqID, group := range byReq {
		if dupReqs[reqID] {
			continue // 渠道自身重复已单独成类，不再参与配对
		}
		t := group[0]
		v, ok := voucherByReq[reqID]
		if !ok {
			rep.add(r.channelOnly(ctx, t, from, to))
			continue
		}
		matchedReqs[reqID] = true
		if d, ok := comparePair(v, t); ok {
			rep.add(d)
		} else {
			rep.Matched++
		}
	}
	// 账本有、渠道没有。
	for _, v := range vouchers {
		if matchedReqs[v.RequestID] || dupReqs[v.RequestID] {
			continue
		}
		if _, ok := byReq[v.RequestID]; ok {
			continue
		}
		rep.add(ChannelDiff{
			Kind:         DiffLedgerOnly,
			RequestID:    v.RequestID,
			VoucherID:    v.ID,
			LedgerAmount: v.Amount,
			LedgerTime:   v.CreatedAt.UnixNano(),
			Evidence: fmt.Sprintf("ledger voucher %s (type=%s amount=%d) has no channel txn in window [%d,%d)",
				v.ID, v.Type, v.Amount, from, to),
		})
	}

	// 稳定输出，便于断言与阅读。
	sort.Slice(rep.Diffs, func(i, j int) bool {
		if rep.Diffs[i].RequestID != rep.Diffs[j].RequestID {
			return rep.Diffs[i].RequestID < rep.Diffs[j].RequestID
		}
		return rep.Diffs[i].Kind < rep.Diffs[j].Kind
	})
	rep.Balanced = internal.Balanced && len(rep.Diffs) == 0
	return rep, nil
}

// channelOnly 渠道有、账本没有；若账本里存在该请求但落在窗口外，写入证据说明。
func (r *Reconciler) channelOnly(ctx context.Context, t ChannelTxn, from, to int64) ChannelDiff {
	d := ChannelDiff{
		Kind:          DiffChannelOnly,
		RequestID:     t.RequestID,
		ChannelTxnID:  t.ChannelTxnID,
		ChannelAmount: t.Amount,
		ChannelTime:   t.OccurredAt,
	}
	if v, found, err := r.store.GetVoucherByRequest(ctx, t.RequestID); err == nil && found {
		d.VoucherID = v.ID
		d.LedgerAmount = v.Amount
		d.LedgerTime = v.CreatedAt.UnixNano()
		d.Evidence = fmt.Sprintf("channel txn %s in window [%d,%d) but ledger voucher %s created at %d (outside window)",
			t.ChannelTxnID, from, to, v.ID, d.LedgerTime)
	} else {
		d.Evidence = fmt.Sprintf("channel txn %s (amount=%d direction=%s) has no ledger voucher",
			t.ChannelTxnID, t.Amount, t.Direction)
	}
	return d
}

// channelDupDiff 渠道自身重复：同一业务请求标识多条明细。
func channelDupDiff(reqID string, group []ChannelTxn, vouchers []domain.Voucher) ChannelDiff {
	sort.Slice(group, func(i, j int) bool { return group[i].ChannelTxnID < group[j].ChannelTxnID })
	parts := make([]string, 0, len(group))
	consistent := true
	for i, t := range group {
		parts = append(parts, fmt.Sprintf("%s(amount=%d direction=%s time=%d)",
			t.ChannelTxnID, t.Amount, t.Direction, t.OccurredAt))
		if i > 0 && (t.Amount != group[0].Amount || t.Direction != group[0].Direction) {
			consistent = false
		}
	}
	d := ChannelDiff{
		Kind:          DiffChannelDup,
		RequestID:     reqID,
		ChannelAmount: group[0].Amount,
		ChannelTime:   group[0].OccurredAt,
		Evidence: fmt.Sprintf("channel statement has %d txns for one request (content_consistent=%v): %s",
			len(group), consistent, strings.Join(parts, ", ")),
	}
	for _, v := range vouchers {
		if v.RequestID == reqID {
			d.VoucherID = v.ID
			d.LedgerAmount = v.Amount
			d.LedgerTime = v.CreatedAt.UnixNano()
			break
		}
	}
	return d
}

// comparePair 比较账本凭证与渠道明细的金额与方向；不一致返回差异。
// 方向口径约定：TRANSFER 对应渠道 DEBIT（支付出金），REVERSAL 对应 CREDIT（退款入金）。
func comparePair(v domain.Voucher, t ChannelTxn) (ChannelDiff, bool) {
	wantDir := "DEBIT"
	if v.Type == domain.VoucherReversal {
		wantDir = "CREDIT"
	}
	var problems []string
	if v.Amount != t.Amount {
		problems = append(problems, fmt.Sprintf("amount ledger=%d channel=%d", v.Amount, t.Amount))
	}
	if t.Direction != wantDir {
		problems = append(problems, fmt.Sprintf("direction ledger_type=%s expects %s but channel=%s",
			v.Type, wantDir, t.Direction))
	}
	if len(problems) == 0 {
		return ChannelDiff{}, false
	}
	return ChannelDiff{
		Kind:          DiffFieldMismatch,
		RequestID:     v.RequestID,
		VoucherID:     v.ID,
		ChannelTxnID:  t.ChannelTxnID,
		LedgerAmount:  v.Amount,
		ChannelAmount: t.Amount,
		LedgerTime:    v.CreatedAt.UnixNano(),
		ChannelTime:   t.OccurredAt,
		Evidence:      strings.Join(problems, "; "),
	}, true
}

func (rep *Report) add(d ChannelDiff) {
	rep.Diffs = append(rep.Diffs, d)
	rep.CountByKind[d.Kind]++
}

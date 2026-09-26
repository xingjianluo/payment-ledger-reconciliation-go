package reconcile

import (
	"context"
	"fmt"
	"sort"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/store"
)

// DiffKind 差异分类。对账只报告、不修复。
type DiffKind string

const (
	DiffMissingEntry    DiffKind = "MISSING_ENTRY"      // 缺账：凭证分录在流水中缺失
	DiffDuplicateEntry  DiffKind = "DUPLICATE_ENTRY"    // 重复：同一凭证分录出现多份流水
	DiffAmountMismatch  DiffKind = "AMOUNT_MISMATCH"    // 金额/方向不符：流水与凭证分录不一致
	DiffBalanceMismatch DiffKind = "BALANCE_MISMATCH"   // 余额不符：流水汇总与账户余额不一致
	DiffUnbalanced      DiffKind = "UNBALANCED_VOUCHER" // 凭证自身借贷不守恒
	DiffOrphanEntry     DiffKind = "ORPHAN_ENTRY"       // 流水存在但对应凭证缺失
)

// Diff 单条差异及其判定依据。
type Diff struct {
	Kind      DiffKind
	AccountID string
	VoucherID string
	RequestID string
	Expected  int64
	Actual    int64
	Evidence  string
}

func (d Diff) String() string {
	s := "diff kind=" + string(d.Kind)
	if d.RequestID != "" {
		s += " request_id=" + d.RequestID
	}
	if d.VoucherID != "" {
		s += " voucher_id=" + d.VoucherID
	}
	if d.AccountID != "" {
		s += " account_id=" + d.AccountID
	}
	s += fmt.Sprintf(" expected=%d actual=%d", d.Expected, d.Actual)
	if d.Evidence != "" {
		s += " | evidence: " + d.Evidence
	}
	return s
}

// Options 对账选项。
type Options struct {
	// OpeningBalances 为窗口起点 [from] 的各账户余额；
	// 提供时按 “窗口内流水净额 == 当前余额 - 期初余额” 核对。
	// 为 nil 时按全量流水重算余额并与账户当前余额核对。
	OpeningBalances map[string]int64
}

// Report 对账报告。
type Report struct {
	WindowFrom int64
	WindowTo   int64
	Diffs      []Diff
	// 按类型统计，便于断言。
	CountByKind map[DiffKind]int
	Balanced    bool
}

// Reconciler 对账器：只读核对，不静默忽略、不自行修复。
type Reconciler struct {
	store store.Store
}

func NewReconciler(st store.Store) *Reconciler {
	return &Reconciler{store: st}
}

type entryKey struct {
	voucherID string
	accountID string
	side      domain.EntrySide
}

// Reconcile 在 [from,to) 时间窗口内核对凭证、流水与余额。
// 建议在 ledger.Service.View 内调用以获得快照一致性。
func (r *Reconciler) Reconcile(ctx context.Context, from, to int64, opts *Options) (Report, error) {
	rep := Report{WindowFrom: from, WindowTo: to, CountByKind: map[DiffKind]int{}}

	vouchers, err := r.store.ListVouchers(ctx, from, to)
	if err != nil {
		return rep, err
	}
	entries, err := r.store.ListEntries(ctx, from, to)
	if err != nil {
		return rep, err
	}

	voucherSet := map[string]domain.Voucher{}
	for _, v := range vouchers {
		voucherSet[v.ID] = v
	}

	// actualByKey 收集窗口内每条实际流水（按凭证+账户+方向归组）。
	actualByKey := map[entryKey][]store.LedgerEntry{}
	actualByVoucher := map[string][]store.LedgerEntry{}
	for _, e := range entries {
		k := entryKey{e.VoucherID, e.AccountID, e.Side}
		actualByKey[k] = append(actualByKey[k], e)
		actualByVoucher[e.VoucherID] = append(actualByVoucher[e.VoucherID], e)
		if _, ok := voucherSet[e.VoucherID]; !ok {
			r.add(&rep, Diff{
				Kind:      DiffOrphanEntry,
				AccountID: e.AccountID,
				VoucherID: e.VoucherID,
				RequestID: e.RequestID,
				Actual:    e.Amount,
				Evidence: fmt.Sprintf("entry seq=%d side=%s has no voucher in window [%d,%d)",
					e.Seq, e.Side, from, to),
			})
		}
	}

	// 逐凭证：守恒 + 每条分录与流水一一对应。
	for _, v := range vouchers {
		if !v.Balanced() {
			r.add(&rep, Diff{
				Kind:      DiffUnbalanced,
				VoucherID: v.ID,
				RequestID: v.RequestID,
				Evidence:  fmt.Sprintf("voucher debit/credit not balanced, entries=%d", len(v.Entries)),
			})
		}
		expectedCount := map[entryKey]int64{} // 期望金额（同键多条会累加）
		expectedSeen := map[entryKey]int{}
		for _, e := range v.Entries {
			k := entryKey{v.ID, e.AccountID, e.Side}
			expectedCount[k] += e.Amount
			expectedSeen[k]++
		}
		for k, wantAmount := range expectedCount {
			got := actualByKey[k]
			if len(got) == 0 {
				r.add(&rep, Diff{
					Kind:      DiffMissingEntry,
					AccountID: k.accountID,
					VoucherID: v.ID,
					RequestID: v.RequestID,
					Expected:  wantAmount,
					Evidence: fmt.Sprintf("voucher expects %s entry of %d but no ledger entry found",
						k.side, wantAmount),
				})
				continue
			}
			var gotAmount int64
			for _, ge := range got {
				gotAmount += ge.Amount
			}
			if len(got) > expectedSeen[k] {
				r.add(&rep, Diff{
					Kind:      DiffDuplicateEntry,
					AccountID: k.accountID,
					VoucherID: v.ID,
					RequestID: v.RequestID,
					Expected:  int64(expectedSeen[k]),
					Actual:    int64(len(got)),
					Evidence: fmt.Sprintf("expected %d entry(ies), found %d (seqs=%s)",
						expectedSeen[k], len(got), seqList(got)),
				})
			}
			if gotAmount != wantAmount {
				r.add(&rep, Diff{
					Kind:      DiffAmountMismatch,
					AccountID: k.accountID,
					VoucherID: v.ID,
					RequestID: v.RequestID,
					Expected:  wantAmount,
					Actual:    gotAmount,
					Evidence:  fmt.Sprintf("entry amount differs from voucher, seqs=%s", seqList(got)),
				})
			}
		}
	}

	if err := r.checkBalances(ctx, &rep, entries, opts); err != nil {
		return rep, err
	}

	rep.Balanced = len(rep.Diffs) == 0
	return rep, nil
}

// checkBalances 按流水重算账户余额并与账面余额核对。
func (r *Reconciler) checkBalances(ctx context.Context, rep *Report, windowEntries []store.LedgerEntry, opts *Options) error {
	accounts, err := r.store.ListAccounts(ctx)
	if err != nil {
		return err
	}

	var sums map[string]int64
	if opts != nil && opts.OpeningBalances != nil {
		// 窗口口径：当前余额 - 期初余额 应等于窗口内净额。
		sums = sumEntries(windowEntries)
		for _, acc := range accounts {
			opening := opts.OpeningBalances[acc.ID]
			gotMovement := sums[acc.ID]
			wantMovement := acc.Balance - opening
			if gotMovement != wantMovement {
				r.add(rep, Diff{
					Kind:      DiffBalanceMismatch,
					AccountID: acc.ID,
					Expected:  wantMovement,
					Actual:    gotMovement,
					Evidence: fmt.Sprintf("current_balance=%d opening_balance=%d window_net_entries=%d",
						acc.Balance, opening, gotMovement),
				})
			}
		}
		return nil
	}

	// 全量口径：从全部历史流水重算账户当前余额。
	all, err := r.store.ListEntries(ctx, 0, 1<<62)
	if err != nil {
		return err
	}
	sums = sumEntries(all)
	for _, acc := range accounts {
		if sums[acc.ID] != acc.Balance {
			r.add(rep, Diff{
				Kind:      DiffBalanceMismatch,
				AccountID: acc.ID,
				Expected:  acc.Balance,
				Actual:    sums[acc.ID],
				Evidence: fmt.Sprintf("book_balance=%d but recomputed_from_entries=%d version=%d",
					acc.Balance, sums[acc.ID], acc.Version),
			})
		}
	}
	return nil
}

func (r *Reconciler) add(rep *Report, d Diff) {
	rep.Diffs = append(rep.Diffs, d)
	rep.CountByKind[d.Kind]++
}

func sumEntries(entries []store.LedgerEntry) map[string]int64 {
	out := map[string]int64{}
	for _, e := range entries {
		if e.Side == domain.Debit {
			out[e.AccountID] += e.Amount
		} else {
			out[e.AccountID] -= e.Amount
		}
	}
	return out
}

func seqList(es []store.LedgerEntry) string {
	seqs := make([]int64, 0, len(es))
	for _, e := range es {
		seqs = append(seqs, e.Seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	s := "["
	for i, n := range seqs {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf("%d", n)
	}
	return s + "]"
}

// Package reconcile 基于时间窗口对账本凭证、外部渠道流水与账户余额进行对账，
// 输出明确分类的差异。对账只报告差异，绝不静默忽略，也绝不自行改账修复。
//
// 对账口径（窗口 [WindowStart, WindowEnd)，左闭右开）：
//
//  1. 凭证守恒：每张凭证借方合计必须等于贷方合计，否则报“凭证借贷不守恒”。
//  2. 业务配对：转账、冲正凭证与外部渠道流水按业务幂等标识 RequestID 一一配对
//     （入金凭证属于系统内部账务，不参与外部流水配对）。
//     - 外部有、账本无：缺账；
//     - 账本有、外部无：多账；
//     - 同一 RequestID 外部流水出现多次：重复；
//     - 双方都有但金额或收付方不一致：金额不符。
//  3. 余额核对：对每个给出期末余额的账户，重放窗口内全部凭证得到净额，
//     校验 期初余额 + 窗口净额 == 期末余额，不一致报“余额不符”
//     （典型成因：并发丢失更新或凭证/流水被篡改）。
package reconcile

import (
	"fmt"
	"sort"
	"time"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
)

// Input 是一次对账所需的全部输入。
type Input struct {
	WindowStart time.Time
	WindowEnd   time.Time
	Vouchers    []*domain.Voucher     // 窗口内账本凭证（含冲正、入金）
	Flows       []domain.ExternalFlow // 窗口内外部渠道流水
	// StartingBalances 为窗口期初账户余额快照（可选，缺省按 0 处理）。
	StartingBalances map[string]int64
	// EndingBalances 为窗口期末账户余额快照。对账将对其中出现的每个账户做余额核对。
	EndingBalances map[string]int64
}

type flowGroup struct {
	first domain.ExternalFlow
	extra []domain.ExternalFlow
}

// Reconcile 执行对账并返回差异报告。
func Reconcile(in Input) *domain.ReconcileReport {
	report := &domain.ReconcileReport{WindowStart: in.WindowStart, WindowEnd: in.WindowEnd}

	// 1. 窗口过滤（左闭右开）。
	var vouchers []*domain.Voucher
	voucherByID := map[string]*domain.Voucher{}
	for _, v := range in.Vouchers {
		if inWindow(v.PostedAt, in.WindowStart, in.WindowEnd) {
			vouchers = append(vouchers, v)
			voucherByID[v.ID] = v
		}
	}
	var flows []domain.ExternalFlow
	for _, f := range in.Flows {
		if inWindow(f.OccurredAt, in.WindowStart, in.WindowEnd) {
			flows = append(flows, f)
		}
	}

	// 2. 凭证借贷守恒检查。
	sort.Slice(vouchers, func(i, j int) bool { return vouchers[i].ID < vouchers[j].ID })
	for _, v := range vouchers {
		debit, credit := debitCredit(v)
		if debit != credit {
			report.Issues = append(report.Issues, domain.Issue{
				Type:      domain.IssueUnbalancedVoucher,
				RequestID: v.RequestID,
				VoucherID: v.ID,
				Detail: fmt.Sprintf("凭证 %s（类型=%s, 请求标识=%s）借方合计=%d 分，贷方合计=%d 分，借贷不守恒",
					v.ID, v.Type, v.RequestID, debit, credit),
				Expected: credit,
				Actual:   debit,
			})
		}
	}

	// 3. 业务凭证（转账/冲正）按 RequestID 建索引。
	bizVouchers := map[string]*domain.Voucher{}
	var bizVoucherIDs []string
	for _, v := range vouchers {
		if v.Type == domain.VoucherTransfer || v.Type == domain.VoucherReversal {
			if _, exists := bizVouchers[v.RequestID]; !exists {
				bizVoucherIDs = append(bizVoucherIDs, v.RequestID)
			}
			bizVouchers[v.RequestID] = v
		}
	}

	// 4. 外部流水按 RequestID 分组，识别重复。
	groups := map[string]*flowGroup{}
	var requestIDs []string
	for _, f := range flows {
		g, ok := groups[f.RequestID]
		if !ok {
			g = &flowGroup{first: f}
			groups[f.RequestID] = g
			requestIDs = append(requestIDs, f.RequestID)
			continue
		}
		g.extra = append(g.extra, f)
	}
	sort.Strings(requestIDs)
	sort.Strings(bizVoucherIDs)

	for _, rid := range requestIDs {
		g := groups[rid]
		for _, dup := range g.extra {
			report.Issues = append(report.Issues, domain.Issue{
				Type:      domain.IssueDuplicateEntry,
				RequestID: rid,
				Detail: fmt.Sprintf("请求标识=%s 的外部流水重复出现：渠道流水号 %q 与首次流水号 %q 重复，金额=%d 分",
					rid, dup.ExternalID, g.first.ExternalID, dup.Amount),
				Expected: 1,
				Actual:   int64(1 + len(g.extra)),
			})
		}

		v, ok := bizVouchers[rid]
		if !ok {
			report.Issues = append(report.Issues, domain.Issue{
				Type:      domain.IssueMissingEntry,
				RequestID: rid,
				Detail: fmt.Sprintf("请求标识=%s 外部渠道流水存在（流水号=%q, 金额=%d 分, 付款方=%s, 收款方=%s），但窗口内无对应账本凭证",
					rid, g.first.ExternalID, g.first.Amount, g.first.FromID, g.first.ToID),
				Expected: g.first.Amount,
				Actual:   0,
			})
			continue
		}

		// 金额与收付方核对。
		debit, credit := debitCredit(v)
		var voucherAmount int64
		for _, e := range v.Entries {
			voucherAmount = e.Amount // 守恒前提下两条分录金额相同
			break
		}
		if voucherAmount != g.first.Amount {
			report.Issues = append(report.Issues, domain.Issue{
				Type:      domain.IssueAmountMismatch,
				RequestID: rid,
				VoucherID: v.ID,
				Detail: fmt.Sprintf("请求标识=%s 金额不符：外部流水=%d 分，账本凭证 %s 金额=%d 分（借=%d, 贷=%d）",
					rid, g.first.Amount, v.ID, voucherAmount, debit, credit),
				Expected: g.first.Amount,
				Actual:   voucherAmount,
			})
			continue
		}
		if reason, ok := partiesMismatch(v, g.first, voucherByID); !ok {
			report.Issues = append(report.Issues, domain.Issue{
				Type:      domain.IssueAmountMismatch,
				RequestID: rid,
				VoucherID: v.ID,
				Detail: fmt.Sprintf("请求标识=%s 收付方不符：外部流水付款方=%s 收款方=%s；%s（金额一致=%d 分）",
					rid, g.first.FromID, g.first.ToID, reason, g.first.Amount),
				Expected: g.first.Amount,
				Actual:   voucherAmount,
			})
		}
	}

	// 5. 账本有、外部无：多账。
	for _, rid := range bizVoucherIDs {
		if _, ok := groups[rid]; ok {
			continue
		}
		v := bizVouchers[rid]
		var amount int64
		for _, e := range v.Entries {
			amount = e.Amount
			break
		}
		report.Issues = append(report.Issues, domain.Issue{
			Type:      domain.IssueUnexpectedEntry,
			RequestID: rid,
			VoucherID: v.ID,
			Detail: fmt.Sprintf("请求标识=%s 账本凭证 %s（类型=%s, 金额=%d 分, 过账时间=%s）在外部渠道流水中无对应记录",
				rid, v.ID, v.Type, amount, v.PostedAt.Format(time.RFC3339)),
			Expected: 0,
			Actual:   amount,
		})
	}

	// 6. 余额核对：期初 + 窗口内全部凭证净额 == 期末。
	netByAccount := map[string]int64{}
	for _, v := range vouchers {
		for _, e := range v.Entries {
			netByAccount[e.AccountID] += e.SignedAmount()
		}
	}
	var accounts []string
	for id := range in.EndingBalances {
		accounts = append(accounts, id)
	}
	sort.Strings(accounts)
	for _, id := range accounts {
		start := in.StartingBalances[id]
		got := start + netByAccount[id]
		want := in.EndingBalances[id]
		if got != want {
			report.Issues = append(report.Issues, domain.Issue{
				Type:      domain.IssueBalanceMismatch,
				AccountID: id,
				Detail: fmt.Sprintf("账户 %s 余额不符：期初=%d 分 + 窗口凭证净额=%d 分 = %d 分，期末快照=%d 分，差额=%d 分（可能存在丢失更新或流水/余额被篡改）",
					id, start, netByAccount[id], got, want, got-want),
				Expected: want,
				Actual:   got,
			})
		}
	}

	return report
}

func inWindow(t, start, end time.Time) bool {
	return !t.Before(start) && t.Before(end)
}

func debitCredit(v *domain.Voucher) (debit, credit int64) {
	for _, e := range v.Entries {
		if e.Direction == domain.Debit {
			debit += e.Amount
		} else {
			credit += e.Amount
		}
	}
	return debit, credit
}

// partiesMismatch 校验收付方。转账：外部 from/to 对应凭证贷/借方账户。
// 冲正：外部冲正流水描述的是原始转账业务，若原始凭证也在窗口内则按其收付方核对，
// 否则（原始凭证跨窗口）只核对金额、跳过收付方核对。
// 返回空字符串表示一致；否则返回判定依据。
func partiesMismatch(v *domain.Voucher, f domain.ExternalFlow, voucherByID map[string]*domain.Voucher) (string, bool) {
	if v.Type == domain.VoucherReversal {
		orig, ok := voucherByID[v.ReverseOf]
		if !ok {
			return "", true
		}
		return partiesOfTransfer(orig, f)
	}
	return partiesOfTransfer(v, f)
}

func partiesOfTransfer(v *domain.Voucher, f domain.ExternalFlow) (string, bool) {
	var fromAcct, toAcct string
	for _, e := range v.Entries {
		if e.Direction == domain.Credit {
			fromAcct = e.AccountID
		} else {
			toAcct = e.AccountID
		}
	}
	if fromAcct == f.FromID && toAcct == f.ToID {
		return "", true
	}
	return fmt.Sprintf("账本凭证 %s 付款方=%s 收款方=%s", v.ID, fromAcct, toAcct), false
}

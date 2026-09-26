package reconcile

import (
	"strings"
	"testing"
	"time"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
)

var (
	t0 = time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	t1 = t0.Add(time.Hour)
	t2 = t0.Add(2 * time.Hour)
	t3 = t0.Add(3 * time.Hour)
)

// transferVoucher 构造一张转账凭证：借 to、贷 from。
func transferVoucher(id, reqID, from, to string, amount int64, at time.Time) *domain.Voucher {
	return &domain.Voucher{
		ID: id, Type: domain.VoucherTransfer, RequestID: reqID, PostedAt: at,
		Entries: []domain.Entry{
			{VoucherID: id, AccountID: to, Direction: domain.Debit, Amount: amount},
			{VoucherID: id, AccountID: from, Direction: domain.Credit, Amount: amount},
		},
	}
}

func reversalVoucher(id, reqID, origID, from, to string, amount int64, at time.Time) *domain.Voucher {
	return &domain.Voucher{
		ID: id, Type: domain.VoucherReversal, RequestID: reqID, ReverseOf: origID, PostedAt: at,
		Entries: []domain.Entry{
			{VoucherID: id, AccountID: from, Direction: domain.Debit, Amount: amount},
			{VoucherID: id, AccountID: to, Direction: domain.Credit, Amount: amount},
		},
	}
}

func flow(extID, reqID, from, to string, amount int64, at time.Time) domain.ExternalFlow {
	return domain.ExternalFlow{
		ExternalID: extID, RequestID: reqID, FromID: from, ToID: to, Amount: amount, OccurredAt: at,
	}
}

func findIssue(issues []domain.Issue, typ domain.IssueType, requestID string) *domain.Issue {
	for i := range issues {
		if issues[i].Type == typ && issues[i].RequestID == requestID {
			return &issues[i]
		}
	}
	return nil
}

func TestCleanReconcile(t *testing.T) {
	vouchers := []*domain.Voucher{
		transferVoucher("TV-1", "req-1", "A", "B", 1000, t1),
		transferVoucher("TV-2", "req-2", "B", "C", 400, t2),
	}
	flows := []domain.ExternalFlow{
		flow("E1", "req-1", "A", "B", 1000, t1),
		flow("E2", "req-2", "B", "C", 400, t2),
	}
	in := Input{
		WindowStart: t0, WindowEnd: t3, Vouchers: vouchers, Flows: flows,
		StartingBalances: map[string]int64{"A": 5000, "B": 2000, "C": 0},
		EndingBalances:   map[string]int64{"A": 4000, "B": 2600, "C": 400},
	}
	rep := Reconcile(in)
	if !rep.Balanced() {
		for _, is := range rep.Issues {
			t.Errorf("干净数据不应有差异，实际: 类型=%s %s", is.Type, is.Detail)
		}
	}
}

func TestReversalMatchesOriginalFlow(t *testing.T) {
	orig := transferVoucher("TV-1", "req-pay", "A", "B", 800, t1)
	rv := reversalVoucher("RV-1", "req-rev", "TV-1", "A", "B", 800, t2)
	flows := []domain.ExternalFlow{
		flow("E1", "req-pay", "A", "B", 800, t1),
		// 外部冲正流水沿用原始业务的收付方方向。
		flow("E2", "req-rev", "A", "B", 800, t2),
	}
	in := Input{
		WindowStart: t0, WindowEnd: t3,
		Vouchers: []*domain.Voucher{orig, rv}, Flows: flows,
		StartingBalances: map[string]int64{"A": 5000, "B": 0},
		EndingBalances:   map[string]int64{"A": 5000, "B": 0}, // 转账后又冲正，余额还原
	}
	rep := Reconcile(in)
	if !rep.Balanced() {
		for _, is := range rep.Issues {
			t.Errorf("转账+冲正应全部配对，实际: 类型=%s %s", is.Type, is.Detail)
		}
	}
}

func TestMissingAndUnexpectedEntries(t *testing.T) {
	vouchers := []*domain.Voucher{
		transferVoucher("TV-1", "req-ledger-only", "A", "B", 300, t1),
	}
	flows := []domain.ExternalFlow{
		flow("E1", "req-ledger-only", "A", "B", 300, t1),
		flow("E2", "req-flow-only", "A", "C", 700, t2), // 账本里没有
	}
	rep := Reconcile(Input{WindowStart: t0, WindowEnd: t3, Vouchers: vouchers, Flows: flows})

	if is := findIssue(rep.Issues, domain.IssueMissingEntry, "req-flow-only"); is == nil {
		t.Fatal("缺账未被识别")
	} else if !strings.Contains(is.Detail, "req-flow-only") || !strings.Contains(is.Detail, "无对应账本凭证") {
		t.Errorf("缺账依据不足: %s", is.Detail)
	}

	// 再造一条多账：账本有、外部无。
	rep2 := Reconcile(Input{
		WindowStart: t0, WindowEnd: t3,
		Vouchers: append(vouchers, transferVoucher("TV-2", "req-ghost", "C", "A", 50, t2)),
		Flows:    flows,
	})
	if is := findIssue(rep2.Issues, domain.IssueUnexpectedEntry, "req-ghost"); is == nil {
		t.Fatal("多账未被识别")
	} else if !strings.Contains(is.Detail, "TV-2") || !strings.Contains(is.Detail, "无对应记录") {
		t.Errorf("多账依据不足: %s", is.Detail)
	}
}

func TestDuplicateFlow(t *testing.T) {
	vouchers := []*domain.Voucher{transferVoucher("TV-1", "req-1", "A", "B", 1000, t1)}
	flows := []domain.ExternalFlow{
		flow("E1", "req-1", "A", "B", 1000, t1),
		flow("E1-DUP", "req-1", "A", "B", 1000, t1.Add(time.Minute)),
	}
	rep := Reconcile(Input{WindowStart: t0, WindowEnd: t3, Vouchers: vouchers, Flows: flows})
	is := findIssue(rep.Issues, domain.IssueDuplicateEntry, "req-1")
	if is == nil {
		t.Fatal("重复流水未被识别")
	}
	if !strings.Contains(is.Detail, "E1-DUP") || !strings.Contains(is.Detail, "重复") {
		t.Errorf("重复依据不足: %s", is.Detail)
	}
	// 重复不等于缺账：req-1 同时配对成功，不应再报缺账/多账。
	if findIssue(rep.Issues, domain.IssueMissingEntry, "req-1") != nil {
		t.Error("已配对的重复请求不应同时被判为缺账")
	}
}

func TestAmountMismatch(t *testing.T) {
	vouchers := []*domain.Voucher{transferVoucher("TV-1", "req-amt", "A", "B", 1000, t1)}
	flows := []domain.ExternalFlow{flow("E1", "req-amt", "A", "B", 900, t1)}
	rep := Reconcile(Input{WindowStart: t0, WindowEnd: t3, Vouchers: vouchers, Flows: flows})
	is := findIssue(rep.Issues, domain.IssueAmountMismatch, "req-amt")
	if is == nil {
		t.Fatal("金额不符未被识别")
	}
	if is.Expected != 900 || is.Actual != 1000 {
		t.Errorf("金额不符期望/实际 = %d/%d", is.Expected, is.Actual)
	}
	if !strings.Contains(is.Detail, "req-amt") {
		t.Errorf("差异信息须含请求标识: %s", is.Detail)
	}
}

func TestPartiesMismatch(t *testing.T) {
	// 账本 A->B，外部 A->C，金额一致。
	vouchers := []*domain.Voucher{transferVoucher("TV-1", "req-party", "A", "B", 500, t1)}
	flows := []domain.ExternalFlow{flow("E1", "req-party", "A", "C", 500, t1)}
	rep := Reconcile(Input{WindowStart: t0, WindowEnd: t3, Vouchers: vouchers, Flows: flows})
	is := findIssue(rep.Issues, domain.IssueAmountMismatch, "req-party")
	if is == nil {
		t.Fatal("收付方不符未被识别（归入金额不符类）")
	}
	if !strings.Contains(is.Detail, "收付方不符") || !strings.Contains(is.Detail, "req-party") {
		t.Errorf("收付方不符依据不足: %s", is.Detail)
	}
}

func TestUnbalancedVoucher(t *testing.T) {
	bad := &domain.Voucher{
		ID: "TV-BAD", Type: domain.VoucherTransfer, RequestID: "req-bad", PostedAt: t1,
		Entries: []domain.Entry{
			{VoucherID: "TV-BAD", AccountID: "B", Direction: domain.Debit, Amount: 1000},
			{VoucherID: "TV-BAD", AccountID: "A", Direction: domain.Credit, Amount: 900},
		},
	}
	rep := Reconcile(Input{
		WindowStart: t0, WindowEnd: t3,
		Vouchers: []*domain.Voucher{bad},
		Flows:    []domain.ExternalFlow{flow("E1", "req-bad", "A", "B", 1000, t1)},
	})
	is := findIssue(rep.Issues, domain.IssueUnbalancedVoucher, "req-bad")
	if is == nil {
		t.Fatal("借贷不守恒凭证未被识别")
	}
	if is.Expected != 900 || is.Actual != 1000 {
		t.Errorf("守恒差异期望借=%d 贷=%d", is.Expected, is.Actual)
	}
}

func TestBalanceMismatchDetectsTamper(t *testing.T) {
	vouchers := []*domain.Voucher{transferVoucher("TV-1", "req-1", "A", "B", 1000, t1)}
	flows := []domain.ExternalFlow{flow("E1", "req-1", "A", "B", 1000, t1)}
	// A: 5000-1000=4000；期末快照被篡改为 3999，制造 1 分丢失更新。
	in := Input{
		WindowStart: t0, WindowEnd: t3, Vouchers: vouchers, Flows: flows,
		StartingBalances: map[string]int64{"A": 5000, "B": 0},
		EndingBalances:   map[string]int64{"A": 3999, "B": 1000},
	}
	rep := Reconcile(in)
	is := findIssue(rep.Issues, domain.IssueBalanceMismatch, "")
	if is == nil || is.AccountID != "A" {
		t.Fatalf("应识别 A 账户余额不符，实际 issues=%v", rep.Issues)
	}
	if is.Expected != 3999 || is.Actual != 4000 {
		t.Errorf("余额不符 期望=%d 实际=%d", is.Expected, is.Actual)
	}
	if !strings.Contains(is.Detail, "差额=1") {
		t.Errorf("余额差异须给出差额与判定依据: %s", is.Detail)
	}
}

func TestWindowFilteringHalfOpen(t *testing.T) {
	vBefore := transferVoucher("TV-0", "req-before", "A", "B", 100, t0) // 恰好等于 start：包含
	vAfter := transferVoucher("TV-9", "req-after", "A", "B", 100, t3)   // 恰好等于 end：排除
	vMiddle := transferVoucher("TV-1", "req-mid", "A", "B", 200, t1)
	in := Input{
		WindowStart: t0, WindowEnd: t3,
		Vouchers: []*domain.Voucher{vBefore, vMiddle, vAfter},
		Flows: []domain.ExternalFlow{
			flow("E0", "req-before", "A", "B", 100, t0),
			flow("E1", "req-mid", "A", "B", 200, t1),
			flow("E9", "req-after", "A", "B", 100, t3),
		},
	}
	rep := Reconcile(in)
	// req-before / req-mid 都在窗口内且配对；req-after 在右边界外被排除，不产生任何差异。
	if !rep.Balanced() {
		for _, is := range rep.Issues {
			t.Errorf("窗口过滤后不应有差异: 类型=%s %s", is.Type, is.Detail)
		}
	}
}

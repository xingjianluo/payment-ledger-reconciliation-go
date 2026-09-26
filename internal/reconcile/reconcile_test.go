package reconcile_test

import (
	"context"
	"testing"
	"time"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/ledger"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/reconcile"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/store"
)

func setup(t *testing.T, opening map[string]int64) (*ledger.Service, *store.MemoryStore, *reconcile.Reconciler, int64, int64) {
	t.Helper()
	st := store.NewMemoryStore()
	svc := ledger.NewService(st)
	ctx := context.Background()
	for id, bal := range map[string]int64{"A": opening["A"], "B": opening["B"], "C": opening["C"]} {
		if err := svc.CreateAccount(ctx, domain.Account{ID: id, Name: id, Balance: bal}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-1", FromID: "A", ToID: "B", Amount: 300}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-2", FromID: "A", ToID: "B", Amount: 120}); err != nil {
		t.Fatal(err)
	}
	return svc, st, reconcile.NewReconciler(st),
		time.Now().Add(-time.Hour).UnixNano(), time.Now().Add(time.Hour).UnixNano()
}

func runReconcile(t *testing.T, svc *ledger.Service, rec *reconcile.Reconciler, from, to int64, opening map[string]int64) reconcile.Report {
	t.Helper()
	var report reconcile.Report
	err := svc.View(context.Background(), func(ctx context.Context) error {
		var e error
		report, e = rec.Reconcile(ctx, from, to, &reconcile.Options{OpeningBalances: opening})
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func assertDiff(t *testing.T, rep reconcile.Report, kind reconcile.DiffKind, wantCount int) {
	t.Helper()
	if got := rep.CountByKind[kind]; got != wantCount {
		var details []string
		for _, d := range rep.Diffs {
			details = append(details, d.String())
		}
		t.Fatalf("diff kind=%s count=%d want=%d; all diffs: %v", kind, got, wantCount, details)
	}
}

func TestReconcile_Clean(t *testing.T) {
	opening := map[string]int64{"A": 1000, "B": 100, "C": 0}
	svc, _, rec, from, to := setup(t, opening)
	rep := runReconcile(t, svc, rec, from, to, opening)
	if !rep.Balanced {
		for _, d := range rep.Diffs {
			t.Errorf("unexpected diff: %s", d.String())
		}
	}
}

func TestReconcile_MissingEntry(t *testing.T) {
	opening := map[string]int64{"A": 1000, "B": 100, "C": 0}
	svc, st, rec, from, to := setup(t, opening)

	v, ok, err := st.GetVoucherByRequest(context.Background(), "T-1")
	if err != nil || !ok {
		t.Fatal("voucher T-1 not found")
	}
	if n := st.CorruptDropEntries(v.ID); n != 2 {
		t.Fatalf("dropped %d entries, want 2", n)
	}

	rep := runReconcile(t, svc, rec, from, to, opening)
	assertDiff(t, rep, reconcile.DiffMissingEntry, 2)
	// 缺账同时导致流水侧余额对不上
	if rep.Balanced {
		t.Fatal("report must not be balanced")
	}
}

func TestReconcile_DuplicateEntry(t *testing.T) {
	opening := map[string]int64{"A": 1000, "B": 100, "C": 0}
	svc, st, rec, from, to := setup(t, opening)

	v, _, _ := st.GetVoucherByRequest(context.Background(), "T-2")
	if n := st.CorruptDuplicateEntries(v.ID); n != 2 {
		t.Fatalf("duplicated %d entries, want 2", n)
	}

	rep := runReconcile(t, svc, rec, from, to, opening)
	assertDiff(t, rep, reconcile.DiffDuplicateEntry, 2)
	if rep.Balanced {
		t.Fatal("report must not be balanced")
	}
}

func TestReconcile_AmountMismatch(t *testing.T) {
	opening := map[string]int64{"A": 1000, "B": 100, "C": 0}
	svc, st, rec, from, to := setup(t, opening)

	v, _, _ := st.GetVoucherByRequest(context.Background(), "T-1")
	if !st.CorruptEntryAmount(v.ID, 301) {
		t.Fatal("corruption failed")
	}

	rep := runReconcile(t, svc, rec, from, to, opening)
	assertDiff(t, rep, reconcile.DiffAmountMismatch, 1)
	// 301 vs 300：账面余额与流水净额差 1
	assertDiff(t, rep, reconcile.DiffBalanceMismatch, 1)
	for _, d := range rep.Diffs {
		if d.Kind == reconcile.DiffAmountMismatch {
			if d.Expected != 300 || d.Actual != 301 {
				t.Fatalf("evidence numbers wrong: expected=%d actual=%d", d.Expected, d.Actual)
			}
			t.Logf("amount mismatch evidence: %s", d.Evidence)
		}
	}
}

func TestReconcile_OrphanEntry(t *testing.T) {
	opening := map[string]int64{"A": 1000, "B": 100, "C": 0}
	svc, st, rec, from, to := setup(t, opening)

	v, _, _ := st.GetVoucherByRequest(context.Background(), "T-1")
	if !st.CorruptOrphanEntry(v.ID, "V-FAKE-999") {
		t.Fatal("corruption failed")
	}

	rep := runReconcile(t, svc, rec, from, to, opening)
	assertDiff(t, rep, reconcile.DiffOrphanEntry, 1)
}

func TestReconcile_BalanceMismatchOnly(t *testing.T) {
	opening := map[string]int64{"A": 1000, "B": 100, "C": 0}
	svc, st, rec, from, to := setup(t, opening)

	// 仅篡改账面余额：凭证、流水本身都一致
	if !st.CorruptBalance("A", 9999) {
		t.Fatal("corruption failed")
	}
	rep := runReconcile(t, svc, rec, from, to, opening)
	assertDiff(t, rep, reconcile.DiffBalanceMismatch, 1)
	assertDiff(t, rep, reconcile.DiffMissingEntry, 0)
	assertDiff(t, rep, reconcile.DiffDuplicateEntry, 0)
	assertDiff(t, rep, reconcile.DiffAmountMismatch, 0)
	assertDiff(t, rep, reconcile.DiffOrphanEntry, 0)

	var found bool
	for _, d := range rep.Diffs {
		if d.AccountID == "A" {
			found = true
			t.Logf("balance mismatch: %s", d.String())
		}
	}
	if !found {
		t.Fatal("balance mismatch must identify account A")
	}
}

func TestReconcile_WindowExcludesOutside(t *testing.T) {
	opening := map[string]int64{"A": 1000, "B": 100, "C": 0}
	svc, _, rec, _, _ := setup(t, opening)

	// 空窗口：无凭证无流水；期初取当前余额则净额应为 0
	a, _ := svc.GetAccount(context.Background(), "A")
	b, _ := svc.GetAccount(context.Background(), "B")
	c, _ := svc.GetAccount(context.Background(), "C")
	now := time.Now()
	from := now.Add(time.Hour).UnixNano()
	to := now.Add(2 * time.Hour).UnixNano()
	rep := runReconcile(t, svc, rec, from, to,
		map[string]int64{"A": a.Balance, "B": b.Balance, "C": c.Balance})
	if !rep.Balanced {
		t.Fatalf("future window must be clean: %v", rep.Diffs)
	}
}

func TestReconcile_UnbalancedVoucher(t *testing.T) {
	opening := map[string]int64{"A": 1000, "B": 100, "C": 0}
	svc, st, rec, from, to := setup(t, opening)
	ctx := context.Background()

	// 直接落一张借贷不守恒的凭证：借 50 / 贷 30
	now := time.Now().UTC()
	bad := domain.Voucher{
		ID: "VBAD-1", Type: domain.VoucherTransfer, RequestID: "BAD-1",
		Amount: 50, FromAccountID: "A", ToAccountID: "B", CreatedAt: now,
		Entries: []domain.Entry{
			{AccountID: "B", Side: domain.Debit, Amount: 50},
			{AccountID: "A", Side: domain.Credit, Amount: 30},
		},
	}
	if err := st.PutVoucher(ctx, bad); err != nil {
		t.Fatal(err)
	}
	ts := now.UnixNano()
	if err := st.AppendEntry(ctx, store.LedgerEntry{VoucherID: bad.ID, RequestID: bad.RequestID,
		AccountID: "B", Side: domain.Debit, Amount: 50, OccurredAt: ts, Kind: bad.Type}); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendEntry(ctx, store.LedgerEntry{VoucherID: bad.ID, RequestID: bad.RequestID,
		AccountID: "A", Side: domain.Credit, Amount: 30, OccurredAt: ts, Kind: bad.Type}); err != nil {
		t.Fatal(err)
	}

	rep := runReconcile(t, svc, rec, from, to, opening)
	assertDiff(t, rep, reconcile.DiffUnbalanced, 1)
}

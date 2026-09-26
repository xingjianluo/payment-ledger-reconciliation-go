package ledger_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/ledger"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/store"
)

func testService(t *testing.T) (*ledger.Service, *store.MemoryStore) {
	t.Helper()
	st := store.NewMemoryStore()
	svc := ledger.NewService(st)
	ctx := context.Background()
	must(t, svc.CreateAccount(ctx, domain.Account{ID: "A", Name: "a", Balance: 1000}))
	must(t, svc.CreateAccount(ctx, domain.Account{ID: "B", Name: "b", Balance: 100}))
	return svc, st
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertKind(t *testing.T, err error, want domain.FailureKind) {
	t.Helper()
	le, ok := domain.AsLedgerError(err)
	if !ok {
		t.Fatalf("want ledger error kind=%s, got %v", want, err)
	}
	if le.Kind != want {
		t.Fatalf("want kind=%s, got kind=%s (request_id=%s evidence=%s)",
			want, le.Kind, le.RequestID, le.Evidence)
	}
}

func TestTransfer_IdempotentReplay(t *testing.T) {
	svc, _ := testService(t)
	ctx := context.Background()
	req := domain.TransferRequest{RequestID: "T-1", FromID: "A", ToID: "B", Amount: 300}

	v1, err := svc.Transfer(ctx, req)
	must(t, err)
	v2, err := svc.Transfer(ctx, req)
	must(t, err)
	if v1.ID != v2.ID {
		t.Fatalf("replay must return same voucher: %s vs %s", v1.ID, v2.ID)
	}

	a, _ := svc.GetAccount(ctx, "A")
	b, _ := svc.GetAccount(ctx, "B")
	if a.Balance != 700 || b.Balance != 400 {
		t.Fatalf("double posting detected: A=%d B=%d", a.Balance, b.Balance)
	}
	if !v1.Balanced() {
		t.Fatal("voucher not balanced")
	}
}

func TestTransfer_ContentConflict(t *testing.T) {
	svc, _ := testService(t)
	ctx := context.Background()
	_, err := svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-9", FromID: "A", ToID: "B", Amount: 100})
	must(t, err)

	// 相同 requestID，金额不同
	_, err = svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-9", FromID: "A", ToID: "B", Amount: 250})
	assertKind(t, err, domain.KindIdempotentMiss)
	t.Logf("conflict rejection: %v", err)

	// 相同 requestID，收款方不同
	_, err = svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-9", FromID: "A", ToID: "X", Amount: 100})
	assertKind(t, err, domain.KindIdempotentMiss)

	// 被拒绝后，原始内容重放仍应拿到原始凭证
	v, err := svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-9", FromID: "A", ToID: "B", Amount: 100})
	must(t, err)
	if v.Amount != 100 {
		t.Fatalf("original replay altered: amount=%d", v.Amount)
	}
}

func TestTransfer_InsufficientFundsNoPartialPosting(t *testing.T) {
	svc, st := testService(t)
	ctx := context.Background()

	beforeA, _ := svc.GetAccount(ctx, "A")
	beforeB, _ := svc.GetAccount(ctx, "B")
	nBefore := len(mustListEntries(t, st, ctx))

	_, err := svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-2", FromID: "A", ToID: "B", Amount: 10_000})
	assertKind(t, err, domain.KindInsufficient)

	afterA, _ := svc.GetAccount(ctx, "A")
	afterB, _ := svc.GetAccount(ctx, "B")
	if afterA.Balance != beforeA.Balance || afterB.Balance != beforeB.Balance {
		t.Fatalf("partial posting on insufficient: A %d->%d, B %d->%d",
			beforeA.Balance, afterA.Balance, beforeB.Balance, afterB.Balance)
	}
	if n := len(mustListEntries(t, st, ctx)); n != nBefore {
		t.Fatalf("entries created despite rejection: %d -> %d", nBefore, n)
	}

	// 确定性失败也幂等：重复提交得到相同结论
	_, err = svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-2", FromID: "A", ToID: "B", Amount: 10_000})
	assertKind(t, err, domain.KindInsufficient)
}

func TestTransfer_InactiveAccount(t *testing.T) {
	svc, _ := testService(t)
	ctx := context.Background()
	must(t, svc.CreateAccount(ctx, domain.Account{ID: "F", Name: "frozen", Balance: 999, Status: domain.AccountFrozen}))

	beforeA, _ := svc.GetAccount(ctx, "A")
	_, err := svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-3", FromID: "A", ToID: "F", Amount: 10})
	assertKind(t, err, domain.KindAccountInactive)
	afterA, _ := svc.GetAccount(ctx, "A")
	if afterA.Balance != beforeA.Balance {
		t.Fatal("posted to frozen destination")
	}

	_, err = svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-4", FromID: "F", ToID: "A", Amount: 10})
	assertKind(t, err, domain.KindAccountInactive)
}

func TestTransfer_InvalidRequests(t *testing.T) {
	svc, _ := testService(t)
	ctx := context.Background()
	cases := []domain.TransferRequest{
		{RequestID: "", FromID: "A", ToID: "B", Amount: 1},
		{RequestID: "x", FromID: "A", ToID: "A", Amount: 1},
		{RequestID: "x", FromID: "A", ToID: "B", Amount: 0},
		{RequestID: "x", FromID: "A", ToID: "B", Amount: -5},
		{RequestID: "x", FromID: "A", ToID: "GHOST", Amount: 1},
	}
	for i, c := range cases {
		_, err := svc.Transfer(ctx, c)
		if err == nil {
			t.Fatalf("case %d expected error", i)
		}
	}
}

func TestConcurrent_DuplicateSubmit(t *testing.T) {
	svc, st := testService(t)
	ctx := context.Background()

	const goroutines = 64
	var wg sync.WaitGroup
	var mu sync.Mutex
	voucherIDs := map[string]int{}
	errs := map[domain.FailureKind]int{}

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			v, err := svc.Transfer(ctx, domain.TransferRequest{
				RequestID: "CONC-1", FromID: "A", ToID: "B", Amount: 250,
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if le, ok := domain.AsLedgerError(err); ok {
					errs[le.Kind]++
				} else {
					t.Errorf("unexpected non-ledger error: %v", err)
				}
				return
			}
			voucherIDs[v.ID]++
		}()
	}
	wg.Wait()

	if len(errs) != 0 {
		t.Fatalf("concurrent replays must succeed, got errors: %v", errs)
	}
	if len(voucherIDs) != 1 || voucherIDs[idOf(voucherIDs)] != goroutines {
		t.Fatalf("exactly one voucher must be posted, got %v", voucherIDs)
	}

	a, _ := svc.GetAccount(ctx, "A")
	b, _ := svc.GetAccount(ctx, "B")
	if a.Balance != 750 || b.Balance != 350 {
		t.Fatalf("lost update / double posting: A=%d B=%d (want 750/350)", a.Balance, b.Balance)
	}
	// 成功凭证恰好展开为借/贷各 1 条流水
	es := mustListEntries(t, st, ctx)
	if len(es) != 2 {
		t.Fatalf("want 2 ledger entries for one voucher, got %d", len(es))
	}
	var debit, credit int64
	for _, e := range es {
		if e.Side == domain.Debit {
			debit += e.Amount
		} else {
			credit += e.Amount
		}
	}
	if debit != 250 || credit != 250 {
		t.Fatalf("debit/credit not conserved: debit=%d credit=%d", debit, credit)
	}
}

func TestConcurrent_DistinctRequests(t *testing.T) {
	svc, _ := testService(t)
	ctx := context.Background()

	// 200 笔不同请求，每笔 1 元；A 初始 1000，最多成功 1000 笔。
	const n = 200
	var wg sync.WaitGroup
	success, insufficient := int64(0), int64(0)
	var wgmu sync.Mutex
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, err := svc.Transfer(ctx, domain.TransferRequest{
				RequestID: fmt.Sprintf("D-%03d", i), FromID: "A", ToID: "B", Amount: 1,
			})
			wgmu.Lock()
			defer wgmu.Unlock()
			if err == nil {
				success++
			} else if le, ok := domain.AsLedgerError(err); ok && le.Kind == domain.KindInsufficient {
				insufficient++
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if success+insufficient != n || success != n {
		t.Fatalf("success=%d insufficient=%d, all %d must be deterministic (enough funds expected)",
			success, insufficient, n)
	}
	a, _ := svc.GetAccount(ctx, "A")
	b, _ := svc.GetAccount(ctx, "B")
	if a.Balance != 1000-success || b.Balance != 100+success {
		t.Fatalf("lost update: A=%d B=%d success=%d", a.Balance, b.Balance, success)
	}
}

func TestReversal_TraceabilityAndConservation(t *testing.T) {
	svc, st := testService(t)
	ctx := context.Background()

	orig, err := svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-10", FromID: "A", ToID: "B", Amount: 300})
	must(t, err)
	rv, err := svc.Reverse(ctx, domain.ReversalRequest{RequestID: "RV-10", OriginalRequest: "T-10"})
	must(t, err)

	if rv.OriginalID != orig.ID {
		t.Fatalf("reversal not linked: reversal=%s original=%s", rv.OriginalID, orig.ID)
	}
	if rv.Type != domain.VoucherReversal || !rv.Balanced() {
		t.Fatal("reversal voucher malformed")
	}
	// 冲正后净余额回到原点
	a, _ := svc.GetAccount(ctx, "A")
	b, _ := svc.GetAccount(ctx, "B")
	if a.Balance != 1000 || b.Balance != 100 {
		t.Fatalf("net effect after reversal not zero: A=%d B=%d", a.Balance, b.Balance)
	}
	// 冲正流水可追溯到原始凭证
	var linked int
	for _, e := range mustListEntries(t, st, ctx) {
		if e.Kind == domain.VoucherReversal && e.Related == orig.ID {
			linked++
		}
	}
	if linked != 2 {
		t.Fatalf("reversal entries not traceable to original voucher: linked=%d", linked)
	}

	// 重复冲正请求：同 requestID 幂等
	same, err := svc.Reverse(ctx, domain.ReversalRequest{RequestID: "RV-10", OriginalRequest: "T-10"})
	must(t, err)
	if same.ID != rv.ID {
		t.Fatal("reversal replay posted again")
	}
	// 不同 requestID 再次冲正同一原始凭证：拒绝
	_, err = svc.Reverse(ctx, domain.ReversalRequest{RequestID: "RV-11", OriginalRequest: "T-10"})
	assertKind(t, err, domain.KindAlreadyReversed)
}

func TestReversal_UnknownOriginal(t *testing.T) {
	svc, _ := testService(t)
	ctx := context.Background()
	_, err := svc.Reverse(ctx, domain.ReversalRequest{RequestID: "RV-x", OriginalRequest: "NOPE"})
	assertKind(t, err, domain.KindVoucherNotFound)
}

func mustListEntries(t *testing.T, st *store.MemoryStore, ctx context.Context) []store.LedgerEntry {
	t.Helper()
	es, err := st.ListEntries(ctx, 0, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	return es
}

func idOf(m map[string]int) string {
	for k := range m {
		return k
	}
	return ""
}

func TestIdempotent_FailedThenConflict(t *testing.T) {
	svc, _ := testService(t)
	ctx := context.Background()

	// 首次因余额不足失败
	_, err := svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-50", FromID: "A", ToID: "B", Amount: 9_999})
	assertKind(t, err, domain.KindInsufficient)

	// 相同 requestID 以不同内容重放：内容冲突优先于原失败结论
	_, err = svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-50", FromID: "A", ToID: "B", Amount: 1})
	assertKind(t, err, domain.KindIdempotentMiss)
}

func TestReversal_ContentConflict(t *testing.T) {
	svc, _ := testService(t)
	ctx := context.Background()
	_, err := svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-60", FromID: "A", ToID: "B", Amount: 10})
	must(t, err)
	rv, err := svc.Reverse(ctx, domain.ReversalRequest{RequestID: "RV-60", OriginalRequest: "T-60"})
	must(t, err)

	// 同一冲正 RequestID 却指向别的原始凭证：冲突
	_, err = svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-61", FromID: "A", ToID: "B", Amount: 10})
	must(t, err)
	_, err = svc.Reverse(ctx, domain.ReversalRequest{RequestID: "RV-60", OriginalRequest: "T-61"})
	assertKind(t, err, domain.KindIdempotentMiss)

	// 原始内容重放仍是同一张冲正凭证
	again, err := svc.Reverse(ctx, domain.ReversalRequest{RequestID: "RV-60", OriginalRequest: "T-60"})
	must(t, err)
	if again.ID != rv.ID {
		t.Fatal("reversal replay altered")
	}
}

func TestReversal_InsufficientAfterFundsMoved(t *testing.T) {
	svc, _ := testService(t)
	ctx := context.Background()
	// B 收到 100 后又把 150 转给 A（B 余额 50），此时冲正最早的收款会因资金不足被拒
	_, err := svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-70", FromID: "A", ToID: "B", Amount: 100})
	must(t, err)
	_, err = svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-71", FromID: "B", ToID: "A", Amount: 150})
	must(t, err)

	a, _ := svc.GetAccount(ctx, "A")
	b, _ := svc.GetAccount(ctx, "B")
	_, err = svc.Reverse(ctx, domain.ReversalRequest{RequestID: "RV-70", OriginalRequest: "T-70"})
	assertKind(t, err, domain.KindInsufficient)
	// 拒绝冲正不得改动余额
	a2, _ := svc.GetAccount(ctx, "A")
	b2, _ := svc.GetAccount(ctx, "B")
	if a2.Balance != a.Balance || b2.Balance != b.Balance {
		t.Fatalf("partial reversal posting: A %d->%d B %d->%d", a.Balance, a2.Balance, b.Balance, b2.Balance)
	}
}

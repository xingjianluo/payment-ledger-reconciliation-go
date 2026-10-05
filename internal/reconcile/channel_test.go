package reconcile_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/ledger"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/reconcile"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/store"
)

// ---- 渠道对账测试辅助 ----

var chOpening = map[string]int64{"A": 100000, "B": 100, "C": 0}

// setupChannel 建账并录入两笔转账（T-1: 300 A→B，T-2: 120 A→B）。
func setupChannel(t *testing.T) (*ledger.Service, *reconcile.Reconciler, int64, int64) {
	t.Helper()
	st := store.NewMemoryStore()
	svc := ledger.NewService(st)
	ctx := context.Background()
	for id, bal := range chOpening {
		if err := svc.CreateAccount(ctx, domain.Account{ID: id, Name: id, Balance: bal}); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range []domain.TransferRequest{
		{RequestID: "T-1", FromID: "A", ToID: "B", Amount: 300},
		{RequestID: "T-2", FromID: "A", ToID: "B", Amount: 120},
	} {
		if _, err := svc.Transfer(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	return svc, reconcile.NewReconciler(st),
		time.Now().Add(-time.Hour).UnixNano(), time.Now().Add(time.Hour).UnixNano()
}

// runChannel 在快照读内执行渠道对账。
func runChannel(t *testing.T, svc *ledger.Service, rec *reconcile.Reconciler, from, to int64, txns []reconcile.ChannelTxn) reconcile.ChannelReport {
	t.Helper()
	var rep reconcile.ChannelReport
	err := svc.View(context.Background(), func(ctx context.Context) error {
		var e error
		rep, e = rec.ReconcileWithChannel(ctx, from, to, txns, &reconcile.Options{OpeningBalances: chOpening})
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func assertChannelDiff(t *testing.T, rep reconcile.ChannelReport, kind reconcile.ChannelDiffKind, wantCount int) {
	t.Helper()
	if got := rep.CountByKind[kind]; got != wantCount {
		var details []string
		for _, d := range rep.ChannelDiffs {
			details = append(details, d.String())
		}
		t.Fatalf("channel diff kind=%s count=%d want=%d; all channel diffs: %v", kind, got, wantCount, details)
	}
}

// findChannelDiff 按类别与请求标识定位差异，用于校验判定依据字段。
func findChannelDiff(rep reconcile.ChannelReport, kind reconcile.ChannelDiffKind, requestID string) *reconcile.ChannelDiff {
	for i := range rep.ChannelDiffs {
		d := &rep.ChannelDiffs[i]
		if d.Kind == kind && d.RequestID == requestID {
			return d
		}
	}
	return nil
}

func okTxn(id, reqID string, amount int64, dir reconcile.ChannelDirection, at int64) reconcile.ChannelTxn {
	return reconcile.ChannelTxn{ChannelTxnID: id, RequestID: reqID, Amount: amount,
		Direction: dir, Status: reconcile.ChannelSuccess, OccurredAt: at}
}

// ---- 场景用例 ----

// 场景：渠道对账单与账本完全一致，双边配对成功，且账本自身核对结论保留在报告中。
func TestChannel_AllMatched(t *testing.T) {
	svc, rec, from, to := setupChannel(t)
	now := time.Now().UnixNano()
	txns := []reconcile.ChannelTxn{
		okTxn("CH-1", "T-1", 300, reconcile.ChannelDebit, now),
		okTxn("CH-2", "T-2", 120, reconcile.ChannelDebit, now),
	}
	rep := runChannel(t, svc, rec, from, to, txns)
	if !rep.Balanced {
		for _, d := range rep.ChannelDiffs {
			t.Errorf("unexpected channel diff: %s", d.String())
		}
	}
	if rep.Matched != 2 {
		t.Fatalf("matched=%d want=2", rep.Matched)
	}
	if !rep.Internal.Balanced {
		t.Fatal("internal ledger reconciliation should be balanced and preserved in report")
	}
	t.Logf("场景[双边完全一致]通过：matched=%d internal_balanced=%v", rep.Matched, rep.Internal.Balanced)
}

// 场景：账本有而渠道没有（渠道未达/漏记），差异须带账本侧判定依据。
func TestChannel_LedgerOnly(t *testing.T) {
	svc, rec, from, to := setupChannel(t)
	now := time.Now().UnixNano()
	txns := []reconcile.ChannelTxn{okTxn("CH-2", "T-2", 120, reconcile.ChannelDebit, now)}
	rep := runChannel(t, svc, rec, from, to, txns)
	assertChannelDiff(t, rep, reconcile.DiffLedgerOnly, 1)
	d := findChannelDiff(rep, reconcile.DiffLedgerOnly, "T-1")
	if d == nil {
		t.Fatal("LEDGER_ONLY diff for T-1 not found")
	}
	if d.LedgerVoucherID == "" || d.LedgerAmount != 300 || d.LedgerDirection != reconcile.ChannelDebit || d.LedgerAt == 0 {
		t.Fatalf("ledger-side evidence incomplete: %s", d.String())
	}
	t.Logf("场景[账本有渠道没有]通过：%s", d.String())
}

// 场景：渠道有而账本没有（渠道多记/未入账），差异须带渠道侧判定依据。
func TestChannel_ChannelOnly(t *testing.T) {
	svc, rec, from, to := setupChannel(t)
	now := time.Now().UnixNano()
	txns := []reconcile.ChannelTxn{
		okTxn("CH-1", "T-1", 300, reconcile.ChannelDebit, now),
		okTxn("CH-2", "T-2", 120, reconcile.ChannelDebit, now),
		okTxn("CH-9", "T-9", 500, reconcile.ChannelDebit, now), // 账本中不存在
	}
	rep := runChannel(t, svc, rec, from, to, txns)
	assertChannelDiff(t, rep, reconcile.DiffChannelOnly, 1)
	d := findChannelDiff(rep, reconcile.DiffChannelOnly, "T-9")
	if d == nil {
		t.Fatal("CHANNEL_ONLY diff for T-9 not found")
	}
	if len(d.ChannelTxnIDs) != 1 || d.ChannelTxnIDs[0] != "CH-9" || d.ChannelAmount != 500 || d.ChannelAt == 0 {
		t.Fatalf("channel-side evidence incomplete: %s", d.String())
	}
	t.Logf("场景[渠道有账本没有]通过：%s", d.String())
}

// 场景：两边都有但金额对不上、方向对不上，各自成类且双边依据齐全。
func TestChannel_ContentMismatch(t *testing.T) {
	svc, rec, from, to := setupChannel(t)
	now := time.Now().UnixNano()
	txns := []reconcile.ChannelTxn{
		okTxn("CH-1", "T-1", 301, reconcile.ChannelDebit, now),  // 金额不符
		okTxn("CH-2", "T-2", 120, reconcile.ChannelCredit, now), // 方向不符
	}
	rep := runChannel(t, svc, rec, from, to, txns)
	assertChannelDiff(t, rep, reconcile.DiffContentMismatch, 2)
	if rep.Matched != 0 {
		t.Fatalf("matched=%d want=0", rep.Matched)
	}
	d := findChannelDiff(rep, reconcile.DiffContentMismatch, "T-1")
	if d == nil || d.LedgerAmount != 300 || d.ChannelAmount != 301 {
		t.Fatalf("amount mismatch evidence wrong: %+v", d)
	}
	d2 := findChannelDiff(rep, reconcile.DiffContentMismatch, "T-2")
	if d2 == nil || d2.LedgerDirection != reconcile.ChannelDebit || d2.ChannelDirection != reconcile.ChannelCredit {
		t.Fatalf("direction mismatch evidence wrong: %+v", d2)
	}
	t.Logf("场景[金额/方向对不上]通过：%s ; %s", d.String(), d2.String())
}

// 场景：渠道自身重复（同一 RequestID 多条且内容不一致）单独成类，
// 该请求不再落入其他类别；内容完全相同的重复记录则折叠、不计差异。
func TestChannel_DuplicateAndFold(t *testing.T) {
	svc, rec, from, to := setupChannel(t)
	now := time.Now().UnixNano()
	txns := []reconcile.ChannelTxn{
		// T-1：渠道给了两条内容不一致的记录
		okTxn("CH-1a", "T-1", 300, reconcile.ChannelDebit, now),
		okTxn("CH-1b", "T-1", 301, reconcile.ChannelDebit, now),
		// T-2：完全相同的记录重复出现（重发/重复导入），应折叠为一条并正常配对
		okTxn("CH-2", "T-2", 120, reconcile.ChannelDebit, now),
		okTxn("CH-2", "T-2", 120, reconcile.ChannelDebit, now),
	}
	rep := runChannel(t, svc, rec, from, to, txns)
	assertChannelDiff(t, rep, reconcile.DiffChannelDuplicate, 1)
	assertChannelDiff(t, rep, reconcile.DiffLedgerOnly, 0)  // T-1 已在重复类交代，不再重复计数
	assertChannelDiff(t, rep, reconcile.DiffChannelOnly, 0) // 折叠后的 T-2 不算渠道单边
	if rep.Matched != 1 {
		t.Fatalf("matched=%d want=1 (only T-2)", rep.Matched)
	}
	d := findChannelDiff(rep, reconcile.DiffChannelDuplicate, "T-1")
	if d == nil || len(d.ChannelTxnIDs) != 2 {
		t.Fatalf("duplicate evidence wrong: %+v", d)
	}
	t.Logf("场景[渠道自身重复/相同记录折叠]通过：%s", d.String())
}

// 场景：窗口边界只算一次——from 含、to 不含，账本侧与渠道侧口径一致。
func TestChannel_WindowBoundary(t *testing.T) {
	svc, rec, _, _ := setupChannel(t)
	ctx := context.Background()
	v, ok, err := svc.GetVoucherByRequest(ctx, "T-1")
	if err != nil || !ok {
		t.Fatal("voucher T-1 not found")
	}
	ts := v.CreatedAt.UnixNano()

	// 渠道记录恰好落在边界上：at=from 计入，at=to 不计入。
	txns := []reconcile.ChannelTxn{
		okTxn("CH-from", "B-FROM", 1, reconcile.ChannelDebit, ts),
		okTxn("CH-to", "B-TO", 1, reconcile.ChannelDebit, ts+1000),
	}
	// 窗口 [ts, ts+1000)：T-1 凭证（at=ts）计入，B-TO（at=to）不计入。
	rep := runChannel(t, svc, rec, ts, ts+1000, txns)
	assertChannelDiff(t, rep, reconcile.DiffChannelOnly, 1) // 仅 B-FROM
	if findChannelDiff(rep, reconcile.DiffChannelOnly, "B-TO") != nil {
		t.Fatal("record at window end must be excluded")
	}
	assertChannelDiff(t, rep, reconcile.DiffLedgerOnly, 1) // 仅 T-1（T-2 在窗口外）
	if findChannelDiff(rep, reconcile.DiffLedgerOnly, "T-1") == nil {
		t.Fatal("voucher at window start must be included")
	}
	t.Logf("场景[窗口边界只算一次]通过：from含/to不含，账本侧与渠道侧一致")
}

// 场景：同一份对账单重复导入/重跑，差异不重复计数，两次结果完全一致。
func TestChannel_RerunIdempotent(t *testing.T) {
	svc, rec, from, to := setupChannel(t)
	now := time.Now().UnixNano()
	txns := []reconcile.ChannelTxn{
		okTxn("CH-1", "T-1", 300, reconcile.ChannelDebit, now),
		okTxn("CH-9", "T-9", 500, reconcile.ChannelDebit, now), // 渠道单边
	}
	rep1 := runChannel(t, svc, rec, from, to, txns)
	rep2 := runChannel(t, svc, rec, from, to, txns)
	if rep1.Matched != rep2.Matched || len(rep1.ChannelDiffs) != len(rep2.ChannelDiffs) {
		t.Fatalf("rerun not idempotent: run1 matched=%d diffs=%d, run2 matched=%d diffs=%d",
			rep1.Matched, len(rep1.ChannelDiffs), rep2.Matched, len(rep2.ChannelDiffs))
	}
	for k, v := range rep1.CountByKind {
		if rep2.CountByKind[k] != v {
			t.Fatalf("count mismatch on rerun for kind=%s: %d vs %d", k, v, rep2.CountByKind[k])
		}
	}
	assertChannelDiff(t, rep1, reconcile.DiffLedgerOnly, 1) // T-2
	assertChannelDiff(t, rep1, reconcile.DiffChannelOnly, 1)
	t.Logf("场景[重复导入/重跑幂等]通过：两次运行结论一致 diffs=%d", len(rep1.ChannelDiffs))
}

// 场景：渠道失败/处理中记录不构成差异，但如实计数披露，不静默丢弃。
func TestChannel_NonSuccessCounted(t *testing.T) {
	svc, rec, from, to := setupChannel(t)
	now := time.Now().UnixNano()
	txns := []reconcile.ChannelTxn{
		okTxn("CH-1", "T-1", 300, reconcile.ChannelDebit, now),
		okTxn("CH-2", "T-2", 120, reconcile.ChannelDebit, now),
		{ChannelTxnID: "CH-F", RequestID: "T-F", Amount: 10, Direction: reconcile.ChannelDebit, Status: reconcile.ChannelFailed, OccurredAt: now},
		{ChannelTxnID: "CH-P", RequestID: "T-P", Amount: 20, Direction: reconcile.ChannelDebit, Status: reconcile.ChannelPending, OccurredAt: now},
	}
	rep := runChannel(t, svc, rec, from, to, txns)
	if !rep.Balanced {
		t.Fatalf("non-success channel records must not produce diffs: %v", rep.ChannelDiffs)
	}
	if rep.SkippedChannelNonSuccess != 2 {
		t.Fatalf("skipped=%d want=2", rep.SkippedChannelNonSuccess)
	}
	t.Logf("场景[非成功状态计数披露]通过：skipped=%d", rep.SkippedChannelNonSuccess)
}

// 场景：冲正凭证按 CREDIT 方向与渠道退款记录配对。
func TestChannel_ReversalMatchesCredit(t *testing.T) {
	svc, rec, from, to := setupChannel(t)
	ctx := context.Background()
	if _, err := svc.Reverse(ctx, domain.ReversalRequest{RequestID: "RV-1", OriginalRequest: "T-1"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	txns := []reconcile.ChannelTxn{
		okTxn("CH-1", "T-1", 300, reconcile.ChannelDebit, now),
		okTxn("CH-2", "T-2", 120, reconcile.ChannelDebit, now),
		okTxn("CH-R1", "RV-1", 300, reconcile.ChannelCredit, now), // 渠道侧退款/回流
	}
	rep := runChannel(t, svc, rec, from, to, txns)
	if !rep.Balanced || rep.Matched != 3 {
		for _, d := range rep.ChannelDiffs {
			t.Errorf("unexpected: %s", d.String())
		}
		t.Fatalf("matched=%d want=3", rep.Matched)
	}
	t.Logf("场景[冲正按CREDIT配对]通过：matched=%d", rep.Matched)
}

// 场景：对账期间并发入账，报告基于同一快照时点，账本自身核对始终一致。
func TestChannel_SnapshotUnderConcurrency(t *testing.T) {
	st := store.NewMemoryStore()
	svc := ledger.NewService(st)
	rec := reconcile.NewReconciler(st)
	ctx := context.Background()
	opening := map[string]int64{"A": 1 << 40, "B": 0}
	for id, bal := range opening {
		if err := svc.CreateAccount(ctx, domain.Account{ID: id, Name: id, Balance: bal}); err != nil {
			t.Fatal(err)
		}
	}
	from := time.Now().Add(-time.Hour).UnixNano()
	to := time.Now().Add(time.Hour).UnixNano()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 8; w++ { // 并发写入
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = svc.Transfer(ctx, domain.TransferRequest{
					RequestID: fmt.Sprintf("CW-%d-%d", w, i), FromID: "A", ToID: "B", Amount: 1})
			}
		}(w)
	}
	for i := 0; i < 50; i++ { // 并发对账：每次都必须看到一致快照
		var rep reconcile.ChannelReport
		err := svc.View(ctx, func(ctx context.Context) error {
			var e error
			rep, e = rec.ReconcileWithChannel(ctx, from, to, nil, &reconcile.Options{OpeningBalances: opening})
			return e
		})
		if err != nil {
			t.Fatal(err)
		}
		if !rep.Internal.Balanced {
			t.Fatalf("iteration %d: internal reconciliation saw inconsistent snapshot", i)
		}
	}
	close(stop)
	wg.Wait()
	t.Logf("场景[并发写入下快照一致]通过：50 次对账内部核对全部一致")
}

// 场景：几千条渠道明细的大对账单正常跑完，配对计数准确。
func TestChannel_LargeStatement(t *testing.T) {
	st := store.NewMemoryStore()
	svc := ledger.NewService(st)
	rec := reconcile.NewReconciler(st)
	ctx := context.Background()
	opening := map[string]int64{"A": 1 << 40, "B": 0}
	for id, bal := range opening {
		if err := svc.CreateAccount(ctx, domain.Account{ID: id, Name: id, Balance: bal}); err != nil {
			t.Fatal(err)
		}
	}
	const n = 5000
	now := time.Now().UnixNano()
	txns := make([]reconcile.ChannelTxn, 0, n+2)
	for i := 0; i < n; i++ {
		reqID := fmt.Sprintf("L-%06d", i)
		if _, err := svc.Transfer(ctx, domain.TransferRequest{RequestID: reqID, FromID: "A", ToID: "B", Amount: 10}); err != nil {
			t.Fatal(err)
		}
		txns = append(txns, okTxn(fmt.Sprintf("CH-%06d", i), reqID, 10, reconcile.ChannelDebit, now))
	}
	// 混入少量差异：1 条渠道单边 + 1 条金额不符。
	txns = append(txns, okTxn("CH-X", "L-X", 10, reconcile.ChannelDebit, now))
	txns[n-1].Amount = 11

	from := time.Now().Add(-time.Hour).UnixNano()
	to := time.Now().Add(time.Hour).UnixNano()
	start := time.Now()
	var rep reconcile.ChannelReport
	err := svc.View(ctx, func(ctx context.Context) error {
		var e error
		rep, e = rec.ReconcileWithChannel(ctx, from, to, txns, &reconcile.Options{OpeningBalances: opening})
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if rep.Matched != n-1 {
		t.Fatalf("matched=%d want=%d", rep.Matched, n-1)
	}
	assertChannelDiff(t, rep, reconcile.DiffChannelOnly, 1)
	assertChannelDiff(t, rep, reconcile.DiffContentMismatch, 1)
	t.Logf("场景[大对账单]通过：%d 条渠道明细 matched=%d 耗时=%s", len(txns), rep.Matched, elapsed)
}

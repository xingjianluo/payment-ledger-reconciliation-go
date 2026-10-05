package channelrecon_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/channelrecon"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/ledger"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/reconcile"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/store"
)

var base = time.Now().Add(-time.Hour).UnixNano()

func setup(t *testing.T) (*ledger.Service, *store.MemoryStore, *channelrecon.Reconciler, int64, int64) {
	t.Helper()
	st := store.NewMemoryStore()
	svc := ledger.NewService(st)
	ctx := context.Background()
	for id, bal := range map[string]int64{"A": 1_000_000, "B": 1000} {
		if err := svc.CreateAccount(ctx, domain.Account{ID: id, Name: id, Balance: bal}); err != nil {
			t.Fatal(err)
		}
	}
	return svc, st, channelrecon.NewReconciler(st), base, base + int64(2*time.Hour)
}

func transfer(t *testing.T, svc *ledger.Service, reqID string, amount int64) domain.Voucher {
	t.Helper()
	v, err := svc.Transfer(context.Background(),
		domain.TransferRequest{RequestID: reqID, FromID: "A", ToID: "B", Amount: amount})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// txn 构造窗口内一条 SUCCESS 渠道明细。
func txn(id, reqID, dir string, amount int64) channelrecon.ChannelTxn {
	return channelrecon.ChannelTxn{
		ChannelTxnID: id, RequestID: reqID, Direction: dir,
		Amount: amount, OccurredAt: base + int64(time.Minute), Status: channelrecon.StatusSuccess,
	}
}

func run(t *testing.T, svc *ledger.Service, rec *channelrecon.Reconciler,
	stmt *channelrecon.Statement, from, to int64) channelrecon.Report {
	t.Helper()
	opening := map[string]int64{"A": 1_000_000, "B": 1000}
	var rep channelrecon.Report
	err := svc.View(context.Background(), func(ctx context.Context) error {
		var e error
		rep, e = rec.Reconcile(ctx, stmt, from, to, &reconcile.Options{OpeningBalances: opening})
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func assertKind(t *testing.T, rep channelrecon.Report, kind channelrecon.ChannelDiffKind, want int) {
	t.Helper()
	if got := rep.CountByKind[kind]; got != want {
		var details []string
		for _, d := range rep.Diffs {
			details = append(details, d.String())
		}
		t.Fatalf("kind=%s count=%d want=%d; diffs:\n%s", kind, got, want, strings.Join(details, "\n"))
	}
}

func TestChannelReconcile(t *testing.T) {
	t.Run("全部配对成功且保留账本自身核对结论", func(t *testing.T) {
		svc, _, rec, from, to := setup(t)
		transfer(t, svc, "T-1", 300)
		transfer(t, svc, "T-2", 120)
		stmt := channelrecon.NewStatement()
		stmt.Import([]channelrecon.ChannelTxn{txn("C-1", "T-1", "DEBIT", 300), txn("C-2", "T-2", "DEBIT", 120)})

		rep := run(t, svc, rec, stmt, from, to)
		if !rep.Balanced || !rep.Internal.Balanced {
			for _, d := range rep.Diffs {
				t.Errorf("unexpected diff: %s", d.String())
			}
			for _, d := range rep.Internal.Diffs {
				t.Errorf("unexpected internal diff: %s", d.String())
			}
		}
		if rep.Matched != 2 || rep.LedgerTxns != 2 || rep.ChannelTxns != 2 {
			t.Fatalf("matched=%d ledger=%d channel=%d, want 2/2/2", rep.Matched, rep.LedgerTxns, rep.ChannelTxns)
		}
		t.Logf("matched=%d internal_balanced=%v", rep.Matched, rep.Internal.Balanced)
	})

	t.Run("账本有而渠道没有-LEDGER_ONLY", func(t *testing.T) {
		svc, _, rec, from, to := setup(t)
		v := transfer(t, svc, "T-1", 300)
		rep := run(t, svc, rec, channelrecon.NewStatement(), from, to)
		assertKind(t, rep, channelrecon.DiffLedgerOnly, 1)
		d := rep.Diffs[0]
		if d.RequestID != "T-1" || d.VoucherID != v.ID || d.LedgerAmount != 300 || d.LedgerTime == 0 {
			t.Fatalf("ledger-only evidence incomplete: %s", d.String())
		}
		t.Logf("diff: %s", d.String())
	})

	t.Run("渠道有而账本没有-CHANNEL_ONLY", func(t *testing.T) {
		svc, _, rec, from, to := setup(t)
		stmt := channelrecon.NewStatement()
		stmt.Import([]channelrecon.ChannelTxn{txn("C-9", "T-GHOST", "DEBIT", 500)})
		rep := run(t, svc, rec, stmt, from, to)
		assertKind(t, rep, channelrecon.DiffChannelOnly, 1)
		d := rep.Diffs[0]
		if d.RequestID != "T-GHOST" || d.ChannelTxnID != "C-9" || d.ChannelAmount != 500 {
			t.Fatalf("channel-only evidence incomplete: %s", d.String())
		}
		t.Logf("diff: %s", d.String())
	})

	t.Run("渠道有而账本凭证在窗口外-证据指出窗口外时间", func(t *testing.T) {
		svc, _, rec, _, _ := setup(t)
		transfer(t, svc, "T-1", 300) // 凭证时间在 now，落在下面窄窗口之外
		narrowFrom := base
		narrowTo := base + int64(30*time.Minute) // 窗口早于任何凭证
		stmt := channelrecon.NewStatement()
		stmt.Import([]channelrecon.ChannelTxn{txn("C-1", "T-1", "DEBIT", 300)})
		rep := run(t, svc, rec, stmt, narrowFrom, narrowTo)
		assertKind(t, rep, channelrecon.DiffChannelOnly, 1)
		if !strings.Contains(rep.Diffs[0].Evidence, "outside window") {
			t.Fatalf("want outside-window evidence, got: %s", rep.Diffs[0].Evidence)
		}
		t.Logf("diff: %s", rep.Diffs[0].String())
	})

	t.Run("两边都有但金额对不上-FIELD_MISMATCH", func(t *testing.T) {
		svc, _, rec, from, to := setup(t)
		transfer(t, svc, "T-1", 300)
		stmt := channelrecon.NewStatement()
		stmt.Import([]channelrecon.ChannelTxn{txn("C-1", "T-1", "DEBIT", 301)})
		rep := run(t, svc, rec, stmt, from, to)
		assertKind(t, rep, channelrecon.DiffFieldMismatch, 1)
		d := rep.Diffs[0]
		if d.LedgerAmount != 300 || d.ChannelAmount != 301 || d.LedgerTime == 0 || d.ChannelTime == 0 {
			t.Fatalf("mismatch evidence incomplete: %s", d.String())
		}
		t.Logf("diff: %s", d.String())
	})

	t.Run("两边都有但方向对不上-FIELD_MISMATCH", func(t *testing.T) {
		svc, _, rec, from, to := setup(t)
		transfer(t, svc, "T-1", 300)
		if _, err := svc.Reverse(context.Background(),
			domain.ReversalRequest{RequestID: "RV-1", OriginalRequest: "T-1"}); err != nil {
			t.Fatal(err)
		}
		stmt := channelrecon.NewStatement()
		stmt.Import([]channelrecon.ChannelTxn{
			txn("C-1", "T-1", "DEBIT", 300),
			txn("C-2", "RV-1", "DEBIT", 300), // 冲正应为 CREDIT，方向不符
		})
		rep := run(t, svc, rec, stmt, from, to)
		assertKind(t, rep, channelrecon.DiffFieldMismatch, 1)
		assertKind(t, rep, channelrecon.DiffLedgerOnly, 0)
		if rep.Matched != 1 {
			t.Fatalf("matched=%d want 1", rep.Matched)
		}
		t.Logf("diff: %s", rep.Diffs[0].String())
	})

	t.Run("渠道自身重复且内容不一致-CHANNEL_DUPLICATE", func(t *testing.T) {
		svc, _, rec, from, to := setup(t)
		transfer(t, svc, "T-1", 300)
		stmt := channelrecon.NewStatement()
		stmt.Import([]channelrecon.ChannelTxn{
			txn("C-1", "T-1", "DEBIT", 300),
			txn("C-2", "T-1", "DEBIT", 301), // 同一请求标识，金额不一致
		})
		rep := run(t, svc, rec, stmt, from, to)
		assertKind(t, rep, channelrecon.DiffChannelDup, 1)
		// 重复组不再参与配对，不得再报 LEDGER_ONLY / MISMATCH
		assertKind(t, rep, channelrecon.DiffLedgerOnly, 0)
		assertKind(t, rep, channelrecon.DiffFieldMismatch, 0)
		d := rep.Diffs[0]
		if d.RequestID != "T-1" || !strings.Contains(d.Evidence, "content_consistent=false") {
			t.Fatalf("dup evidence incomplete: %s", d.String())
		}
		t.Logf("diff: %s", d.String())
	})

	t.Run("渠道自身重复但内容一致-仍成类不静默", func(t *testing.T) {
		svc, _, rec, from, to := setup(t)
		transfer(t, svc, "T-1", 300)
		stmt := channelrecon.NewStatement()
		stmt.Import([]channelrecon.ChannelTxn{
			txn("C-1", "T-1", "DEBIT", 300),
			txn("C-2", "T-1", "DEBIT", 300), // 不同渠道流水号、内容一致
		})
		rep := run(t, svc, rec, stmt, from, to)
		assertKind(t, rep, channelrecon.DiffChannelDup, 1)
		if !strings.Contains(rep.Diffs[0].Evidence, "content_consistent=true") {
			t.Fatalf("dup evidence incomplete: %s", rep.Diffs[0].String())
		}
		t.Logf("diff: %s", rep.Diffs[0].String())
	})

	t.Run("非成功状态渠道明细-计数报告不参与配对", func(t *testing.T) {
		svc, _, rec, from, to := setup(t)
		transfer(t, svc, "T-1", 300)
		pending := txn("C-2", "T-PENDING", "DEBIT", 999)
		pending.Status = "PENDING"
		stmt := channelrecon.NewStatement()
		stmt.Import([]channelrecon.ChannelTxn{txn("C-1", "T-1", "DEBIT", 300), pending})
		rep := run(t, svc, rec, stmt, from, to)
		if rep.SkippedChannel != 1 {
			t.Fatalf("skipped=%d want 1", rep.SkippedChannel)
		}
		// PENDING 明细不产生 CHANNEL_ONLY，也不让账本侧凭空配对
		assertKind(t, rep, channelrecon.DiffChannelOnly, 0)
		if !rep.Balanced {
			t.Fatalf("want balanced, diffs: %v", rep.Diffs)
		}
		t.Logf("skipped=%d matched=%d", rep.SkippedChannel, rep.Matched)
	})

	t.Run("时间窗口边界只算一次", func(t *testing.T) {
		svc, _, rec, _, _ := setup(t)
		transfer(t, svc, "T-1", 300) // 凭证时间约为 base+1h，落在 [base, base+2h) 内
		atFrom := txn("C-1", "T-1", "DEBIT", 300)
		atFrom.OccurredAt = base // 恰好等于 from：计入本窗口
		atTo := txn("C-2", "T-1", "DEBIT", 300)
		atTo.OccurredAt = base + int64(2*time.Hour) // 恰好等于 to：不计入本窗口
		stmt := channelrecon.NewStatement()
		stmt.Import([]channelrecon.ChannelTxn{atFrom})
		rep := run(t, svc, rec, stmt, base, base+int64(2*time.Hour))
		if rep.ChannelTxns != 1 || rep.Matched != 1 {
			t.Fatalf("from-boundary txn must be counted once: channel=%d matched=%d", rep.ChannelTxns, rep.Matched)
		}
		stmt2 := channelrecon.NewStatement()
		stmt2.Import([]channelrecon.ChannelTxn{atTo})
		rep2 := run(t, svc, rec, stmt2, base, base+int64(2*time.Hour))
		if rep2.ChannelTxns != 0 {
			t.Fatalf("to-boundary txn must be excluded from [from,to): channel=%d", rep2.ChannelTxns)
		}
		// 同一笔落入下一个窗口且只落入下一个窗口
		rep3 := run(t, svc, rec, stmt2, base+int64(2*time.Hour), base+int64(4*time.Hour))
		if rep3.ChannelTxns != 1 {
			t.Fatalf("to-boundary txn must appear in next window: channel=%d", rep3.ChannelTxns)
		}
		t.Logf("boundary ok: prev=%d next=%d", rep2.ChannelTxns, rep3.ChannelTxns)
	})

	t.Run("同一份对账单重复导入与重跑不重复计数", func(t *testing.T) {
		svc, _, rec, from, to := setup(t)
		transfer(t, svc, "T-1", 300)
		batch := []channelrecon.ChannelTxn{txn("C-1", "T-1", "DEBIT", 300), txn("C-9", "T-GHOST", "DEBIT", 500)}
		stmt := channelrecon.NewStatement()
		if n := stmt.Import(batch); n != 2 {
			t.Fatalf("first import added=%d want 2", n)
		}
		if n := stmt.Import(batch); n != 0 {
			t.Fatalf("re-import added=%d want 0 (dedup)", n)
		}
		if stmt.Len() != 2 {
			t.Fatalf("statement len=%d want 2", stmt.Len())
		}
		rep1 := run(t, svc, rec, stmt, from, to)
		rep2 := run(t, svc, rec, stmt, from, to) // 重跑
		assertKind(t, rep1, channelrecon.DiffChannelOnly, 1)
		if len(rep1.Diffs) != len(rep2.Diffs) || rep1.Matched != rep2.Matched {
			t.Fatalf("rerun not deterministic: %d vs %d diffs", len(rep1.Diffs), len(rep2.Diffs))
		}
		t.Logf("reimport dedup ok, rerun diffs=%d", len(rep2.Diffs))
	})

	t.Run("核对期间并发入账-报告基于一致时点", func(t *testing.T) {
		svc, _, rec, from, to := setup(t)
		transfer(t, svc, "T-1", 300)
		stmt := channelrecon.NewStatement()
		stmt.Import([]channelrecon.ChannelTxn{txn("C-1", "T-1", "DEBIT", 300)})

		stop := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				n := 0
				for {
					select {
					case <-stop:
						return
					default:
					}
					n++
					_, _ = svc.Transfer(context.Background(), domain.TransferRequest{
						RequestID: fmt.Sprintf("BG-%d-%d", worker, n), FromID: "A", ToID: "B", Amount: 1})
				}
			}(i)
		}
		// 并发写入期间反复核对：每次报告都必须内部自洽（快照一致）。
		for i := 0; i < 50; i++ {
			rep := run(t, svc, rec, stmt, from, to)
			if !rep.Internal.Balanced {
				t.Fatalf("internal reconcile saw inconsistent snapshot: %v", rep.Internal.Diffs)
			}
			if rep.Matched != 1 {
				t.Fatalf("matched=%d want 1 (background txns are ledger-only)", rep.Matched)
			}
		}
		close(stop)
		wg.Wait()
		t.Log("snapshot consistency held under concurrent posting")
	})

	t.Run("几千条渠道明细可正常跑完", func(t *testing.T) {
		svc, _, rec, from, to := setup(t)
		const n = 5000
		stmt := channelrecon.NewStatement()
		batch := make([]channelrecon.ChannelTxn, 0, n)
		for i := 0; i < n; i++ {
			reqID := fmt.Sprintf("T-%05d", i)
			transfer(t, svc, reqID, 10)
			batch = append(batch, txn(fmt.Sprintf("C-%05d", i), reqID, "DEBIT", 10))
		}
		// 掺入少量差异：1 条渠道多记、1 条金额不符
		batch = append(batch, txn("C-X", "T-GHOST", "DEBIT", 1))
		batch[100].Amount = 11
		stmt.Import(batch)

		start := time.Now()
		rep := run(t, svc, rec, stmt, from, to)
		elapsed := time.Since(start)
		assertKind(t, rep, channelrecon.DiffChannelOnly, 1)
		assertKind(t, rep, channelrecon.DiffFieldMismatch, 1)
		if rep.Matched != n-1 {
			t.Fatalf("matched=%d want %d", rep.Matched, n-1)
		}
		if elapsed > 10*time.Second {
			t.Fatalf("reconcile too slow: %v", elapsed)
		}
		t.Logf("n=%d matched=%d elapsed=%v", n, rep.Matched, elapsed)
	})
}

func TestParseCSV(t *testing.T) {
	t.Run("标准列与两种时间格式", func(t *testing.T) {
		csvText := "channel_txn_id,request_id,direction,amount,occurred_at,status\n" +
			"C-1,T-1,DEBIT,300,1700000000000000000,SUCCESS\n" +
			"C-2,T-2,credit,120,2023-11-14T22:13:20Z,success\n"
		txns, err := channelrecon.ParseCSV(strings.NewReader(csvText))
		if err != nil {
			t.Fatal(err)
		}
		if len(txns) != 2 {
			t.Fatalf("parsed %d txns want 2", len(txns))
		}
		if txns[0].OccurredAt != 1700000000000000000 || txns[0].Direction != "DEBIT" {
			t.Fatalf("bad row 1: %+v", txns[0])
		}
		want := time.Date(2023, 11, 14, 22, 13, 20, 0, time.UTC).UnixNano()
		if txns[1].OccurredAt != want || txns[1].Direction != "CREDIT" || txns[1].Status != "SUCCESS" {
			t.Fatalf("bad row 2: %+v", txns[1])
		}
		t.Logf("parsed %d txns", len(txns))
	})

	t.Run("缺列与坏行要报错", func(t *testing.T) {
		if _, err := channelrecon.ParseCSV(strings.NewReader("a,b\n1,2\n")); err == nil {
			t.Fatal("want missing-column error")
		}
		bad := "channel_txn_id,request_id,direction,amount,occurred_at,status\n" +
			"C-1,T-1,DEBIT,not-a-number,1700000000000000000,SUCCESS\n"
		if _, err := channelrecon.ParseCSV(strings.NewReader(bad)); err == nil {
			t.Fatal("want bad-amount error")
		}
		badDir := "channel_txn_id,request_id,direction,amount,occurred_at,status\n" +
			"C-1,T-1,SIDEWAYS,300,1700000000000000000,SUCCESS\n"
		if _, err := channelrecon.ParseCSV(strings.NewReader(badDir)); err == nil {
			t.Fatal("want bad-direction error")
		}
		t.Log("parse errors reported as expected")
	})

	t.Run("CSV导入后参与端到端核对", func(t *testing.T) {
		svc, _, rec, from, to := setup(t)
		transfer(t, svc, "T-1", 300)
		csvText := "channel_txn_id,request_id,direction,amount,occurred_at,status\n" +
			fmt.Sprintf("C-1,T-1,DEBIT,300,%d,SUCCESS\n", base+int64(time.Minute))
		txns, err := channelrecon.ParseCSV(strings.NewReader(csvText))
		if err != nil {
			t.Fatal(err)
		}
		stmt := channelrecon.NewStatement()
		stmt.Import(txns)
		rep := run(t, svc, rec, stmt, from, to)
		if !rep.Balanced || rep.Matched != 1 {
			t.Fatalf("want balanced matched=1, got balanced=%v matched=%d diffs=%v",
				rep.Balanced, rep.Matched, rep.Diffs)
		}
		t.Log("csv end-to-end reconcile balanced")
	})
}

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/channelrecon"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/ledger"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/reconcile"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/store"
)

func main() {
	ctx := context.Background()
	st := store.NewMemoryStore()
	svc := ledger.NewService(st)
	rec := reconcile.NewReconciler(st)

	must(openAccount(svc, "A", "cash-a", 1000))
	must(openAccount(svc, "B", "cash-b", 100))
	must(openAccount(svc, "C", "frozen-c", 0))

	// 1) 正常转账 + 重复提交（幂等）
	v, err := svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-1", FromID: "A", ToID: "B", Amount: 300, Narration: "pay-1"})
	must(err)
	fmt.Printf("transfer ok: voucher=%s amount=%d\n", v.ID, v.Amount)

	v2, err := svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-1", FromID: "A", ToID: "B", Amount: 300})
	must(err)
	fmt.Printf("duplicate replay: same voucher=%v (no double posting)\n", v2.ID == v.ID)

	// 2) 内容冲突的重复请求
	_, err = svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-1", FromID: "A", ToID: "B", Amount: 999})
	if le, ok := domain.AsLedgerError(err); ok {
		fmt.Printf("conflicting replay rejected: kind=%s evidence=%s\n", le.Kind, le.Evidence)
	} else {
		panic("expected IDEMPOTENT_CONTENT_MISMATCH")
	}

	// 3) 余额不足（不得部分入账）
	before := balance(svc, "A")
	_, err = svc.Transfer(ctx, domain.TransferRequest{RequestID: "T-2", FromID: "A", ToID: "B", Amount: 1_000_000})
	if le, ok := domain.AsLedgerError(err); ok {
		fmt.Printf("insufficient rejected: kind=%s balance unchanged=%v\n", le.Kind, balance(svc, "A") == before)
	}

	// 4) 冲正
	rv, err := svc.Reverse(ctx, domain.ReversalRequest{RequestID: "RV-1", OriginalRequest: "T-1", Narration: "reverse T-1"})
	must(err)
	fmt.Printf("reversal ok: voucher=%s original=%s\n", rv.ID, rv.OriginalID)
	_, err = svc.Reverse(ctx, domain.ReversalRequest{RequestID: "RV-2", OriginalRequest: "T-1"})
	if le, ok := domain.AsLedgerError(err); ok {
		fmt.Printf("double reversal rejected: kind=%s\n", le.Kind)
	}

	// 5) 对账：传入窗口期初余额（生产中来自上一日切余额快照）。
	from := time.Now().Add(-time.Hour).UnixNano()
	to := time.Now().Add(time.Hour).UnixNano()
	opening := map[string]int64{"A": 1000, "B": 100, "C": 0}
	var report reconcile.Report
	must(svc.View(ctx, func(ctx context.Context) error {
		var err error
		report, err = rec.Reconcile(ctx, from, to, &reconcile.Options{OpeningBalances: opening})
		return err
	}))
	fmt.Printf("reconcile balanced=%v diffs=%d\n", report.Balanced, len(report.Diffs))
	fmt.Printf("balances: A=%d B=%d\n", balance(svc, "A"), balance(svc, "B"))

	// 6) 渠道对账：本地 CSV 构造渠道对账单，与账本双边核对（只读）。
	csvText := "channel_txn_id,request_id,direction,amount,occurred_at,status\n" +
		fmt.Sprintf("CH-1,T-1,DEBIT,300,%d,SUCCESS\n", time.Now().UnixNano()) +   // 与账本一致
		fmt.Sprintf("CH-2,RV-1,CREDIT,300,%d,SUCCESS\n", time.Now().UnixNano()) + // 冲正：渠道视角为入金
		fmt.Sprintf("CH-3,T-GHOST,DEBIT,88,%d,SUCCESS\n", time.Now().UnixNano())  // 渠道有、账本没有
	txns, err := channelrecon.ParseCSV(strings.NewReader(csvText))
	must(err)
	stmt := channelrecon.NewStatement()
	fmt.Printf("statement imported=%d (re-import adds %d)\n", stmt.Import(txns), stmt.Import(txns))

	crec := channelrecon.NewReconciler(st)
	var crep channelrecon.Report
	must(svc.View(ctx, func(ctx context.Context) error {
		var err error
		crep, err = crec.Reconcile(ctx, stmt, from, to, &reconcile.Options{OpeningBalances: opening})
		return err
	}))
	fmt.Printf("channel reconcile balanced=%v matched=%d ledger_txns=%d channel_txns=%d\n",
		crep.Balanced, crep.Matched, crep.LedgerTxns, crep.ChannelTxns)
	fmt.Printf("internal reconcile preserved: balanced=%v diffs=%d\n",
		crep.Internal.Balanced, len(crep.Internal.Diffs))
	for _, d := range crep.Diffs {
		fmt.Println("  " + d.String())
	}
}

func openAccount(svc *ledger.Service, id, name string, bal int64) error {
	return svc.CreateAccount(context.Background(), domain.Account{ID: id, Name: name, Balance: bal})
}

func balance(svc *ledger.Service, id string) int64 {
	a, err := svc.GetAccount(context.Background(), id)
	if err != nil {
		panic(err)
	}
	return a.Balance
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

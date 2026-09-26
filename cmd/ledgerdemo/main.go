package main

import (
	"context"
	"fmt"
	"time"

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

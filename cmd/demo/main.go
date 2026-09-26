// Command demo 在本地演示幂等转账、并发入账、冲正与对账差异分类。
// 运行：go run ./cmd/demo
// 不依赖网络、真实凭据或任何外部服务，全部数据在进程内存中完成。
package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/ledger"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/reconcile"
)

const yuan = 100.0

func money(c int64) string { return fmt.Sprintf("%.2f元", float64(c)/yuan) }

func printBalances(l *ledger.Ledger, ids ...string) {
	for _, id := range ids {
		b, _ := l.Balance(id)
		fmt.Printf("    账户 %s 余额 = %s\n", id, money(b))
	}
}

func printReport(title string, rep *domain.ReconcileReport) {
	fmt.Printf("  %s：窗口 %s ~ %s\n", title,
		rep.WindowStart.Format("15:04:05"), rep.WindowEnd.Format("15:04:05"))
	if rep.Balanced() {
		fmt.Println("    结论：账账相符，无差异 ✅")
		return
	}
	fmt.Printf("    结论：发现 %d 项差异（只报告、不自动修复）：\n", len(rep.Issues))
	for _, is := range rep.Issues {
		loc := is.RequestID
		if loc == "" {
			loc = is.AccountID
		}
		fmt.Printf("    - [%s] 标识=%s 依据: %s\n", is.Type, loc, is.Detail)
	}
}

func mustTransfer(l *ledger.Ledger, req ledger.TransferRequest, at time.Time) *domain.Voucher {
	v, err := l.Transfer(req, at)
	if err != nil {
		panic(err)
	}
	return v
}

func main() {
	base := time.Date(2026, 9, 26, 9, 0, 0, 0, time.UTC)
	sec := func(n int) time.Time { return base.Add(time.Duration(n) * time.Second) }

	fmt.Println("================ 场景 1：幂等转账（重复提交不重复记账） ================")
	l := ledger.New(base)
	must(l.CreateAccount("A", "商户A", domain.AccountActive, base))
	must(l.CreateAccount("B", "商户B", domain.AccountActive, base))
	must(l.CreateAccount("C", "商户C", domain.AccountActive, base))
	if _, err := l.Opening(ledger.OpeningRequest{
		RequestID: "open-A", AccountID: "A", Amount: 10_000_000, Summary: "期初注资",
	}, sec(0)); err != nil {
		panic(err)
	}

	req := ledger.TransferRequest{RequestID: "PAY-20260926-0001", FromID: "A", ToID: "B", Amount: 30000}
	v1 := mustTransfer(l, req, sec(1))
	v2 := mustTransfer(l, req, sec(2)) // 完全相同的重复提交
	fmt.Printf("  首次凭证=%s，重复提交返回凭证=%s，两者相同=%v\n", v1.ID, v2.ID, v1.ID == v2.ID)
	printBalances(l, "A", "B")

	// 相同 RequestID、不同内容：必须拒绝并给出可区分原因。
	_, err := l.Transfer(ledger.TransferRequest{
		RequestID: "PAY-20260926-0001", FromID: "A", ToID: "C", Amount: 5,
	}, sec(3))
	fmt.Printf("  内容冲突的重复请求 -> %v\n", err)
	fmt.Printf("    （errors.Is 幂等冲突 = %v）\n", errors.Is(err, ledger.ErrIdempotencyConflict))

	fmt.Println("\n================ 场景 2：并发重复提交 + 余额争抢 ================")
	const (
		distinctTxs = 20
		duplicates  = 10
	)
	var wg sync.WaitGroup
	var okCount, failCount int64
	var counterMu sync.Mutex
	for i := 0; i < distinctTxs; i++ {
		r := ledger.TransferRequest{
			RequestID: fmt.Sprintf("CONC-%03d", i), FromID: "A", ToID: "B", Amount: 5000,
		}
		for j := 0; j < duplicates; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, e := l.Transfer(r, sec(4)); e != nil {
					counterMu.Lock()
					failCount++
					counterMu.Unlock()
				} else {
					counterMu.Lock()
					okCount++
					counterMu.Unlock()
				}
			}()
		}
	}
	wg.Wait()
	fmt.Printf("  %d 笔业务 × 每笔 %d 次并发重复提交：成功返回 %d 次（应=%d），失败 %d 次（应=0）\n",
		distinctTxs, duplicates, okCount, distinctTxs*duplicates, failCount)
	fmt.Printf("  B 账户入账分录数 = %d（应=%d：场景1的1条 + 本场景 %d 条，无重复记账、无丢失更新）\n",
		len(l.Entries("B")), 1+distinctTxs, distinctTxs)
	printBalances(l, "A", "B")

	// 余额争抢：30 笔各 100 元，而 C 只被注入 1000 元，恰好 10 笔成功、20 笔失败。
	_, e := l.Opening(ledger.OpeningRequest{RequestID: "open-C", AccountID: "C", Amount: 100000}, sec(5))
	must(e)
	const racers = 30
	var raceOK, raceFail int64
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := l.Transfer(ledger.TransferRequest{
				RequestID: fmt.Sprintf("RACE-%02d", i), FromID: "C", ToID: "B", Amount: 10000,
			}, sec(6))
			counterMu.Lock()
			defer counterMu.Unlock()
			if errors.Is(err, ledger.ErrInsufficientFunds) {
				raceFail++
			} else if err == nil {
				raceOK++
			} else {
				panic(err)
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("  余额争抢：%d 笔各 100 元、C 仅有 1000 元 -> 成功 %d 笔（应=10），余额不足拒绝 %d 笔（应=20）\n",
		racers, raceOK, raceFail)
	printBalances(l, "C", "B")

	fmt.Println("\n================ 场景 3：余额不足 / 账户冻结（无部分入账） ================")
	_, err = l.Transfer(ledger.TransferRequest{
		RequestID: "PAY-POOR", FromID: "B", ToID: "A", Amount: 99999999,
	}, sec(5))
	fmt.Printf("  余额不足 -> %v\n", err)

	must(l.SetAccountStatus("C", domain.AccountFrozen))
	_, err = l.Transfer(ledger.TransferRequest{
		RequestID: "PAY-FROZEN", FromID: "A", ToID: "C", Amount: 100,
	}, sec(6))
	fmt.Printf("  收款方冻结 -> %v\n", err)
	must(l.SetAccountStatus("C", domain.AccountActive))

	fmt.Println("\n================ 场景 4：冲正（可追溯、可重放、不可二次冲正） ================")
	orig := mustTransfer(l, ledger.TransferRequest{
		RequestID: "PAY-REV", FromID: "A", ToID: "C", Amount: 12000, Summary: "待退款订单",
	}, sec(7))
	rv, err := l.Reverse(ledger.ReversalRequest{
		RequestID: "REV-0001", OriginalID: orig.ID, Summary: "客诉全额退款",
	}, sec(8))
	must(err)
	fmt.Printf("  原始凭证=%s 冲正凭证=%s，冲正凭证 ReverseOf=%s，借贷各 %s\n",
		orig.ID, rv.ID, rv.ReverseOf, money(12000))
	rv2, err := l.Reverse(ledger.ReversalRequest{RequestID: "REV-0001", OriginalID: orig.ID}, sec(9))
	must(err)
	fmt.Printf("  冲正请求重复提交 -> 返回同一凭证 %s（相同=%v）\n", rv2.ID, rv2.ID == rv.ID)
	_, err = l.Reverse(ledger.ReversalRequest{RequestID: "REV-0002", OriginalID: orig.ID}, sec(10))
	fmt.Printf("  二次冲正 -> %v\n", err)
	printBalances(l, "A", "C")

	fmt.Println("\n================ 场景 5：对账（相符 + 缺账 + 重复 + 余额不符） ================")
	// 用一个独立小账本构造对账窗口。
	l2 := ledger.New(base)
	must(l2.CreateAccount("X", "付款方X", domain.AccountActive, base))
	must(l2.CreateAccount("Y", "收款方Y", domain.AccountActive, base))
	_, err = l2.Opening(ledger.OpeningRequest{RequestID: "op-X", AccountID: "X", Amount: 100000}, sec(0))
	must(err)
	mustTransfer(l2, ledger.TransferRequest{RequestID: "FLOW-1", FromID: "X", ToID: "Y", Amount: 20000}, sec(10))
	mustTransfer(l2, ledger.TransferRequest{RequestID: "FLOW-2", FromID: "X", ToID: "Y", Amount: 5000}, sec(11))
	// FLOW-3 外部有、账本无（缺账）；FLOW-2 外部重复推送一次（重复）。
	flows := []domain.ExternalFlow{
		{ExternalID: "CH-1", RequestID: "FLOW-1", FromID: "X", ToID: "Y", Amount: 20000, OccurredAt: sec(10)},
		{ExternalID: "CH-2", RequestID: "FLOW-2", FromID: "X", ToID: "Y", Amount: 5000, OccurredAt: sec(11)},
		{ExternalID: "CH-2-DUP", RequestID: "FLOW-2", FromID: "X", ToID: "Y", Amount: 5000, OccurredAt: sec(12)},
		{ExternalID: "CH-3", RequestID: "FLOW-3", FromID: "X", ToID: "Y", Amount: 8000, OccurredAt: sec(13)},
	}
	winStart, winEnd := sec(9), sec(20)
	vouchers := l2.VouchersInWindow(winStart, winEnd)
	balX, _ := l2.Balance("X")
	balY, _ := l2.Balance("Y")

	good := reconcile.Input{
		WindowStart: winStart, WindowEnd: winEnd, Vouchers: vouchers,
		Flows:            flows[:2], // 只给完全匹配的两条 -> 相符
		StartingBalances: map[string]int64{"X": 100000, "Y": 0},
		EndingBalances:   map[string]int64{"X": balX, "Y": balY},
	}
	printReport("对账 A（口径一致）", reconcile.Reconcile(good))

	bad := reconcile.Input{
		WindowStart: winStart, WindowEnd: winEnd, Vouchers: vouchers,
		Flows:            flows, // 缺账 + 重复
		StartingBalances: map[string]int64{"X": 100000, "Y": 0},
		// X 期末快照故意少 1 分，模拟并发丢失更新/被篡改。
		EndingBalances: map[string]int64{"X": balX - 1, "Y": balY},
	}
	printReport("对账 B（注入异常）", reconcile.Reconcile(bad))

	fmt.Println("\n演示完成：全部结论均可由请求标识、凭证号与借贷分录追溯解释。")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

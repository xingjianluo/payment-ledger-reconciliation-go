package ledger

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
)

func testNow() time.Time { return time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC) }

func newFundedLedger(t *testing.T, funds map[string]int64) *Ledger {
	t.Helper()
	l := New(testNow())
	if err := l.CreateAccount("A", "商户A", domain.AccountActive, testNow()); err != nil {
		t.Fatalf("开户A失败: %v", err)
	}
	if err := l.CreateAccount("B", "商户B", domain.AccountActive, testNow()); err != nil {
		t.Fatalf("开户B失败: %v", err)
	}
	if err := l.CreateAccount("C", "商户C", domain.AccountActive, testNow()); err != nil {
		t.Fatalf("开户C失败: %v", err)
	}
	i := 0
	for id, amt := range funds {
		i++
		if _, err := l.Opening(OpeningRequest{
			RequestID: fmt.Sprintf("opening-%s-%d", id, i),
			AccountID: id, Amount: amt, Summary: "期初注资",
		}, testNow()); err != nil {
			t.Fatalf("注资 %s 失败: %v", id, err)
		}
	}
	return l
}

func assertBalance(t *testing.T, l *Ledger, id string, want int64) {
	t.Helper()
	got, err := l.Balance(id)
	if err != nil {
		t.Fatalf("查询余额 %s 失败: %v", id, err)
	}
	if got != want {
		t.Errorf("账户 %s 余额 = %d 分, 期望 %d 分", id, got, want)
	}
}

func assertConservation(t *testing.T, l *Ledger, label string) {
	t.Helper()
	// 全系统（含期初权益账户）所有带符号分录之和必须为 0。
	ids := []string{"A", "B", "C", OpeningAccountID}
	var total int64
	for _, id := range ids {
		for _, e := range l.Entries(id) {
			total += e.SignedAmount()
		}
	}
	if total != 0 {
		t.Errorf("[%s] 全系统借贷不守恒，净额合计=%d 分，期望 0", label, total)
	}
}

func TestTransferSuccessAndBalances(t *testing.T) {
	l := newFundedLedger(t, map[string]int64{"A": 10000, "B": 5000})

	v, err := l.Transfer(TransferRequest{
		RequestID: "req-1", FromID: "A", ToID: "B", Amount: 3000, Summary: "订单1",
	}, testNow().Add(time.Second))
	if err != nil {
		t.Fatalf("转账被意外拒绝: %v", err)
	}
	debit, credit := TotalDebitCredit(v)
	if debit != 3000 || credit != 3000 {
		t.Errorf("凭证 %s 借贷不守恒: 借=%d 贷=%d", v.ID, debit, credit)
	}
	assertBalance(t, l, "A", 7000)
	assertBalance(t, l, "B", 8000)
	assertConservation(t, l, "普通转账后")
}

// 重复提交（完全相同请求）必须返回同一凭证，绝不重复记账。
func TestIdempotentDuplicateTransfer(t *testing.T) {
	l := newFundedLedger(t, map[string]int64{"A": 10000})

	req := TransferRequest{RequestID: "req-dup", FromID: "A", ToID: "B", Amount: 2000}
	v1, err := l.Transfer(req, testNow().Add(time.Second))
	if err != nil {
		t.Fatalf("首次转账失败: %v", err)
	}
	v2, err := l.Transfer(req, testNow().Add(2*time.Second))
	if err != nil {
		t.Fatalf("重复提交被意外拒绝: %v", err)
	}
	if v1.ID != v2.ID {
		t.Fatalf("重复提交生成了新凭证: 首次=%s 重复=%s", v1.ID, v2.ID)
	}
	assertBalance(t, l, "A", 8000)
	assertBalance(t, l, "B", 2000)
	if got := len(l.Entries("A")); got != 2 { // 1 条入金 + 1 条转账贷方
		t.Errorf("A 分录数=%d, 期望 2（重复提交不得重复记账）", got)
	}
	assertConservation(t, l, "重复提交后")
}

// 相同请求标识但内容冲突必须被拒绝，且失败原因可区分、可解包。
func TestIdempotencyConflict(t *testing.T) {
	l := newFundedLedger(t, map[string]int64{"A": 10000})

	_, err := l.Transfer(TransferRequest{RequestID: "req-x", FromID: "A", ToID: "B", Amount: 1000}, testNow())
	if err != nil {
		t.Fatalf("首次转账失败: %v", err)
	}
	_, err = l.Transfer(TransferRequest{RequestID: "req-x", FromID: "A", ToID: "C", Amount: 9999}, testNow())
	if err == nil {
		t.Fatal("内容冲突的重复请求必须被拒绝，实际成功")
	}
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("错误应可解包为 ErrIdempotencyConflict，实际: %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "req-x") || !strings.Contains(msg, "指纹") {
		t.Errorf("失败信息必须包含请求标识与判定依据，实际: %s", msg)
	}
	// 冲突请求不得改变任何余额。
	assertBalance(t, l, "A", 9000)
	assertBalance(t, l, "B", 1000)
	assertBalance(t, l, "C", 0)
	assertConservation(t, l, "幂等冲突后")
}

// 余额不足不得入账（无部分入账）。
func TestInsufficientFunds(t *testing.T) {
	l := newFundedLedger(t, map[string]int64{"A": 100})

	_, err := l.Transfer(TransferRequest{RequestID: "req-poor", FromID: "A", ToID: "B", Amount: 101}, testNow())
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("应返回 ErrInsufficientFunds，实际: %v", err)
	}
	if !strings.Contains(err.Error(), "req-poor") || !strings.Contains(err.Error(), "余额=100") {
		t.Errorf("失败信息须含请求标识与余额判定依据，实际: %s", err.Error())
	}
	assertBalance(t, l, "A", 100)
	assertBalance(t, l, "B", 0)
	if got := len(l.Entries("B")); got != 0 {
		t.Errorf("余额不足时收款方不得产生任何分录，实际 %d 条", got)
	}
	assertConservation(t, l, "余额不足后")

	// 余额不足的失败请求不应占用幂等键：补足条件后同标识请求可成功。
	if _, err := l.Opening(OpeningRequest{
		RequestID: "open-A-more", AccountID: "A", Amount: 1000,
	}, testNow()); err != nil {
		t.Fatalf("补注入金失败: %v", err)
	}
	v, err := l.Transfer(TransferRequest{RequestID: "req-poor", FromID: "A", ToID: "B", Amount: 101}, testNow())
	if err != nil {
		t.Fatalf("条件满足后同标识请求应成功，实际: %v", err)
	}
	if v == nil {
		t.Fatal("凭证不应为 nil")
	}
}

// 冻结账户不得入账。
func TestFrozenAccountRejected(t *testing.T) {
	l := newFundedLedger(t, map[string]int64{"A": 10000})
	if err := l.SetAccountStatus("B", domain.AccountFrozen); err != nil {
		t.Fatalf("冻结B失败: %v", err)
	}
	_, err := l.Transfer(TransferRequest{RequestID: "req-frozen", FromID: "A", ToID: "B", Amount: 100}, testNow())
	if !errors.Is(err, ErrAccountFrozen) {
		t.Fatalf("应返回 ErrAccountFrozen，实际: %v", err)
	}
	if !strings.Contains(err.Error(), "收款账户 B 状态=冻结") {
		t.Errorf("失败信息须说明哪个账户为何被拒，实际: %s", err.Error())
	}
	assertBalance(t, l, "A", 10000)
	assertBalance(t, l, "B", 0)
	assertConservation(t, l, "账户冻结后")
}

func TestInvalidTransfers(t *testing.T) {
	l := newFundedLedger(t, map[string]int64{"A": 1000})
	cases := []struct {
		name string
		req  TransferRequest
		want error
	}{
		{"空请求标识", TransferRequest{FromID: "A", ToID: "B", Amount: 10}, ErrEmptyRequestID},
		{"零金额", TransferRequest{RequestID: "r", FromID: "A", ToID: "B", Amount: 0}, ErrInvalidAmount},
		{"负金额", TransferRequest{RequestID: "r2", FromID: "A", ToID: "B", Amount: -5}, ErrInvalidAmount},
		{"同账户", TransferRequest{RequestID: "r3", FromID: "A", ToID: "A", Amount: 5}, ErrSameAccount},
		{"不存在账户", TransferRequest{RequestID: "r4", FromID: "A", ToID: "Z", Amount: 5}, ErrAccountNotFound},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := l.Transfer(c.req, testNow())
			if !errors.Is(err, c.want) {
				t.Fatalf("期望 %v，实际 %v", c.want, err)
			}
		})
	}
	assertConservation(t, l, "非法请求后")
}

func TestReversalTraceAndEffect(t *testing.T) {
	l := newFundedLedger(t, map[string]int64{"A": 10000, "B": 1000})

	v, err := l.Transfer(TransferRequest{
		RequestID: "tx-1", FromID: "A", ToID: "B", Amount: 4000,
	}, testNow().Add(time.Second))
	if err != nil {
		t.Fatalf("转账失败: %v", err)
	}
	rv, err := l.Reverse(ReversalRequest{
		RequestID: "rev-1", OriginalID: v.ID, Summary: "全额退款冲正",
	}, testNow().Add(2*time.Second))
	if err != nil {
		t.Fatalf("冲正失败: %v", err)
	}
	if rv.Type != domain.VoucherReversal || rv.ReverseOf != v.ID {
		t.Fatalf("冲正凭证追溯关联错误: %+v", rv)
	}
	debit, credit := TotalDebitCredit(rv)
	if debit != 4000 || credit != 4000 {
		t.Errorf("冲正凭证不守恒: 借=%d 贷=%d", debit, credit)
	}
	assertBalance(t, l, "A", 10000)
	assertBalance(t, l, "B", 1000)
	if got, ok := l.ReverseVoucherOf(v.ID); !ok || got != rv.ID {
		t.Errorf("原始凭证应能查到冲正凭证，实际 got=%s ok=%v", got, ok)
	}
	// 冲正重复提交幂等。
	rv2, err := l.Reverse(ReversalRequest{RequestID: "rev-1", OriginalID: v.ID}, testNow())
	if err != nil || rv2.ID != rv.ID {
		t.Fatalf("冲正重复提交应返回同一凭证，v2=%v err=%v", rv2, err)
	}
	// 原凭证不得被二次冲正。
	_, err = l.Reverse(ReversalRequest{RequestID: "rev-2", OriginalID: v.ID}, testNow())
	if !errors.Is(err, ErrAlreadyReversed) {
		t.Fatalf("二次冲正应返回 ErrAlreadyReversed，实际: %v", err)
	}
	if !strings.Contains(err.Error(), v.ID) {
		t.Errorf("二次冲正失败信息须含原始凭证标识，实际: %s", err.Error())
	}
	assertConservation(t, l, "冲正后")
}

func TestReversalGuards(t *testing.T) {
	l := newFundedLedger(t, map[string]int64{"A": 10000, "B": 1000})
	op, err := l.Opening(OpeningRequest{RequestID: "op-1", AccountID: "A", Amount: 100}, testNow())
	if err != nil {
		t.Fatalf("入金失败: %v", err)
	}
	// 不能冲正入金凭证。
	_, err = l.Reverse(ReversalRequest{RequestID: "rev-op", OriginalID: op.ID}, testNow())
	if !errors.Is(err, ErrOriginalNotTransfer) {
		t.Fatalf("冲正入金凭证应返回 ErrOriginalNotTransfer，实际: %v", err)
	}
	// 原始凭证不存在。
	_, err = l.Reverse(ReversalRequest{RequestID: "rev-missing", OriginalID: "TV-999999"}, testNow())
	if !errors.Is(err, ErrOriginalNotFound) {
		t.Fatalf("应返回 ErrOriginalNotFound，实际: %v", err)
	}
	// 原收款方余额不足（钱已被后续全额转走）时不得冲正。
	tv, err := l.Transfer(TransferRequest{RequestID: "tx-out", FromID: "A", ToID: "B", Amount: 1000}, testNow())
	if err != nil {
		t.Fatalf("转账失败: %v", err)
	}
	// B 此时余额 2000（期初1000+收入1000），全额转给 C 后归零。
	if _, err := l.Transfer(TransferRequest{
		RequestID: "tx-away", FromID: "B", ToID: "C", Amount: 2000,
	}, testNow()); err != nil {
		t.Fatalf("转出失败: %v", err)
	}
	_, err = l.Reverse(ReversalRequest{RequestID: "rev-ins", OriginalID: tv.ID}, testNow())
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("收款方余额不足时冲正应被拒绝，实际: %v", err)
	}
	if !strings.Contains(err.Error(), "rev-ins") {
		t.Errorf("失败信息须含请求标识，实际: %s", err.Error())
	}
	assertConservation(t, l, "冲正保护后")
}

// 并发下多笔不同请求 + 每个请求的大量重复提交：
// 只有每笔业务恰好记一次账，余额、流水与凭证保持一致，无丢失更新。
func TestConcurrentDuplicateSubmissions(t *testing.T) {
	l := newFundedLedger(t, map[string]int64{"A": 1_000_000}) // A 余额充足

	const (
		distinctTxs = 50
		duplicates  = 20
	)
	var wg sync.WaitGroup
	errCh := make(chan error, distinctTxs*duplicates)
	voucherCh := make(chan string, distinctTxs*duplicates)
	for i := 0; i < distinctTxs; i++ {
		req := TransferRequest{
			RequestID: fmt.Sprintf("concurrent-req-%03d", i),
			FromID:    "A", ToID: "B", Amount: int64(100 + i),
		}
		for j := 0; j < duplicates; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				v, err := l.Transfer(req, testNow())
				if err != nil {
					errCh <- fmt.Errorf("请求标识=%s 并发提交意外失败: %w", req.RequestID, err)
					return
				}
				voucherCh <- v.ID
			}()
		}
	}
	wg.Wait()
	close(errCh)
	close(voucherCh)
	for e := range errCh {
		t.Fatal(e)
	}

	// 每笔业务的全部重复提交必须返回同一个凭证 ID，且该 ID 出现 duplicates 次。
	counts := map[string]int{}
	for vid := range voucherCh {
		counts[vid]++
	}
	if len(counts) != distinctTxs {
		t.Fatalf("并发重复提交后凭证数=%d, 期望恰好 %d（每笔业务只记一次账）", len(counts), distinctTxs)
	}
	for vid, n := range counts {
		if n != duplicates {
			t.Errorf("凭证 %s 被返回 %d 次，期望 %d 次（重复提交必须稳定命中同一凭证）", vid, n, duplicates)
		}
	}
	// B 的分录数必须等于业务数（无重复分录）。
	if got := len(l.Entries("B")); got != distinctTxs {
		t.Fatalf("B 账户分录数=%d, 期望 %d（存在丢失更新或重复记账）", got, distinctTxs)
	}
	var expectSum int64
	for i := 0; i < distinctTxs; i++ {
		expectSum += int64(100 + i)
	}
	assertBalance(t, l, "A", 1_000_000-expectSum)
	assertBalance(t, l, "B", expectSum)
	assertConservation(t, l, "并发重复提交后")
}

// 并发争抢同一笔余额（A 只够支付部分请求）：成功笔数与最终余额必须精确一致。
func TestConcurrentBalanceContention(t *testing.T) {
	l := newFundedLedger(t, map[string]int64{"A": 1000})

	const n = 30
	var wg sync.WaitGroup
	var success, rejected int64
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := TransferRequest{
				RequestID: fmt.Sprintf("race-%02d", i), FromID: "A", ToID: "B", Amount: 100,
			}
			_, err := l.Transfer(req, testNow())
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if !errors.Is(err, ErrInsufficientFunds) {
					t.Errorf("请求标识=%s 非预期错误: %v", req.RequestID, err)
				}
				rejected++
			} else {
				success++
			}
		}(i)
	}
	wg.Wait()

	if success != 10 || rejected != 20 {
		t.Fatalf("成功=%d 拒绝=%d，期望 10/20（余额精确耗尽，无超扣无丢失更新）", success, rejected)
	}
	assertBalance(t, l, "A", 0)
	assertBalance(t, l, "B", 1000)
	assertConservation(t, l, "并发争抢余额后")
}

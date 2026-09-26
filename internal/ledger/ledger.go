// Package ledger 实现并发安全的复式记账核心：幂等转账、冲正、余额查询。
//
// 全部写操作在同一互斥锁内完成“校验 + 过账”，从而保证并发下不产生部分入账、
// 不出现丢失更新。金额单位均为“分”（int64），杜绝浮点误差。
package ledger

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
)

// OpeningAccountID 是内置的“期初权益”账户。入金时贷记该账户，
// 使全系统借贷始终守恒；它不参与对外转账，也不应被冻结。
const OpeningAccountID = "__OPENING_EQUITY__"

// 可区分的业务失败原因。
var (
	ErrEmptyRequestID      = errors.New("请求标识为空")
	ErrAccountNotFound     = errors.New("账户不存在")
	ErrAccountFrozen       = errors.New("账户状态不允许入账")
	ErrInsufficientFunds   = errors.New("余额不足")
	ErrInvalidAmount       = errors.New("金额非法")
	ErrSameAccount         = errors.New("付款方与收款方不能相同")
	ErrIdempotencyConflict = errors.New("幂等冲突：相同请求标识但内容不一致")
	ErrOriginalNotFound    = errors.New("被冲正的原始凭证不存在")
	ErrOriginalNotTransfer = errors.New("只能冲正转账凭证")
	ErrAlreadyReversed     = errors.New("原始凭证已被冲正")
	ErrAccountExists       = errors.New("账户已存在")
)

// RequestError 在失败原因之上附带请求标识与判定依据，
// 便于在日志和测试中断言“是哪一笔请求、为何被拒绝”。
type RequestError struct {
	RequestID string
	Op        string // transfer / reversal
	Reason    error
	Evidence  string // 判定依据
}

func (e *RequestError) Error() string {
	return fmt.Sprintf("[%s] 请求标识=%s 失败原因=%s；判定依据: %s",
		e.Op, e.RequestID, e.Reason.Error(), e.Evidence)
}

// Unwrap 支持 errors.Is 解包到底层原因。
func (e *RequestError) Unwrap() error { return e.Reason }

// TransferRequest 是一笔转账请求。
type TransferRequest struct {
	RequestID string // 幂等标识
	FromID    string // 付款账户
	ToID      string // 收款账户
	Amount    int64  // 金额（分），必须为正
	Summary   string
}

// ReversalRequest 是一笔冲正请求。
type ReversalRequest struct {
	RequestID  string // 冲正请求自身的幂等标识
	OriginalID string // 被冲正的原始转账凭证 ID
	Summary    string
}

// OpeningRequest 是一笔入金（期初注资）请求。
type OpeningRequest struct {
	RequestID string
	AccountID string
	Amount    int64
	Summary   string
}

// Ledger 是内存复式账本。
type Ledger struct {
	mu sync.Mutex

	accounts   map[string]*domain.Account
	entries    map[string][]domain.Entry // accountID -> 已过账分录
	vouchers   map[string]*domain.Voucher
	byRequest  map[string]domain.IdempotencyRecord // requestID -> 指纹与凭证
	reversalOf map[string]string                   // 原始转账凭证ID -> 冲正凭证ID
	voucherSeq int64
}

// New 创建一个空账本并自动开立内置的期初权益账户。
func New(now time.Time) *Ledger {
	l := &Ledger{
		accounts:   map[string]*domain.Account{},
		entries:    map[string][]domain.Entry{},
		vouchers:   map[string]*domain.Voucher{},
		byRequest:  map[string]domain.IdempotencyRecord{},
		reversalOf: map[string]string{},
	}
	l.accounts[OpeningAccountID] = &domain.Account{
		ID:        OpeningAccountID,
		Name:      "期初权益",
		Status:    domain.AccountActive,
		CreatedAt: now,
	}
	l.entries[OpeningAccountID] = nil
	return l
}

func transferFingerprint(r TransferRequest) string {
	return fmt.Sprintf("transfer|%s|%s|%d", r.FromID, r.ToID, r.Amount)
}

func reversalFingerprint(r ReversalRequest) string {
	return fmt.Sprintf("reversal|%s", r.OriginalID)
}

func openingFingerprint(r OpeningRequest) string {
	return fmt.Sprintf("opening|%s|%d", r.AccountID, r.Amount)
}

func (l *Ledger) newVoucherID(t domain.VoucherType) string {
	l.voucherSeq++
	switch t {
	case domain.VoucherReversal:
		return fmt.Sprintf("RV-%06d", l.voucherSeq)
	case domain.VoucherOpening:
		return fmt.Sprintf("OP-%06d", l.voucherSeq)
	default:
		return fmt.Sprintf("TV-%06d", l.voucherSeq)
	}
}

func (l *Ledger) fail(reqID, op string, reason error, evidence string) error {
	return &RequestError{RequestID: reqID, Op: op, Reason: reason, Evidence: evidence}
}

// CreateAccount 开户。重复开户返回错误。
func (l *Ledger) CreateAccount(id, name string, status domain.AccountStatus, at time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if strings.TrimSpace(id) == "" {
		return errors.New("账户ID为空")
	}
	if _, ok := l.accounts[id]; ok {
		return fmt.Errorf("%w: %s", ErrAccountExists, id)
	}
	l.accounts[id] = &domain.Account{ID: id, Name: name, Status: status, CreatedAt: at}
	l.entries[id] = nil
	return nil
}

// SetAccountStatus 变更账户状态（冻结/解冻）。
func (l *Ledger) SetAccountStatus(id string, status domain.AccountStatus) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	acc, ok := l.accounts[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrAccountNotFound, id)
	}
	acc.Status = status
	return nil
}

// balanceLocked 调用方必须持有锁。
func (l *Ledger) balanceLocked(accountID string) int64 {
	var sum int64
	for _, e := range l.entries[accountID] {
		sum += e.SignedAmount()
	}
	return sum
}

// Transfer 以幂等方式提交转账。
//
// 重复提交（相同 RequestID 且内容一致）返回首次创建的同一凭证，绝不重复记账；
// 内容冲突（相同 RequestID 但收付款方或金额不同）返回可解包为
// ErrIdempotencyConflict 的错误，并给出双方指纹作为判定依据。
// 余额不足、账户冻结、账户不存在等情况一律不过账，不产生部分入账。
func (l *Ledger) Transfer(req TransferRequest, at time.Time) (*domain.Voucher, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	fp := transferFingerprint(req)

	// 幂等判定先于一切业务校验：保证重复请求无论何时都得到一致结论。
	if rec, ok := l.byRequest[req.RequestID]; ok {
		if rec.Fingerprint != fp {
			return nil, l.fail(req.RequestID, "transfer", ErrIdempotencyConflict,
				fmt.Sprintf("已存指纹=%q，本次指纹=%q", rec.Fingerprint, fp))
		}
		return l.vouchers[rec.VoucherID], nil
	}

	if strings.TrimSpace(req.RequestID) == "" {
		return nil, l.fail(req.RequestID, "transfer", ErrEmptyRequestID, "RequestID 不得为空")
	}
	if req.Amount <= 0 {
		return nil, l.fail(req.RequestID, "transfer", ErrInvalidAmount,
			fmt.Sprintf("金额必须为正，实际=%d", req.Amount))
	}
	if req.FromID == req.ToID {
		return nil, l.fail(req.RequestID, "transfer", ErrSameAccount,
			fmt.Sprintf("付款方与收款方同为=%s", req.FromID))
	}
	from, ok := l.accounts[req.FromID]
	if !ok {
		return nil, l.fail(req.RequestID, "transfer", ErrAccountNotFound,
			fmt.Sprintf("付款账户 %s 不存在", req.FromID))
	}
	to, ok := l.accounts[req.ToID]
	if !ok {
		return nil, l.fail(req.RequestID, "transfer", ErrAccountNotFound,
			fmt.Sprintf("收款账户 %s 不存在", req.ToID))
	}
	if from.Status != domain.AccountActive {
		return nil, l.fail(req.RequestID, "transfer", ErrAccountFrozen,
			fmt.Sprintf("付款账户 %s 状态=%s", req.FromID, from.Status))
	}
	if to.Status != domain.AccountActive {
		return nil, l.fail(req.RequestID, "transfer", ErrAccountFrozen,
			fmt.Sprintf("收款账户 %s 状态=%s", req.ToID, to.Status))
	}
	current := l.balanceLocked(req.FromID)
	if current < req.Amount {
		return nil, l.fail(req.RequestID, "transfer", ErrInsufficientFunds,
			fmt.Sprintf("付款账户 %s 当前余额=%d 分，申请金额=%d 分", req.FromID, current, req.Amount))
	}

	// 全部校验通过后才过账：借方记收款方（资产增加），贷方记付款方（资产减少）。
	vid := l.newVoucherID(domain.VoucherTransfer)
	entries := []domain.Entry{
		{VoucherID: vid, AccountID: req.ToID, Direction: domain.Debit, Amount: req.Amount},
		{VoucherID: vid, AccountID: req.FromID, Direction: domain.Credit, Amount: req.Amount},
	}
	v := &domain.Voucher{
		ID:        vid,
		Type:      domain.VoucherTransfer,
		RequestID: req.RequestID,
		Entries:   entries,
		PostedAt:  at,
		Summary:   req.Summary,
	}
	l.postLocked(v)
	l.byRequest[req.RequestID] = domain.IdempotencyRecord{
		RequestID: req.RequestID, Fingerprint: fp, VoucherID: vid, CreatedAt: at,
	}
	return v, nil
}

// postLocked 将凭证落库并追加各账户分录。调用方必须持有锁。
func (l *Ledger) postLocked(v *domain.Voucher) {
	l.vouchers[v.ID] = v
	for _, e := range v.Entries {
		l.entries[e.AccountID] = append(l.entries[e.AccountID], e)
	}
}

// Reverse 以幂等方式冲正一笔已存在的转账。
//
// 冲正凭证采用与原转账方向相反的分录（借原付款方、贷原收款方），
// 通过 ReverseOf 字段与原始凭证保持可追溯关联。
// 冲正同样受账户状态约束，且收款方（原收款方）余额不足时拒绝，
// 因此任何失败路径都不会产生部分入账。同一原始凭证只允许冲正一次。
func (l *Ledger) Reverse(req ReversalRequest, at time.Time) (*domain.Voucher, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	fp := reversalFingerprint(req)

	if rec, ok := l.byRequest[req.RequestID]; ok {
		if rec.Fingerprint != fp {
			return nil, l.fail(req.RequestID, "reversal", ErrIdempotencyConflict,
				fmt.Sprintf("已存指纹=%q，本次指纹=%q", rec.Fingerprint, fp))
		}
		return l.vouchers[rec.VoucherID], nil
	}

	if strings.TrimSpace(req.RequestID) == "" {
		return nil, l.fail(req.RequestID, "reversal", ErrEmptyRequestID, "RequestID 不得为空")
	}
	orig, ok := l.vouchers[req.OriginalID]
	if !ok {
		return nil, l.fail(req.RequestID, "reversal", ErrOriginalNotFound,
			fmt.Sprintf("原始凭证 %s 不存在", req.OriginalID))
	}
	if orig.Type != domain.VoucherTransfer {
		return nil, l.fail(req.RequestID, "reversal", ErrOriginalNotTransfer,
			fmt.Sprintf("原始凭证 %s 类型=%s", req.OriginalID, orig.Type))
	}
	if rvID, reversed := l.reversalOf[orig.ID]; reversed {
		return nil, l.fail(req.RequestID, "reversal", ErrAlreadyReversed,
			fmt.Sprintf("原始凭证 %s 已被冲正凭证 %s 冲正", orig.ID, rvID))
	}

	// 从原始凭证解析收付款方与金额，保证冲正内容严格等于原始业务内容。
	var fromID, toID string
	var amount int64
	for _, e := range orig.Entries {
		switch {
		case e.Direction == domain.Credit:
			fromID = e.AccountID
			amount = e.Amount
		case e.Direction == domain.Debit:
			toID = e.AccountID
		}
	}
	from := l.accounts[fromID]
	to := l.accounts[toID]
	if from.Status != domain.AccountActive {
		return nil, l.fail(req.RequestID, "reversal", ErrAccountFrozen,
			fmt.Sprintf("原付款账户 %s 状态=%s", fromID, from.Status))
	}
	if to.Status != domain.AccountActive {
		return nil, l.fail(req.RequestID, "reversal", ErrAccountFrozen,
			fmt.Sprintf("原收款账户 %s 状态=%s", toID, to.Status))
	}
	if bal := l.balanceLocked(toID); bal < amount {
		return nil, l.fail(req.RequestID, "reversal", ErrInsufficientFunds,
			fmt.Sprintf("原收款账户 %s 当前余额=%d 分，待冲正金额=%d 分", toID, bal, amount))
	}

	// 反向分录过账。
	vid := l.newVoucherID(domain.VoucherReversal)
	entries := []domain.Entry{
		{VoucherID: vid, AccountID: fromID, Direction: domain.Debit, Amount: amount},
		{VoucherID: vid, AccountID: toID, Direction: domain.Credit, Amount: amount},
	}
	v := &domain.Voucher{
		ID:        vid,
		Type:      domain.VoucherReversal,
		RequestID: req.RequestID,
		Entries:   entries,
		ReverseOf: orig.ID,
		PostedAt:  at,
		Summary:   req.Summary,
	}
	l.postLocked(v)
	l.reversalOf[orig.ID] = vid
	l.byRequest[req.RequestID] = domain.IdempotencyRecord{
		RequestID: req.RequestID, Fingerprint: fp, VoucherID: vid, CreatedAt: at,
	}
	return v, nil
}

// Opening 以幂等方式为账户注资（期初余额）。贷记内置期初权益账户，保证全系统守恒。
func (l *Ledger) Opening(req OpeningRequest, at time.Time) (*domain.Voucher, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	fp := openingFingerprint(req)
	if rec, ok := l.byRequest[req.RequestID]; ok {
		if rec.Fingerprint != fp {
			return nil, l.fail(req.RequestID, "opening", ErrIdempotencyConflict,
				fmt.Sprintf("已存指纹=%q，本次指纹=%q", rec.Fingerprint, fp))
		}
		return l.vouchers[rec.VoucherID], nil
	}
	if strings.TrimSpace(req.RequestID) == "" {
		return nil, l.fail(req.RequestID, "opening", ErrEmptyRequestID, "RequestID 不得为空")
	}
	if req.Amount <= 0 {
		return nil, l.fail(req.RequestID, "opening", ErrInvalidAmount,
			fmt.Sprintf("金额必须为正，实际=%d", req.Amount))
	}
	acc, ok := l.accounts[req.AccountID]
	if !ok {
		return nil, l.fail(req.RequestID, "opening", ErrAccountNotFound,
			fmt.Sprintf("入账账户 %s 不存在", req.AccountID))
	}
	if acc.Status != domain.AccountActive {
		return nil, l.fail(req.RequestID, "opening", ErrAccountFrozen,
			fmt.Sprintf("入账账户 %s 状态=%s", req.AccountID, acc.Status))
	}

	vid := l.newVoucherID(domain.VoucherOpening)
	entries := []domain.Entry{
		{VoucherID: vid, AccountID: req.AccountID, Direction: domain.Debit, Amount: req.Amount},
		{VoucherID: vid, AccountID: OpeningAccountID, Direction: domain.Credit, Amount: req.Amount},
	}
	v := &domain.Voucher{
		ID:        vid,
		Type:      domain.VoucherOpening,
		RequestID: req.RequestID,
		Entries:   entries,
		PostedAt:  at,
		Summary:   req.Summary,
	}
	l.postLocked(v)
	l.byRequest[req.RequestID] = domain.IdempotencyRecord{
		RequestID: req.RequestID, Fingerprint: fp, VoucherID: vid, CreatedAt: at,
	}
	return v, nil
}

// Balance 返回账户当前余额（全部已过账分录的带符号净额）。
func (l *Ledger) Balance(accountID string) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.accounts[accountID]; !ok {
		return 0, fmt.Errorf("%w: %s", ErrAccountNotFound, accountID)
	}
	return l.balanceLocked(accountID), nil
}

// Account 查询账户副本。
func (l *Ledger) Account(id string) (*domain.Account, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	a, ok := l.accounts[id]
	if !ok {
		return nil, false
	}
	cp := *a
	return &cp, true
}

// Voucher 查询凭证副本。
func (l *Ledger) Voucher(id string) (*domain.Voucher, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	v, ok := l.vouchers[id]
	if !ok {
		return nil, false
	}
	return cloneVoucher(v), true
}

// VoucherByRequest 按幂等标识查询已受理凭证。
func (l *Ledger) VoucherByRequest(requestID string) (*domain.Voucher, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec, ok := l.byRequest[requestID]
	if !ok {
		return nil, false
	}
	return cloneVoucher(l.vouchers[rec.VoucherID]), true
}

// Entries 返回账户全部已过账分录的副本（按过账先后顺序）。
func (l *Ledger) Entries(accountID string) []domain.Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	src := l.entries[accountID]
	out := make([]domain.Entry, len(src))
	copy(out, src)
	return out
}

// VouchersInWindow 返回过账时间落在 [start, end) 的全部凭证（按过账时间、再按 ID 排序）。
func (l *Ledger) VouchersInWindow(start, end time.Time) []*domain.Voucher {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []*domain.Voucher
	for _, v := range l.vouchers {
		if !v.PostedAt.Before(start) && v.PostedAt.Before(end) {
			out = append(out, cloneVoucher(v))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PostedAt.Equal(out[j].PostedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].PostedAt.Before(out[j].PostedAt)
	})
	return out
}

// ReverseVoucherOf 返回冲正指定原始凭证的冲正凭证 ID（若存在）。
func (l *Ledger) ReverseVoucherOf(originalID string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	id, ok := l.reversalOf[originalID]
	return id, ok
}

func cloneVoucher(v *domain.Voucher) *domain.Voucher {
	cp := *v
	cp.Entries = append([]domain.Entry(nil), v.Entries...)
	return &cp
}

// TotalDebitCredit 返回某张凭证的借方合计与贷方合计（守恒校验用）。
func TotalDebitCredit(v *domain.Voucher) (debit, credit int64) {
	for _, e := range v.Entries {
		if e.Direction == domain.Debit {
			debit += e.Amount
		} else {
			credit += e.Amount
		}
	}
	return debit, credit
}

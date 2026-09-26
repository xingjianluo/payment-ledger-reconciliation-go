package ledger

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/store"
)

// Service 账本核心服务：用一把全局锁串行化所有写操作，
// 在单进程内存模型下保证复式记账守恒、余额无丢失更新与幂等语义。
type Service struct {
	mu    sync.RWMutex
	store store.Store
	seq   atomic.Int64
}

func NewService(st store.Store) *Service {
	return &Service{store: st}
}

// CreateAccount 开户。
func (s *Service) CreateAccount(ctx context.Context, acc domain.Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if acc.CreatedAt.IsZero() {
		acc.CreatedAt = s.now()
	}
	acc.UpdatedAt = acc.CreatedAt
	if acc.Status == "" {
		acc.Status = domain.AccountActive
	}
	return s.store.CreateAccount(ctx, acc)
}

// Transfer 提交转账（借转入方 / 贷转出方）。
//
// 幂等语义：
//   - 相同 requestID + 相同内容：返回首次产生的凭证（重复提交不重复记账）；
//   - 相同 requestID + 内容冲突：返回 KindIdempotentMiss；
//   - 首次请求确定性失败（余额不足/账户状态等）也会落幂等记录，重复提交得到相同结论。
func (s *Service) Transfer(ctx context.Context, req domain.TransferRequest) (domain.Voucher, error) {
	if err := validateTransfer(req); err != nil {
		return domain.Voucher{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok, err := s.store.GetIdemp(ctx, req.RequestID)
	if err != nil {
		return domain.Voucher{}, err
	}
	if ok {
		return s.replay(ctx, rec, req.Fingerprint())
	}

	// 全部校验通过后才落任何数据，杜绝部分入账。
	from, err := s.store.GetAccount(ctx, req.FromID)
	if err != nil {
		return s.fail(ctx, req.RequestID, req.Fingerprint(), err)
	}
	to, err := s.store.GetAccount(ctx, req.ToID)
	if err != nil {
		return s.fail(ctx, req.RequestID, req.Fingerprint(), err)
	}
	if !from.CanPost() {
		err = domain.NewError(domain.KindAccountInactive, req.RequestID,
			"debit/credit posting not allowed: source account inactive",
			fmt.Sprintf("account_id=%s status=%s", from.ID, from.Status))
		return s.fail(ctx, req.RequestID, req.Fingerprint(), err)
	}
	if !to.CanPost() {
		err = domain.NewError(domain.KindAccountInactive, req.RequestID,
			"debit/credit posting not allowed: destination account inactive",
			fmt.Sprintf("account_id=%s status=%s", to.ID, to.Status))
		return s.fail(ctx, req.RequestID, req.Fingerprint(), err)
	}
	if from.Balance < req.Amount {
		err = domain.NewError(domain.KindInsufficient, req.RequestID,
			"insufficient funds",
			fmt.Sprintf("account_id=%s balance=%d required=%d", from.ID, from.Balance, req.Amount))
		return s.fail(ctx, req.RequestID, req.Fingerprint(), err)
	}

	now := s.now()
	vid := s.nextVoucherID("V")
	voucher := domain.Voucher{
		ID:            vid,
		Type:          domain.VoucherTransfer,
		RequestID:     req.RequestID,
		Amount:        req.Amount,
		FromAccountID: req.FromID,
		ToAccountID:   req.ToID,
		CreatedAt:     now,
		Narration:     req.Narration,
		Entries: []domain.Entry{
			{AccountID: to.ID, Side: domain.Debit, Amount: req.Amount},    // 借：转入方
			{AccountID: from.ID, Side: domain.Credit, Amount: req.Amount}, // 贷：转出方
		},
	}
	if !voucher.Balanced() {
		return domain.Voucher{}, domain.NewError(domain.KindInvalidRequest, req.RequestID, "unbalanced voucher", "")
	}
	if err := s.post(ctx, voucher); err != nil {
		return domain.Voucher{}, err
	}
	_ = s.store.PutIdemp(ctx, domain.IdempRecord{
		RequestID:   req.RequestID,
		Fingerprint: req.Fingerprint(),
		VoucherID:   vid,
		Status:      domain.IdempSucceeded,
		CreatedAt:   now,
	})
	return voucher, nil
}

// Reverse 对原始转账做全额红字冲正（方向相反的凭证），并与原始凭证双向可追溯。
func (s *Service) Reverse(ctx context.Context, req domain.ReversalRequest) (domain.Voucher, error) {
	if req.RequestID == "" {
		return domain.Voucher{}, domain.NewError(domain.KindInvalidRequest, "", "empty request id", "")
	}
	if req.OriginalRequest == "" {
		return domain.Voucher{}, domain.NewError(domain.KindInvalidRequest, req.RequestID, "empty original request id", "")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok, err := s.store.GetIdemp(ctx, req.RequestID)
	if err != nil {
		return domain.Voucher{}, err
	}
	if ok {
		return s.replay(ctx, rec, req.Fingerprint())
	}

	orig, found, err := s.store.GetVoucherByRequest(ctx, req.OriginalRequest)
	if err != nil {
		return domain.Voucher{}, err
	}
	if !found {
		err = domain.NewError(domain.KindVoucherNotFound, req.RequestID,
			"original voucher not found for reversal",
			"original_request_id="+req.OriginalRequest)
		return s.fail(ctx, req.RequestID, req.Fingerprint(), err)
	}
	if orig.Type != domain.VoucherTransfer {
		err = domain.NewError(domain.KindInvalidRequest, req.RequestID,
			"only transfer vouchers can be reversed",
			fmt.Sprintf("original_voucher_id=%s type=%s", orig.ID, orig.Type))
		return s.fail(ctx, req.RequestID, req.Fingerprint(), err)
	}
	// 原始凭证已被冲正？
	if rv, found, err := s.store.GetReversalOf(ctx, orig.ID); err != nil {
		return domain.Voucher{}, err
	} else if found {
		err = domain.NewError(domain.KindAlreadyReversed, req.RequestID,
			"original voucher already reversed",
			fmt.Sprintf("original_voucher_id=%s reversal_voucher_id=%s", orig.ID, rv.ID))
		return s.fail(ctx, req.RequestID, req.Fingerprint(), err)
	}

	from, err := s.store.GetAccount(ctx, orig.FromAccountID)
	if err != nil {
		return s.fail(ctx, req.RequestID, req.Fingerprint(), err)
	}
	to, err := s.store.GetAccount(ctx, orig.ToAccountID)
	if err != nil {
		return s.fail(ctx, req.RequestID, req.Fingerprint(), err)
	}
	if !from.CanPost() {
		err = domain.NewError(domain.KindAccountInactive, req.RequestID,
			"reversal posting not allowed: original source account inactive",
			fmt.Sprintf("account_id=%s status=%s", from.ID, from.Status))
		return s.fail(ctx, req.RequestID, req.Fingerprint(), err)
	}
	if !to.CanPost() {
		err = domain.NewError(domain.KindAccountInactive, req.RequestID,
			"reversal posting not allowed: original payee account inactive",
			fmt.Sprintf("account_id=%s status=%s", to.ID, to.Status))
		return s.fail(ctx, req.RequestID, req.Fingerprint(), err)
	}
	// 冲正把资金从原转入方划回；原转入方余额可能已被后续业务花掉。
	if to.Balance < orig.Amount {
		err = domain.NewError(domain.KindInsufficient, req.RequestID,
			"reversal rejected: funds no longer available at original payee",
			fmt.Sprintf("account_id=%s balance=%d required=%d original_voucher_id=%s",
				to.ID, to.Balance, orig.Amount, orig.ID))
		return s.fail(ctx, req.RequestID, req.Fingerprint(), err)
	}

	now := s.now()
	vid := s.nextVoucherID("R")
	voucher := domain.Voucher{
		ID:            vid,
		Type:          domain.VoucherReversal,
		RequestID:     req.RequestID,
		Amount:        orig.Amount,
		FromAccountID: orig.ToAccountID, // 资金回流：原转入方划出
		ToAccountID:   orig.FromAccountID,
		OriginalID:    orig.ID,
		CreatedAt:     now,
		Narration:     req.Narration,
		Entries: []domain.Entry{
			{AccountID: orig.FromAccountID, Side: domain.Debit, Amount: orig.Amount},
			{AccountID: orig.ToAccountID, Side: domain.Credit, Amount: orig.Amount},
		},
	}
	if !voucher.Balanced() {
		return domain.Voucher{}, domain.NewError(domain.KindInvalidRequest, req.RequestID, "unbalanced reversal voucher", "")
	}
	if err := s.post(ctx, voucher); err != nil {
		return domain.Voucher{}, err
	}
	_ = s.store.PutIdemp(ctx, domain.IdempRecord{
		RequestID:   req.RequestID,
		Fingerprint: req.Fingerprint(),
		VoucherID:   vid,
		Status:      domain.IdempSucceeded,
		CreatedAt:   now,
	})
	return voucher, nil
}

// post 在调用方已持锁、已完成全部校验的前提下，原子写入凭证、流水与余额。
// 各步之间没有可失败的业务分支；若存储写入异常，错误直接上抛（内存实现不会失败）。
func (s *Service) post(ctx context.Context, v domain.Voucher) error {
	if err := s.store.PutVoucher(ctx, v); err != nil {
		return err
	}
	ts := v.CreatedAt.UnixNano()
	for _, e := range v.Entries {
		le := store.LedgerEntry{
			VoucherID:  v.ID,
			RequestID:  v.RequestID,
			AccountID:  e.AccountID,
			Side:       e.Side,
			Amount:     e.Amount,
			OccurredAt: ts,
			Kind:       v.Type,
		}
		if v.Type == domain.VoucherReversal {
			le.Related = v.OriginalID
		} else {
			le.Related = counterparty(v, e.AccountID)
		}
		if err := s.store.AppendEntry(ctx, le); err != nil {
			return err
		}
	}
	// 更新余额：借 + / 贷 -。在全局写锁内，读改写不会丢失更新。
	delta := map[string]int64{}
	for _, e := range v.Entries {
		if e.Side == domain.Debit {
			delta[e.AccountID] += e.Amount
		} else {
			delta[e.AccountID] -= e.Amount
		}
	}
	for id, d := range delta {
		acc, err := s.store.GetAccount(ctx, id)
		if err != nil {
			return err
		}
		acc.Balance += d
		acc.Version++
		acc.UpdatedAt = v.CreatedAt
		if err := s.store.UpdateAccount(ctx, acc); err != nil {
			return err
		}
	}
	return nil
}

// fail 记录确定性失败结论并重放同一错误，保证重复请求结论一致。
func (s *Service) fail(ctx context.Context, requestID, fingerprint string, bizErr error) (domain.Voucher, error) {
	le, _ := domain.AsLedgerError(bizErr)
	rec := domain.IdempRecord{
		RequestID:   requestID,
		Fingerprint: fingerprint,
		Status:      domain.IdempFailed,
		CreatedAt:   s.now(),
	}
	if le != nil {
		rec.FailedKind = le.Kind
	}
	_ = s.store.PutIdemp(ctx, rec)
	return domain.Voucher{}, bizErr
}

// replay 处理命中幂等记录的重复提交。
func (s *Service) replay(ctx context.Context, rec domain.IdempRecord, incomingFingerprint string) (domain.Voucher, error) {
	requestID := rec.RequestID
	if rec.Fingerprint != incomingFingerprint {
		return domain.Voucher{}, domain.NewError(domain.KindIdempotentMiss, requestID,
			"idempotent request replayed with conflicting content",
			fmt.Sprintf("request_id=%s stored_fingerprint=%q incoming_fingerprint=%q",
				requestID, rec.Fingerprint, incomingFingerprint))
	}
	if rec.Status == domain.IdempFailed {
		return domain.Voucher{}, domain.NewError(rec.FailedKind, requestID,
			"request previously rejected with deterministic result",
			fmt.Sprintf("request_id=%s original_failure=%s", requestID, rec.FailedKind))
	}
	v, ok, err := s.store.GetVoucher(ctx, rec.VoucherID)
	if err != nil {
		return domain.Voucher{}, err
	}
	if !ok {
		return domain.Voucher{}, domain.NewError(domain.KindVoucherNotFound, requestID,
			"idempotent record points to missing voucher", "voucher_id="+rec.VoucherID)
	}
	return v, nil
}

func validateTransfer(req domain.TransferRequest) error {
	if req.RequestID == "" {
		return domain.NewError(domain.KindInvalidRequest, "", "empty request id", "")
	}
	if req.FromID == "" || req.ToID == "" {
		return domain.NewError(domain.KindInvalidRequest, req.RequestID, "empty account id",
			fmt.Sprintf("from=%q to=%q", req.FromID, req.ToID))
	}
	if req.FromID == req.ToID {
		return domain.NewError(domain.KindInvalidRequest, req.RequestID, "same source and destination account",
			"account_id="+req.FromID)
	}
	if req.Amount <= 0 {
		return domain.NewError(domain.KindInvalidRequest, req.RequestID, "amount must be positive",
			fmt.Sprintf("amount=%d", req.Amount))
	}
	return nil
}

func counterparty(v domain.Voucher, accountID string) string {
	switch accountID {
	case v.FromAccountID:
		return v.ToAccountID
	case v.ToAccountID:
		return v.FromAccountID
	default:
		return ""
	}
}

func (s *Service) nextVoucherID(prefix string) string {
	n := s.seq.Add(1)
	return fmt.Sprintf("%s%012d", prefix, n)
}

func (s *Service) now() time.Time { return time.Now().UTC() }

// ---- 只读查询（快照读，使用读锁；与写操作互斥但读之间共享）----

func (s *Service) GetAccount(ctx context.Context, id string) (domain.Account, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.store.GetAccount(ctx, id)
}

func (s *Service) GetVoucher(ctx context.Context, id string) (domain.Voucher, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.store.GetVoucher(ctx, id)
}

func (s *Service) GetVoucherByRequest(ctx context.Context, requestID string) (domain.Voucher, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.store.GetVoucherByRequest(ctx, requestID)
}

// View 在读锁快照内执行只读操作（如对账），保证读取期间无写入。
func (s *Service) View(ctx context.Context, fn func(context.Context) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fn(ctx)
}

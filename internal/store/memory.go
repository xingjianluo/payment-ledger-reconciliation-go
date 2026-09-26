package store

import (
	"context"
	"sort"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
)

// MemoryStore 基于 map 的内存实现。
// 约定：写操作由 ledger.Service 全局串行化，因此这里不再加锁；
// 读方法（对账）在服务锁外调用时通过 RLock 提供快照读。
type MemoryStore struct {
	accounts  map[string]domain.Account
	vouchers  map[string]domain.Voucher
	byReq     map[string]string // requestID -> voucherID
	reversals map[string]string // originalVoucherID -> reversalVoucherID
	idemps    map[string]domain.IdempRecord
	entries   []LedgerEntry
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		accounts:  map[string]domain.Account{},
		vouchers:  map[string]domain.Voucher{},
		byReq:     map[string]string{},
		reversals: map[string]string{},
		idemps:    map[string]domain.IdempRecord{},
	}
}

func (s *MemoryStore) CreateAccount(_ context.Context, acc domain.Account) error {
	if _, ok := s.accounts[acc.ID]; ok {
		return domain.NewError(domain.KindInvalidRequest, "", "account already exists", "account_id="+acc.ID)
	}
	s.accounts[acc.ID] = acc
	return nil
}

func (s *MemoryStore) GetAccount(_ context.Context, id string) (domain.Account, error) {
	acc, ok := s.accounts[id]
	if !ok {
		return domain.Account{}, domain.NewError(domain.KindAccountNotFound, "", "account not found", "account_id="+id)
	}
	return acc, nil
}

func (s *MemoryStore) UpdateAccount(_ context.Context, acc domain.Account) error {
	if _, ok := s.accounts[acc.ID]; !ok {
		return domain.NewError(domain.KindAccountNotFound, "", "account not found", "account_id="+acc.ID)
	}
	s.accounts[acc.ID] = acc
	return nil
}

func (s *MemoryStore) ListAccounts(_ context.Context) ([]domain.Account, error) {
	out := make([]domain.Account, 0, len(s.accounts))
	for _, a := range s.accounts {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *MemoryStore) PutVoucher(_ context.Context, v domain.Voucher) error {
	if _, ok := s.vouchers[v.ID]; ok {
		return domain.NewError(domain.KindInvalidRequest, v.RequestID, "voucher already exists", "voucher_id="+v.ID)
	}
	s.vouchers[v.ID] = v
	s.byReq[v.RequestID] = v.ID
	if v.Type == domain.VoucherReversal {
		s.reversals[v.OriginalID] = v.ID
	}
	return nil
}

func (s *MemoryStore) GetVoucherByRequest(_ context.Context, requestID string) (domain.Voucher, bool, error) {
	id, ok := s.byReq[requestID]
	if !ok {
		return domain.Voucher{}, false, nil
	}
	return s.vouchers[id], true, nil
}

func (s *MemoryStore) GetVoucher(_ context.Context, id string) (domain.Voucher, bool, error) {
	v, ok := s.vouchers[id]
	return v, ok, nil
}

func (s *MemoryStore) ListVouchers(_ context.Context, from, to int64) ([]domain.Voucher, error) {
	out := make([]domain.Voucher, 0)
	for _, v := range s.vouchers {
		ts := v.CreatedAt.UnixNano()
		if ts >= from && ts < to {
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *MemoryStore) PutIdemp(_ context.Context, rec domain.IdempRecord) error {
	s.idemps[rec.RequestID] = rec
	return nil
}

func (s *MemoryStore) GetIdemp(_ context.Context, requestID string) (domain.IdempRecord, bool, error) {
	rec, ok := s.idemps[requestID]
	return rec, ok, nil
}

func (s *MemoryStore) GetReversalOf(_ context.Context, originalVoucherID string) (domain.Voucher, bool, error) {
	id, ok := s.reversals[originalVoucherID]
	if !ok {
		return domain.Voucher{}, false, nil
	}
	return s.vouchers[id], true, nil
}

func (s *MemoryStore) AppendEntry(_ context.Context, e LedgerEntry) error {
	if e.Seq <= 0 {
		e.Seq = int64(len(s.entries)) + 1
	}
	s.entries = append(s.entries, e)
	return nil
}

func (s *MemoryStore) ListEntries(_ context.Context, from, to int64) ([]LedgerEntry, error) {
	out := make([]LedgerEntry, 0)
	for _, e := range s.entries {
		if e.OccurredAt >= from && e.OccurredAt < to {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

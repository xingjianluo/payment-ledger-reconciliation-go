package store

import (
	"context"

	"github.com/xingjianluo/payment-ledger-reconciliation-go/internal/domain"
)

// Store 是账本持久化的最小抽象（本地内存实现）。
// 写操作由 ledger.Service 全局串行化；读方法需自行保证快照一致性。
type Store interface {
	CreateAccount(ctx context.Context, acc domain.Account) error
	GetAccount(ctx context.Context, id string) (domain.Account, error)
	UpdateAccount(ctx context.Context, acc domain.Account) error
	ListAccounts(ctx context.Context) ([]domain.Account, error)

	PutVoucher(ctx context.Context, v domain.Voucher) error
	GetVoucherByRequest(ctx context.Context, requestID string) (domain.Voucher, bool, error)
	GetVoucher(ctx context.Context, id string) (domain.Voucher, bool, error)
	ListVouchers(ctx context.Context, from, to int64) ([]domain.Voucher, error)

	PutIdemp(ctx context.Context, rec domain.IdempRecord) error
	GetIdemp(ctx context.Context, requestID string) (domain.IdempRecord, bool, error)

	// GetReversalOf 返回冲正指定原始凭证的凭证（若存在）。
	GetReversalOf(ctx context.Context, originalVoucherID string) (domain.Voucher, bool, error)

	// AppendEntry 追加一条流水（凭证的一条分录展开为一条流水）。
	AppendEntry(ctx context.Context, e LedgerEntry) error
	ListEntries(ctx context.Context, from, to int64) ([]LedgerEntry, error)
}

// LedgerEntry 账户流水：凭证分录的展开形式，是对账的事实依据。
type LedgerEntry struct {
	Seq        int64
	VoucherID  string
	RequestID  string
	AccountID  string
	Side       domain.EntrySide
	Amount     int64
	OccurredAt int64 // UnixNano
	Related    string
	Kind       domain.VoucherType
}

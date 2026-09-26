package domain

import (
	"fmt"
	"strings"
	"time"
)

// 金额模型：最小货币单位的非负整数。方向由 EntrySide 表达，金额本身一律非负。

// AccountStatus 账户状态。
type AccountStatus string

const (
	AccountActive AccountStatus = "ACTIVE"
	AccountFrozen AccountStatus = "FROZEN"
	AccountClosed AccountStatus = "CLOSED"
)

// Account 账户及其当前余额。
type Account struct {
	ID        string
	Name      string
	Status    AccountStatus
	Balance   int64
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// CanPost 账户是否允许入账。
func (a Account) CanPost() bool { return a.Status == AccountActive }

// EntrySide 借贷方向。
type EntrySide string

const (
	Debit  EntrySide = "DEBIT"
	Credit EntrySide = "CREDIT"
)

// VoucherType 凭证类型。
type VoucherType string

const (
	VoucherTransfer VoucherType = "TRANSFER"
	VoucherReversal VoucherType = "REVERSAL"
)

// Entry 凭证中的一条分录。
type Entry struct {
	AccountID string
	Side      EntrySide
	Amount    int64
}

// Voucher 复式记账凭证：一笔业务对应一张凭证，借贷必须守恒。
type Voucher struct {
	ID            string
	Type          VoucherType
	RequestID     string // 幂等标识
	Entries       []Entry
	Amount        int64
	FromAccountID string
	ToAccountID   string
	OriginalID    string // 冲正凭证指向原始凭证
	CreatedAt     time.Time
	Narration     string
}

// Balanced 判断借贷是否守恒（借贷各自合计相等且为正）。
func (v Voucher) Balanced() bool {
	var debit, credit int64
	for _, e := range v.Entries {
		if e.Amount <= 0 {
			return false
		}
		switch e.Side {
		case Debit:
			debit += e.Amount
		case Credit:
			credit += e.Amount
		default:
			return false
		}
	}
	return debit > 0 && debit == credit
}

// TransferRequest 转账请求。
type TransferRequest struct {
	RequestID string
	FromID    string
	ToID      string
	Amount    int64
	Narration string
}

// Fingerprint 返回请求内容指纹。幂等键相同但指纹不同即视为内容冲突。
func (r TransferRequest) Fingerprint() string {
	return strings.Join([]string{
		"TRANSFER",
		r.FromID,
		r.ToID,
		fmt.Sprintf("%d", r.Amount),
	}, "|")
}

// ReversalRequest 冲正请求。
type ReversalRequest struct {
	RequestID       string
	OriginalRequest string // 原始转账的幂等标识
	Narration       string
}

// Fingerprint 冲正内容由原始请求唯一决定。
func (r ReversalRequest) Fingerprint() string {
	return "REVERSE|" + r.OriginalRequest
}

// IdempStatus 幂等记录的处理状态。
type IdempStatus string

const (
	IdempSucceeded IdempStatus = "SUCCEEDED"
	IdempFailed    IdempStatus = "FAILED"
)

// IdempRecord 幂等记录。
type IdempRecord struct {
	RequestID   string
	Fingerprint string
	VoucherID   string
	Status      IdempStatus
	FailedKind  FailureKind // 失败的确定性结论，重复请求原样返回
	CreatedAt   time.Time
}

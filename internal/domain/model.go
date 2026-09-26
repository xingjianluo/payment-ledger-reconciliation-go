// Package domain 定义支付账本的核心领域模型：账户、复式凭证、分录、
// 幂等请求记录以及对账差异分类。
package domain

import "time"

// Direction 表示分录的记账方向。
type Direction int

const (
	Debit  Direction = 1 // 借方
	Credit Direction = 2 // 贷方
)

func (d Direction) String() string {
	switch d {
	case Debit:
		return "借"
	case Credit:
		return "贷"
	default:
		return "未知方向"
	}
}

// AccountStatus 表示账户状态。
type AccountStatus int

const (
	AccountActive AccountStatus = 1 // 正常，允许入账
	AccountFrozen AccountStatus = 2 // 冻结，拒绝入账
)

func (s AccountStatus) String() string {
	switch s {
	case AccountActive:
		return "正常"
	case AccountFrozen:
		return "冻结"
	default:
		return "未知状态"
	}
}

// VoucherType 表示凭证类型。
type VoucherType int

const (
	VoucherTransfer VoucherType = 1 // 转账
	VoucherReversal VoucherType = 2 // 冲正
	VoucherOpening  VoucherType = 3 // 入金/期初（对开“期初权益”账户）
)

func (t VoucherType) String() string {
	switch t {
	case VoucherTransfer:
		return "转账"
	case VoucherReversal:
		return "冲正"
	case VoucherOpening:
		return "入金"
	default:
		return "未知类型"
	}
}

// Account 是一个资产类账户。金额以“分”为单位，借方流水为正、贷方流水为负，
// 余额等于全部已过账分录净额之和。
type Account struct {
	ID        string
	Name      string
	Status    AccountStatus
	CreatedAt time.Time
}

// Entry 是凭证中的一条复式分录。
type Entry struct {
	VoucherID string
	AccountID string
	Direction Direction
	Amount    int64 // 恒为正的金额（单位：分）
}

// SignedAmount 返回带符号金额：借方为正，贷方为负。
func (e Entry) SignedAmount() int64 {
	if e.Direction == Debit {
		return e.Amount
	}
	return -e.Amount
}

// Voucher 是一笔已过账的复式记账凭证。
type Voucher struct {
	ID        string
	Type      VoucherType
	RequestID string // 幂等标识
	Entries   []Entry
	// ReverseOf 仅冲正凭证使用：被冲正的原始转账凭证 ID。
	ReverseOf string
	PostedAt  time.Time
	// Summary 保存转账/冲正的摘要信息（如付款方、收款方），便于追溯。
	Summary string
}

// IdempotencyRecord 记录一个已受理请求的内容指纹，用于重复请求判定。
type IdempotencyRecord struct {
	RequestID   string
	Fingerprint string
	VoucherID   string
	CreatedAt   time.Time
}

// IssueType 是对账差异的分类。
type IssueType int

const (
	IssueMissingEntry      IssueType = 1 // 缺账：外部有流水，账本无凭证
	IssueUnexpectedEntry   IssueType = 2 // 多账：账本有凭证，外部无流水
	IssueDuplicateEntry    IssueType = 3 // 重复：外部同一笔业务出现多次
	IssueAmountMismatch    IssueType = 4 // 金额不符：同一笔业务金额不一致
	IssueBalanceMismatch   IssueType = 5 // 余额不符：流水净额与账户余额不一致
	IssueUnbalancedVoucher IssueType = 6 // 凭证借贷不守恒
)

func (t IssueType) String() string {
	switch t {
	case IssueMissingEntry:
		return "缺账"
	case IssueUnexpectedEntry:
		return "多账"
	case IssueDuplicateEntry:
		return "重复"
	case IssueAmountMismatch:
		return "金额不符"
	case IssueBalanceMismatch:
		return "余额不符"
	case IssueUnbalancedVoucher:
		return "凭证借贷不守恒"
	default:
		return "未知差异"
	}
}

// ExternalFlow 表示外部渠道（如支付通道对账单）上的一条业务流水。
type ExternalFlow struct {
	ExternalID string // 渠道流水号
	RequestID  string // 业务幂等标识，用于与账本凭证匹配
	FromID     string // 付款账户
	ToID       string // 收款账户
	Amount     int64  // 金额（分），恒为正
	OccurredAt time.Time
}

// Issue 描述一条对账差异及其判定依据。
type Issue struct {
	Type      IssueType
	RequestID string // 相关业务请求标识（余额/守恒类差异可能为空）
	VoucherID string
	AccountID string
	Detail    string // 判定依据
	Expected  int64  // 期望值（部分类型适用）
	Actual    int64  // 实际值（部分类型适用）
}

// ReconcileReport 是一次时间窗口对账的结论。
type ReconcileReport struct {
	WindowStart time.Time
	WindowEnd   time.Time
	Issues      []Issue
}

// Balanced 报告是否无任何差异。
func (r *ReconcileReport) Balanced() bool { return len(r.Issues) == 0 }

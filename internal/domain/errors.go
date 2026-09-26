package domain

import (
	"errors"
	"fmt"
)

// FailureKind 标识请求被拒绝/对账异常的可区分原因。
type FailureKind string

const (
	KindNone            FailureKind = ""
	KindIdempotentMiss  FailureKind = "IDEMPOTENT_CONTENT_MISMATCH"
	KindAccountNotFound FailureKind = "ACCOUNT_NOT_FOUND"
	KindAccountInactive FailureKind = "ACCOUNT_INACTIVE"
	KindInsufficient    FailureKind = "INSUFFICIENT_FUNDS"
	KindVoucherNotFound FailureKind = "VOUCHER_NOT_FOUND"
	KindAlreadyReversed FailureKind = "VOUCHER_ALREADY_REVERSED"
	KindInvalidRequest  FailureKind = "INVALID_REQUEST"
)

// LedgerError 携带失败类别、请求标识与判定依据，便于日志与测试断言。
type LedgerError struct {
	Kind      FailureKind
	RequestID string
	Message   string
	Evidence  string

	wrappedErr error
}

func (e *LedgerError) Error() string {
	s := fmt.Sprintf("ledger error kind=%s", e.Kind)
	if e.RequestID != "" {
		s += " request_id=" + e.RequestID
	}
	if e.Message != "" {
		s += ": " + e.Message
	}
	if e.Evidence != "" {
		s += " | evidence: " + e.Evidence
	}
	if e.wrappedErr != nil {
		s += " | cause: " + e.wrappedErr.Error()
	}
	return s
}

func (e *LedgerError) Unwrap() error { return e.wrappedErr }

// NewError 构造业务错误（供 ledger 包复用）。
func NewError(kind FailureKind, requestID, message, evidence string) *LedgerError {
	return &LedgerError{
		Kind:      kind,
		RequestID: requestID,
		Message:   message,
		Evidence:  evidence,
	}
}

// AsLedgerError 提取业务错误。
func AsLedgerError(err error) (*LedgerError, bool) {
	if err == nil {
		return nil, false
	}
	var le *LedgerError
	if errors.As(err, &le) {
		return le, true
	}
	return nil, false
}

package store

// 以下方法仅用于本地演练与测试：在不经过业务服务的情况下篡改账本数据，
// 以验证对账器能否把差异按类型准确识别出来。生产代码路径不应调用。

// CorruptDropEntries 删除指定凭证的全部流水（模拟缺账）。返回删除条数。
func (s *MemoryStore) CorruptDropEntries(voucherID string) int {
	kept := s.entries[:0]
	n := 0
	for _, e := range s.entries {
		if e.VoucherID == voucherID {
			n++
			continue
		}
		kept = append(kept, e)
	}
	s.entries = kept
	return n
}

// CorruptDuplicateEntries 复制指定凭证的全部流水（模拟重复入账/重放落库）。
func (s *MemoryStore) CorruptDuplicateEntries(voucherID string) int {
	var dup []LedgerEntry
	for _, e := range s.entries {
		if e.VoucherID == voucherID {
			cp := e
			cp.Seq = 0 // AppendEntry 会重新分配序号
			dup = append(dup, cp)
		}
	}
	for _, e := range dup {
		s.AppendEntry(nil, e)
	}
	return len(dup)
}

// CorruptEntryAmount 篡改指定凭证第一条流水的金额（模拟金额不符）。
func (s *MemoryStore) CorruptEntryAmount(voucherID string, newAmount int64) bool {
	for i := range s.entries {
		if s.entries[i].VoucherID == voucherID {
			s.entries[i].Amount = newAmount
			return true
		}
	}
	return false
}

// CorruptOrphanEntry 复制一条流水并指向不存在的凭证（模拟有账无凭）。
func (s *MemoryStore) CorruptOrphanEntry(voucherID, fakeVoucherID string) bool {
	for i := range s.entries {
		if s.entries[i].VoucherID == voucherID {
			cp := s.entries[i]
			cp.VoucherID = fakeVoucherID
			cp.RequestID = "FAKE-" + fakeVoucherID
			cp.Seq = 0
			s.AppendEntry(nil, cp)
			return true
		}
	}
	return false
}

// CorruptBalance 直接改写账户余额，制造账实不符（余额 vs 流水）。
func (s *MemoryStore) CorruptBalance(accountID string, balance int64) bool {
	acc, ok := s.accounts[accountID]
	if !ok {
		return false
	}
	acc.Balance = balance
	s.accounts[accountID] = acc
	return true
}

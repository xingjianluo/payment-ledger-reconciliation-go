package channelrecon

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// ChannelTxn 渠道对账单中的一笔交易明细。
type ChannelTxn struct {
	ChannelTxnID string // 渠道流水号（渠道侧唯一标识）
	RequestID    string // 对应的业务请求标识（与账本 RequestID 对应）
	Direction    string // 渠道视角方向：DEBIT=支付出金 / CREDIT=退款入金
	Amount       int64  // 最小货币单位，非负
	OccurredAt   int64  // 发生时间，UnixNano
	Status       string // 渠道侧状态，仅 SUCCESS 参与配对
}

// StatusSuccess 渠道侧成功状态；其它状态的明细不参与配对但会被计数报告。
const StatusSuccess = "SUCCESS"

// fingerprint 返回明细内容指纹，用于导入去重。
func (t ChannelTxn) fingerprint() string {
	return strings.Join([]string{
		t.ChannelTxnID,
		t.RequestID,
		t.Direction,
		strconv.FormatInt(t.Amount, 10),
		strconv.FormatInt(t.OccurredAt, 10),
		t.Status,
	}, "|")
}

// Statement 渠道对账单：支持分批导入，重复导入自动去重。
type Statement struct {
	seen map[string]string // channelTxnID -> fingerprint
	txns []ChannelTxn
}

func NewStatement() *Statement {
	return &Statement{seen: map[string]string{}}
}

// Import 导入一批明细。流水号与内容指纹完全一致的记录视为同一份对账单的
// 重复导入，去重且不计入差异；返回本次实际新增条数。
// 同一流水号但内容不同的记录不丢弃（不静默忽略），照常保留，
// 后续配对时会以渠道侧重复/不一致的形式暴露。
func (s *Statement) Import(txns []ChannelTxn) int {
	added := 0
	for _, t := range txns {
		fp := t.fingerprint()
		if prev, ok := s.seen[t.ChannelTxnID]; ok && prev == fp {
			continue // 完全相同的重复导入
		}
		s.seen[t.ChannelTxnID] = fp
		s.txns = append(s.txns, t)
		added++
	}
	return added
}

// Txns 返回窗口 [from,to) 内的全部明细（未按状态过滤）。
// 边界与账本侧一致：左闭右开，边界只算一次。
func (s *Statement) Txns(from, to int64) []ChannelTxn {
	out := make([]ChannelTxn, 0, len(s.txns))
	for _, t := range s.txns {
		if t.OccurredAt >= from && t.OccurredAt < to {
			out = append(out, t)
		}
	}
	return out
}

// Len 返回对账单明细总数。
func (s *Statement) Len() int { return len(s.txns) }

// ParseCSV 从本地 CSV 读取渠道明细（列：channel_txn_id,request_id,direction,
// amount,occurred_at,status；occurred_at 支持 UnixNano 整数或 RFC3339）。
// 只依赖 io.Reader，不访问网络或外部系统。
func ParseCSV(r io.Reader) ([]ChannelTxn, error) {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true
	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read csv header: %w", err)
	}
	col := map[string]int{}
	for i, name := range header {
		col[strings.TrimSpace(name)] = i
	}
	for _, need := range []string{"channel_txn_id", "request_id", "direction", "amount", "occurred_at", "status"} {
		if _, ok := col[need]; !ok {
			return nil, fmt.Errorf("csv missing required column %q", need)
		}
	}

	var out []ChannelTxn
	line := 1
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			return out, nil
		}
		line++
		if err != nil {
			return nil, fmt.Errorf("csv line %d: %w", line, err)
		}
		get := func(name string) string { return strings.TrimSpace(rec[col[name]]) }

		amount, err := strconv.ParseInt(get("amount"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("csv line %d: bad amount %q: %w", line, get("amount"), err)
		}
		occurredAt, err := parseTime(get("occurred_at"))
		if err != nil {
			return nil, fmt.Errorf("csv line %d: %w", line, err)
		}
		dir := strings.ToUpper(get("direction"))
		if dir != "DEBIT" && dir != "CREDIT" {
			return nil, fmt.Errorf("csv line %d: bad direction %q (want DEBIT/CREDIT)", line, dir)
		}
		out = append(out, ChannelTxn{
			ChannelTxnID: get("channel_txn_id"),
			RequestID:    get("request_id"),
			Direction:    dir,
			Amount:       amount,
			OccurredAt:   occurredAt,
			Status:       strings.ToUpper(get("status")),
		})
	}
}

// parseTime 支持 UnixNano 整数或 RFC3339 时间串。
func parseTime(s string) (int64, error) {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0, fmt.Errorf("bad occurred_at %q (want UnixNano or RFC3339)", s)
	}
	return t.UnixNano(), nil
}

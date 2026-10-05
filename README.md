# 支付账本与对账核心（本地、零外部依赖）

一套可完全离线运行的支付账本核心：并发入账、请求级幂等、全额冲正、复式记账守恒、
以及只报告不修复的对账差异分类。仅使用 Go 标准库，不需要网络、数据库或真实凭据。

## 目录结构

```
internal/domain/        账务领域模型：账户、凭证、分录、幂等记录、业务错误类型
internal/store/         内存存储（map + 有序流水），含仅供测试的故障注入方法
internal/ledger/        账本核心服务：转账、冲正、幂等判定、串行化过账
internal/reconcile/     对账器：凭证/流水/余额三方核对与差异分类
internal/channelrecon/  渠道对账：对账单导入（CSV）、双边配对与差异分类
cmd/ledgerdemo/         本地可运行的端到端演示
```

## 账务模型

- **账户（Account）**：余额以最小货币单位的非负整数表示；状态为
  `ACTIVE / FROZEN / CLOSED`，非 `ACTIVE` 不允许任何入账。
- **凭证（Voucher）**：一笔业务一张凭证，含两条或多条**分录（Entry）**。
- **复式记账**：每张凭证的借方合计必须等于贷方合计且为正（`Voucher.Balanced()`）。
  转账业务固定为：

  | 方向 | 账户 | 含义 |
  |---|---|---|
  | 借 DEBIT | 转入方 | 资产增加 |
  | 贷 CREDIT | 转出方 | 资产减少 |

  余额更新规则：借 +、贷 −。
- **流水（LedgerEntry）**：凭证每条分录原子展开为一条带全局递增序号的流水，
  是对账的事实依据。过账顺序为「写凭证 → 写流水 → 改余额」，全部业务校验在
  写任何数据之前完成，因此余额不足或账户状态不允许时**不会产生任何部分入账**。

## 并发安全与一致性

- `ledger.Service` 用一把 `sync.RWMutex` 串行化全部写操作（单进程内存模型下
  等价于串行事务），杜绝读—改—写丢失更新；读接口与对账走 `RLock` 快照。
- 每次余额更新带 `Version` 递增，便于核对与追踪。
- 所有测试以 `go test -race` 运行，包含 64 协程并发重复提交与 200 笔并发转账场景，
  断言：只有一张凭证、余额精确守恒、借贷各自合计相等。

## 幂等规则

- 客户端必须为每个业务请求提供 **RequestID（幂等标识）**。
- 服务端保存 `(RequestID → 请求内容指纹, 凭证, 结论)`。指纹由
  业务关键字段构成（转账：类型｜转出方｜转入方｜金额）。
   narration 等备注字段不参与指纹。
- 重复提交的三种判定：

  | 情形 | 结论 | FailureKind |
  |---|---|---|
  | RequestID 相同、指纹相同（成功） | 返回首次凭证，不重复记账 | — |
  | RequestID 相同、指纹相同（首次确定性失败） | 返回与首次完全相同的失败结论 | 原失败类别 |
  | RequestID 相同、指纹不同 | **拒绝**，返回内容冲突 | `IDEMPOTENT_CONTENT_MISMATCH` |

  错误信息同时带 `request_id`、`stored_fingerprint` 与 `incoming_fingerprint` 作为判定依据。

## 冲正规则

- 冲正通过 `Reverse(ReversalRequest{RequestID, OriginalRequest})` 提交，
  同样受 RequestID 幂等保护。
- 冲正生成一张**全额反向凭证**（`VoucherType=REVERSAL`），方向与原始凭证相反，
  `OriginalID` 指向原始凭证；冲正流水的 `Related` 字段也记录原始凭证号，双向可追溯。
- 原始凭证只能被冲正一次：第二个冲正请求返回 `VOUCHER_ALREADY_REVERSED`。
- 找不到原始凭证返回 `VOUCHER_NOT_FOUND`；冲正时原转入方资金已不足
  （被后续业务花掉）返回 `INSUFFICIENT_FUNDS`，不产生任何账务变更。
- 冲正不删除、不改写原始凭证；更正后的净效果体现为两张凭证并存。

## 对账口径

`Reconciler.Reconcile(ctx, from, to, opts)` 核对 `[from, to)` 时间窗口
（UnixNano，基于凭证创建时间/流水发生时间）：

1. **凭证自检**：每张凭证借贷必须守恒，否则记 `UNBALANCED_VOUCHER`。
2. **凭证 → 流水（有凭必有账）**，按 (凭证, 账户, 借贷方向) 归组比对：
   - 凭证分录在流水中缺失 → `MISSING_ENTRY`（缺账）；
   - 同一分录出现多于一条流水 → `DUPLICATE_ENTRY`（重复）；
   - 流水金额合计与凭证不符 → `AMOUNT_MISMATCH`（金额不符）。
3. **流水 → 凭证（有账必有凭）**：流水指向窗口内不存在的凭证
   → `ORPHAN_ENTRY`（有账无凭）。
4. **余额核对**，二选一：
   - 传 `Options.OpeningBalances`（推荐，生产中为上一日切余额快照）：
     校验 `当前余额 − 期初余额 == 窗口内借/贷流水净额`，不符记 `BALANCE_MISMATCH`；
   - 不传：用**全部历史流水**重算账户余额并与账面余额核对（适用于期初余额为零
     的体系，或全新账本的自检）。

对账器**只读、只分类、不静默忽略、不自动修复**；每条 `Diff` 都带
`kind / account_id / voucher_id / request_id / expected / actual / evidence`，
`Report.CountByKind` 提供按类型的计数。

## 渠道对账（账本 ↔ 渠道对账单）

`channelrecon` 包在账本自身核对之上增加「拿渠道对账单来跟账本对」的双边核对，
全程本地完成：不连外部系统、不需要网络或真实凭据。

**渠道数据模型（ChannelTxn）**：每笔渠道明细带
`渠道流水号 / 业务请求标识(RequestID) / 方向 / 金额 / 发生时间 / 状态`。
方向为渠道视角：`DEBIT`=支付出金、`CREDIT`=退款入金；仅 `SUCCESS` 状态参与配对，
其它状态在报告中以 `SkippedChannel` 计数（不静默丢弃）。

**导入与构造**：

```go
txns, _ := channelrecon.ParseCSV(csvReader) // 本地 CSV，列见下
stmt := channelrecon.NewStatement()
stmt.Import(txns)                           // 可分批；重复导入自动去重
```

CSV 列为 `channel_txn_id,request_id,direction,amount,occurred_at,status`，
`occurred_at` 支持 UnixNano 整数或 RFC3339。导入按「流水号 + 内容指纹」去重：
同一份对账单重复导入/重跑不会把差异重复计数；同一流水号但内容不同的记录
不丢弃，会在配对时以渠道侧重复的形式暴露。

**双边核对**：

```go
crec := channelrecon.NewReconciler(st)
var rep channelrecon.Report
svc.View(ctx, func(ctx context.Context) error { // 读锁快照，与并发入账互斥
    var err error
    rep, err = crec.Reconcile(ctx, stmt, from, to, &reconcile.Options{OpeningBalances: opening})
    return err
})
```

- 先完整执行原有账本自身核对，结论原样保留在 `Report.Internal`；
  再按 **RequestID** 把窗口内账本凭证与渠道明细配对，差异各自成类：

  | 类别 | 含义 |
  |---|---|
  | `LEDGER_ONLY` | 账本有、渠道没有（渠道未达/漏记） |
  | `CHANNEL_ONLY` | 渠道有、账本没有（渠道多记/账本未入账；若凭证在窗口外会注明） |
  | `FIELD_MISMATCH` | 两边都有但金额或方向对不上（TRANSFER↔DEBIT、REVERSAL↔CREDIT） |
  | `CHANNEL_DUPLICATE` | 渠道自身重复：同一 RequestID 多条明细（证据注明内容是否一致） |

  每条 `ChannelDiff` 都带 RequestID，并同时带出账本侧与渠道侧各自的金额、
  时间与凭证号/渠道流水号作为判定依据；`Report.CountByKind` 按类型计数，
  各类不混淆、不静默忽略；重复组不再参与配对，避免一笔业务被重复计数。

**与原有核对口径的兼容范围**：

- 时间窗口同为 `[from, to)` 左闭右开：边界上的渠道明细只计入一个窗口，
  相邻窗口既不重复也不遗漏；账本侧沿用凭证创建时间，渠道侧用明细发生时间。
- 余额核对口径（`Options.OpeningBalances` / 全量重算）与内部核对完全一致，
  结论见 `Report.Internal`。
- 整个过程只读：在 `ledger.Service.View` 读锁快照内执行，并发入账时报告
  基于同一个一致时点，不会看到只写了一半的凭证或流水。
- 配对按 RequestID 归组 + 哈希表匹配，几千条渠道明细可在线性时间内完成
  （测试覆盖 5000 条，约百毫秒级）。

## 失败原因一览（FailureKind）

`IDEMPOTENT_CONTENT_MISMATCH`、`ACCOUNT_NOT_FOUND`、`ACCOUNT_INACTIVE`、
`INSUFFICIENT_FUNDS`、`VOUCHER_NOT_FOUND`、`VOUCHER_ALREADY_REVERSED`、
`INVALID_REQUEST`。所有错误均为 `*domain.LedgerError`，可用
`domain.AsLedgerError(err)` 提取类别、请求标识与判定依据。

## 本地验证

无需任何外部依赖：

```bash
# 端到端演示：转账 / 幂等重放 / 冲突拒绝 / 余额不足 / 冲正 / 对账
go run ./cmd/ledgerdemo

# 全部测试（含并发竞态检测）
go test ./... -race -count=1 -v
```

测试覆盖：

- 并发重复提交（64 协程同一 RequestID）：仅一张凭证、余额无丢失更新；
- 200 笔并发不同请求：成功/失败结论确定、余额精确等于初始值加减；
- 幂等重放返回同一凭证；内容冲突被区分拒绝且原始凭证不受影响；
- 余额不足、账户冻结：零流水、零余额变更（无部分入账），且失败结论可重放；
- 冲正与原始凭证可追溯、净效果为零、重复冲正被拒；
- 对账差异六类：缺账、重复、金额不符、有账无凭、借贷不守恒、余额不符，
  以及时间窗口过滤与干净账本零差异；
- 渠道对账：全部配对成功并保留内部核对结论、账本有渠道没有、渠道有账本没有
  （含凭证落在窗口外的证据）、金额/方向对不上、渠道自身重复（内容一致与不一致）、
  非成功状态计数、窗口边界只算一次、对账单重复导入与重跑去重、
  并发入账下快照一致（-race）、5000 条渠道明细性能、CSV 解析与端到端核对。

## 范围与限制

- 存储为进程内内存实现，重启数据不保留；接口化设计下可替换为持久化实现
  （替换后需用数据库事务/乐观锁承担本层内存锁的串行化职责）。
- 单币种、金额为整数最小单位；不涉及换汇与手续费。
- 时间戳取自进程本地时钟，仅用于对账窗口划分，不依赖外部时钟源。

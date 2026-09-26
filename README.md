# 支付账本与对账核心（本地、零外部依赖）

一套可完全离线运行的支付账本核心：并发入账、请求级幂等、全额冲正、复式记账守恒、
以及只报告不修复的对账差异分类。仅使用 Go 标准库，不需要网络、数据库或真实凭据。

## 目录结构

```
internal/domain/      账务领域模型：账户、凭证、分录、幂等记录、业务错误类型
internal/store/       内存存储（map + 有序流水），含仅供测试的故障注入方法
internal/ledger/      账本核心服务：转账、冲正、幂等判定、串行化过账
internal/reconcile/   对账器：凭证/流水/余额三方核对与差异分类
cmd/ledgerdemo/       本地可运行的端到端演示
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
  以及时间窗口过滤与干净账本零差异。

## 范围与限制

- 存储为进程内内存实现，重启数据不保留；接口化设计下可替换为持久化实现
  （替换后需用数据库事务/乐观锁承担本层内存锁的串行化职责）。
- 单币种、金额为整数最小单位；不涉及换汇与手续费。
- 时间戳取自进程本地时钟，仅用于对账窗口划分，不依赖外部时钟源。

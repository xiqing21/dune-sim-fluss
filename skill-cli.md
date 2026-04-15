
## Dune Skill — 区块链数据 SQL 查询平台

**核心定位**：通过 DuneSQL (Trino-based) 查询100+条链的链上数据

**主要能力**：

- `dune query run-sql` — 直接执行 DuneSQL，做任意 SQL 分析
- `dune query create/run/update` — 管理保存的查询，支持参数化
- `dune dataset search` — 搜索数据集目录（canonical/decoded/spell/community）
- `dune dataset search-by-contract` — 按合约地址查找已解码表
- `dune docs search` — 搜索 Dune 文档（无需认证）
- `dune usage` — 查看信用点消耗

**适用场景**：历史时序分析、跨表 JOIN、聚合统计、复杂 SQL 分析

---

## Sim Skill — 实时区块链数据 API

**核心定位**：实时、预索引的区块链数据即时查询，无需 SQL

**主要能力**：

- `dune sim evm balances` — 多链代币余额（含 USD 估值）
- `dune sim evm activity` — 解码的钱包活动流（转账/swap/approve）
- `dune sim evm transactions` — 原始交易历史（支持 ABI 解码）
- `dune sim evm token-info` — 代币元数据/价格/市值
- `dune sim evm token-holders` — ERC20 持有人排行榜
- `dune sim evm collectibles` — NFT 持仓（含垃圾过滤）
- `dune sim evm defi-positions` — DeFi 持仓跨协议聚合
- `dune sim evm stablecoins` — 稳定币余额
- `dune sim svm balances/transactions` — Solana 支持（beta）

**适用场景**：钱包/特定地址的当前状态查询、实时余额、代币价格、NFT 持仓

---

## 两者配合使用

| 需求                 | 用哪个            | 原因                 |
| -------------------- | ----------------- | -------------------- |
| 查某钱包的实时余额   | **Sim**     | 即时返回，无需写 SQL |
| 查某代币当前价格     | **Sim**     | 实时 DEX 价格        |
| 历史 30 天交易量趋势 | **DuneSQL** | Sim 不支持时序       |
| 跨地址聚合分析       | **DuneSQL** | Sim 只查单地址       |
| 查 NFT 持仓+垃圾过滤 | **Sim**     | 内置 spam filter     |
| DeFi 持仓快照        | **Sim**     | 跨协议聚合           |

如果你想用这些 skill 辅助生成文档，可以这样操作：

1. 用 `dune sim evm supported-chains` 获取支持的链列表
2. 用 `dune dataset search` 发现相关数据表
3. 用 `dune query run-sql` 跑实际查询获取示例数据
4. 用 `dune sim evm balances/activity` 获取实时数据样例

需要我帮你用这些 skill 跑一些查询来丰富文档内容吗？

# 从数据孤岛到统一智能：Web3 实时数据架构的下一个范式

**XINXIN Zheng | Web3 Festival 2026, Hong Kong**

---

## 我是谁

5 年国家电网大数据数仓实战经验，深耕 TB 级海量数据治理、数仓架构设计与实时计算优化。深度掌握 FlinkCDC → Fluss → Flink → Paimon 全链路技术栈，是国内最早系统性研究 Fluss 湖流一体架构并将其映射到 Web3 场景的工程师之一。同时具备 Vibecoding 能力——能通过 AI 辅助快速原型开发和产品构建，将技术想法在极短时间内转化为可交互的 Demo。

我在寻找：Web3 数据基础设施团队的 **高级工程师 / 技术合伙人** 职位，以我的大数据实战经验和架构设计能力，帮助团队突破多源数据融合和实时计算的瓶颈。

联系方式：xiaosuange@gmail.com | +86 176 0347 4321

---

## 我看到了什么问题

当前 Web3 数据分析领域存在三个根本性缺陷，而现有的头部平台都无法解决：

### 问题一：数据源割裂——链上、链下、私有数据无法统一

Dune 和 Nansen 只能查询链上数据，无法在同一查询中关联 DEX Swap 事件、CEX 订单簿深度和私有策略信号。但真实的市场洞察恰恰需要多源融合——arXiv 2025 年的研究显示，仅 19 个搜索者就从 720 万次 CEX-DEX 套利中提取了 2.338 亿美元价值，普通用户完全无法实时捕捉这些机会，因为 Dune 上根本没有 CEX 数据。

Chainlink 的产品矩阵（Data Feeds、Data Streams、Functions、Proof of Reserve）构成了一个独立于 DEX 和 CEX 之外的"第三类数据源"——去中心化验证数据流。它的 Data Streams 提供亚秒级签名报告，比 CEX WebSocket 多了密码学验证、防篡改、防抢跑和内置市场状态指示器。这种数据无法被简单地归入"链上"或"链下"，它需要一个新的架构位置。

### 问题二：冷热存储割裂——实时与历史无法统一查询

Dune 的现状正是经典 Lambda 架构的痛点：DuneSQL（Trino）作为批处理层查询 S3，延迟分钟级；Sim API 作为实时层读取节点数据，延迟亚秒级。但两层完全割裂——独立账户、独立定价、无法跨 Join。用户需要写两套代码手动拼接结果。

这意味着 Lambda 架构的"双倍存储双倍计算"问题在 Web3 领域同样存在：同一条链上交易数据既存在于 Kafka（保留 7 天热数据），又存在于 S3 数据湖（全量历史），存储成本翻倍，计算逻辑需要写两遍，数据对齐困难导致策略回测与实盘偏差率可达 20-30%。

### 问题三：复杂计算缺失——批处理引擎无法做实时流式计算

DuneSQL 基于 Trino，其架构设计哲学是 MPP 批处理引擎，五个根本限制使其无法支持流式计算：拉取式执行模型（无持续运行）、无状态算子（无跨查询状态）、无事件时间语义（无法处理乱序事件）、无持续数据摄入管道、无增量计算引擎。

这意味着 Flink 能做而 Dune 做不到的关键场景：

- 实时滑动窗口 VWAP：每秒更新的 5 分钟 VWAP，Dune 只能写固定时间范围的批查询
- CEP 预言机操纵检测："30 秒内 3 次价格更新 + 大额借贷"的操纵模式，Dune 没有任何模式匹配语法
- 多流 Interval Join：链上大额转账后 5 分钟内 CEX 充值关联，Dune 甚至没有 CEX 数据
- 实时状态管理：DeFi 健康因子追踪需要 TB 级状态，Dune 每次查询都从全量历史重新计算
- 会话窗口：同一地址的交易行为聚类，DuneSQL 不支持 Session Window

2024-2025 年预言机操纵攻击造成 DeFi 损失超过 4 亿美元，Aave 单日清算量可达 2 亿美元——如果有实时健康因子监控和 CEP 预警，大量损失可以被预防。

---

## 我的解决方案

**FlinkCDC → Fluss → Flink → Paimon 湖流一体架构**，一套从实时到历史的全链路解决方案。

### 核心架构

```
数据源层：链上事件 · CEX WebSocket · Chainlink 预言机 · 私有数据库
         ↓ FlinkCDC / 自定义 Source
Fluss 统一流数据总线（替代 Kafka）
  fluss://onchain/   链上 DEX Swap、NFT 事件、治理投票
  fluss://cex/       Binance/OKX 深度流、交易流
  fluss://oracle/    Chainlink Feeds、Streams、PoR
  fluss://private/   交易信号、地址标签、组合快照
         ↓ Tiering Service（自动沉降，30 秒间隔）
         ↓ Flink 实时计算层
  事件解码 · 窗口计算 · CEP 模式检测 · Delta Join 多源关联
         ↓
Fluss 热数据（6h，毫秒级）+ Paimon 冷数据（全量，秒级）
         ↓ Union Read（统一查询，Exactly-Once）
用户/API/仪表板：一个 SQL 获取实时 + 历史全量数据
```

### 三大核心突破

**突破一：多源统一集成。** Fluss 的四命名空间架构将链上、CEX、预言机、私有数据统一汇入同一总线。Flink Delta Join 实现毫秒级跨源关联——DEX Swap 价格 vs CEX WebSocket 报价 vs Chainlink Data Streams 签名报告，三源交叉验证，偏差超过 1% 实时告警。这消除了"数据孤岛"。

**突破二：冷热存储自动查询。** Fluss 仅保留 6 小时热数据（vs Kafka 的 7 天），Tiering Service 自动沉降到 Paimon，Union Read 自动合并两层。用户写 `SELECT * FROM dex_swaps`，引擎自动判断数据位置并合并，Exactly-Once 语义保证不多不少。存储成本降低 50-70%，查询延迟从分钟级降至秒级（10-30 倍提升）。这消除了 Lambda 双倍冗余。

**突破三：复杂指标实时计算。** Flink 的窗口计算、CEP、状态管理、Delta Join 四大能力，覆盖了 DuneSQL 根本无法实现的场景。预言机操纵从"事后分析"变为"5 秒预警"，DeFi 清算从"事后清算"变为"30 秒-5 分钟提前预警"。这消除了"事后分析"局限。

---

## 市场验证：竞品无法做到

经过对英语区和中文区市场的广泛调研，**目前没有任何平台在底层架构层面完整融合 DEX + CEX + 私有数据并在此基础上提供场景化交易能力。**

| 平台 | DEX 数据 | CEX 数据 | 私有数据 | 交易执行 | 核心架构 |
|------|---------|---------|---------|---------|---------|
| Amberdata | 100+ 链 | 市场数据 | 部分 | 否 | Redpanda → Databricks → Pinot |
| Nansen | 多链链上 | 仅市场展示 | 否 | DEX Swap | 自建索引 + 实时流 |
| Arkham | 链上 + 3 亿标签 | 市场数据 | 否 | Arkham Exchange | 独立管道，应用层拼接 |
| Solidus HALO | DEX 交易 | CEX 交易 | 合规规则 | 否（合规监控） | 融合管道 |
| FMZ Quant | 链上 RPC | 多 CEX API | 策略数据库 | 是 | 策略层融合，无统一管道 |

**Dune/Nansen 不做交易的核心原因：** 商业选择占约 60%（监管风险、市场定位、利润模式），技术限制约占 40%。DuneSQL 是 Trino 分支，从根本上不支持实时流处理；Sim 是独立系统，两者无法跨 Join。Nansen CEO 公开承认监管问题是阻碍交易功能的关键因素。

**Goldsky 和 Amberdata 的验证：** Goldsky 使用 Redpanda + Flink + ClickHouse 架构，云基础设施成本节约 3-4 倍；Amberdata 迁移到 StarTree Cloud 后，基础设施成本降低 66%。这些生产级案例证明了流式架构在 Web3 场景的可行性和经济效益。

---

## 五个高价值场景

### 场景一：CEX-DEX 实时套利信号

Binance WebSocket depth stream 和链上 Swap 事件同时汇入 Fluss，Flink Interval Join 毫秒级检测价差。当前 19 个搜索者垄断 2.338 亿美元套利利润，Fluss 架构让普通用户也能捕捉部分机会。

### 场景二：DeFi 清算实时预警

Flink 为每个 Aave 用户维护实时健康因子（Keyed State + RocksDB），健康因子低于 1.5 时触发预警。Aave 在 2025 年 2 月单日清算量达 2 亿美元，实时监控可帮助用户避免 5% 以上的清算损失，年化可避免损失在数千万美元级别。

### 场景三：预言机操纵实时检测

Flink CEP 检测"30 秒内 3 次价格更新（单次 >5%）+ 随后 1 分钟内大额借贷"的操纵模式，5 秒内发出预警。2024-2025 年预言机攻击损失超 4 亿美元，2025 年 5 月 Chainlink 误报导致 50 万美元清算——实时检测可阻止 80% 的错误清算。

### 场景四：DAO 治理攻击实时检测

Flink 双流关联（ERC-20 Transfer + VoteCast），检测闪电贷获取投票权重的攻击模式。Beanstalk 攻击中，攻击者通过闪电贷获取约 46% 治理权重并盗取 1.82 亿美元。Fluss 统一总线可同时监控多个 Governor 合约，检测跨 DAO 的连锁攻击。

### 场景五：链上 + 链下混合架构数据总线

Vitalik 将"混合应用"列为以太坊五大激动人心的应用之一，核心模式为"链下计算 + 链上承诺 + ZK 证明"。Fluss 的流表二象性（Log Table 记录完整变更流 + PK Table 提供实时状态快照）天然支撑这一范式——链下计算的中间结果通过 Fluss 流式存储，Flink 做实时校验，Paimon 做历史归档，形成"链下计算 → Fluss → Flink → Paimon → 链上预言机"的完整闭环。

---

## 我的差异化优势

### 技术架构优势

| 维度 | Dune + Sim | Fluss 架构 | 量化提升 |
|------|-----------|-----------|---------|
| 链上数据查询 | Trino 分钟级延迟 | Fluss + Paimon Union Read 秒级 | 10-30x |
| 实时 + 历史关联 | 两套系统手动拼接 | 一条 SQL 自动合并 | 开发效率 5x |
| 预言机操纵检测 | 事后 SQL 分析 | Flink CEP 实时 5s 预警 | 从"事后"到"实时" |
| CEX-DEX 价差监控 | 不可能 | Fluss 统一总线 + Interval Join | 全新能力 |
| 冷热数据查询 | Lambda 双倍存储 | 自动分层，Union Read | 存储降 50-70% |
| 复杂指标 | 不支持 CEP/窗口/状态 | Flink 原生 CEP/窗口/状态 | 全新能力 |

### 个人优势

- **5 年国网大数据实战**：在地球上最复杂的数据环境中积累了 TB 级实时数仓设计经验，从 FlinkCDC 采集到 Paimon 湖仓，全链路亲手落地
- **Fluss 深度研究**：国内最早系统性研究 Fluss 并将其映射到 Web3 的工程师之一，已完成 6 份深度研究报告，覆盖竞品分析、架构设计、场景探索、经济效益量化
- **Vibecoding 能力**：能通过 AI 辅助在极短时间内将技术想法转化为可交互的 Demo，快速验证产品假设
- **跨界融合**：同时理解传统大数据架构和 Web3 数据特性，能将国网级别的数仓设计经验迁移到 Web3 场景

---

## Vitalik 愿景的架构契合

Vitalik 关于以太坊未来的核心论点是"优质应用必为链上 + 链下混合架构"。Fluss 作为统一流数据总线的定位与此高度契合：

- **ENS 域名实时分析**：Flink 窗口计算到期预警 + 双流 Join 市场分析，ENS 抵押借贷清算风控
- **ERC-4337 账户抽象监控**：Flink CEP 检测 Paymaster Sybil 攻击，Bundler 行为追踪和信誉评分
- **ZK 证明验证追踪**：Fluss 统一多 L2 证明事件总线，Flink 实时比较各 L2 证明延迟和聚合效率
- **混合预言机架构**：链上 TWAP 与 Chainlink 价格实时一致性校验，偏差超阈值自动回退
- **DAO 治理安全**：闪电贷 + 投票双流关联，跨 DAO 协议连锁攻击检测

---

## 我在寻找什么

我坚信 Web3 数据基础设施正在经历从"事后分析"到"实时智能"的范式转移。Fluss 湖流一体架构不是对 Dune/Nansen 的全面替换，而是增量叠加——在现有数据层之上添加实时计算和统一查询能力。

我正在寻找志同道合的团队，无论是：

- **Web3 数据平台** 需要 Fluss/Flink/Paimon 架构升级
- **DeFi 协议** 需要实时风控和清算预警
- **交易基础设施** 需要多源数据融合和实时信号
- **创业团队** 需要 CTO/技术合伙人级别的架构设计能力

我带来的不仅是技术方案，更是从 0 到 1 的落地经验和从 1 到 100 的架构视野。

**Let's build the future of Web3 data infrastructure, together.**

---

## 核心参考文献

1. [Amberdata Architecture: Our Technical Foundations](https://engineering.amberdata.io/amberdata-architecture-our-technical-foundations)
2. [Goldsky - A Gold Standard Architecture with ClickHouse and Redpanda](https://clickhouse.com/blog/clickhouse-redpanda-architecture-with-goldsky)
3. [Streaming CEX, DEX, and Blockchain Events in Databricks for Web3](https://community.databricks.com/t5/technical-blog/streaming-cex-dex-and-blockchain-events-in-databricks-for-web3/ba-p/120503)
4. [Measuring CEX-DEX Extracted Value and Searcher Profitability | arXiv](https://arxiv.org/abs/2507.13023)
5. [Chainlink Data Streams Architecture](https://docs.chain.link/data-streams/architecture)
6. [Shipping an L1 zkEVM #1: Realtime Proving | Ethereum Foundation](https://blog.ethereum.org/2025/07/10/realtime-proving)
7. [Fluss: Unified Streaming Storage For Next-Generation Data Analytics](https://www.ververica.com/blog/introducing-fluss)
8. [湖流一体：基于 Fluss + Paimon 的实时湖仓数据底座 | 阿里云](https://developer.aliyun.com/article/1709004)
9. [Oracle Manipulation in Smart Contracts: How to Prevent | Nomos Labs](https://nomoslabs.io/blog/oracle-manipulation-smart-contracts-prevent)
10. [Vitalik: Five Exciting Applications of the Ethereum Application Ecosystem](https://www.chaincatcher.com/en/article/2084088)

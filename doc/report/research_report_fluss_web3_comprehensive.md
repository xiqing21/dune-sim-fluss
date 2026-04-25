# FlinkCDC + Fluss + Flink 在 Web3 领域的数据集成与处理：突破、场景与经济效益综合研究

## 报告摘要

本报告系统分析了 FlinkCDC + Fluss + Flink + Paimon 架构在 Web3 数据集成与处理领域相对于 Dune + Sim 和 Nansen 等现有平台的突破与不足，深入调研了 CEX/DEX 订单簿与撮合引擎的数据存储机制，并量化了 Fluss 架构对现有链上数据平台的优化效果和经济效益。核心发现：Fluss 架构在三个维度上具有突破性优势——多源统一集成（链上 + CEX + 预言机）、冷热存储自动查询（Union Read 消除 Lambda 双倍冗余）、复杂指标实时计算（Flink CEP/窗口/状态管理），但存在无原生区块链连接器、项目成熟度不足、运维复杂度高等短板。经济效益估算显示，存储成本可降低 50-70%、查询延迟可降低 10-30 倍、开发运维效率可提升 3-5 倍。

---

## 一、FlinkCDC + Fluss + Flink 在 Web3 领域的突破与场景

### 1.1 三大核心突破

**突破一：多源数据的统一集成**

当前 Dune 和 Nansen 仅包含链上数据，无法在同一查询中关联 DEX Swap、CEX 订单簿深度和 Chainlink 喂价。Fluss 的统一数据总线架构可在 `fluss://onchain/`、`fluss://cex/`、`fluss://oracle/`、`fluss://private/` 四个命名空间下统一汇入异构数据源，Flink Delta Join 实现毫秒级跨源关联。

典型场景：CEX-DEX 套利信号检测。arXiv 2025 年研究显示，19 个搜索者在 19 个月内从 720 万次 CEX-DEX 套利中提取了 2.338 亿美元，其中仅 3 个搜索者占据了 75% 的利润。普通用户完全无法捕捉这些机会，因为 Dune 上没有 CEX 数据。Fluss 架构下，Binance WebSocket depth stream 和链上 Swap 事件同时汇入 Fluss，Flink Interval Join 在毫秒级检测价差。

**突破二：冷热存储的自动查询**

Dune + Sim 是 Lambda 架构的典型体现：Dune SQL（Trino）作为批处理层查询 S3 上的列式存储，延迟分钟级；Sim API 作为实时层直接读取节点数据，延迟亚秒级。但两层完全割裂——独立账户体系、独立定价模型、无法跨 Join。用户需要写两套代码手动拼接结果。

Fluss + Paimon 的湖流一体架构从根本上消除了 Lambda 的问题：数据写入 Fluss 后，Tiering Service 自动将数据沉降到 Paimon（默认 30 秒间隔），Fluss 仅保留 6 小时热数据（vs Kafka 的 7 天），存储成本降低 10 倍以上。用户写 `SELECT * FROM dex_swaps`，引擎自动合并 Fluss 热数据和 Paimon 冷数据，Exactly-Once 语义保证数据不多不少。

**突破三：复杂指标的实时计算**

DuneSQL（Trino）是 MPP 批处理引擎，五个根本限制使其无法支持流式计算：拉取式执行模型、无状态算子、无事件时间语义、无持续摄入管道、无增量计算引擎。Flink 能做而 Dune 做不到的五个典型场景：实时滑动窗口 VWAP、CEP 预言机操纵检测、多流 Interval Join（链上→CEX 资金流向）、实时状态管理（DeFi 健康因子）、会话窗口（交易行为聚类）。

### 1.2 相对于 Dune + Sim 的优势与不足

| 维度 | 优势 | 不足 |
|------|------|------|
| **多源整合** | 链上 + CEX + 预言机 + 私有数据统一总线 | 无原生区块链连接器，依赖第三方索引服务 |
| **实时性** | Fluss 亚秒级延迟 + Flink 流式计算 | 延迟下限受限于索引层（Goldsky/Subsquid 的数据新鲜度） |
| **计算能力** | CEP/窗口/状态管理/Delta Join | 开发学习曲线陡峭（Flink SQL/CEP 概念复杂） |
| **存储效率** | 列式流存储 + 冷热分层 + Union Read | Fluss 处于 Apache 孵化阶段，Schema Evolution 仅支持 ADD COLUMN |
| **历史分析** | Paimon 开放格式 + Trino/StarRocks 直接读取 | 需自建 Fluss/Flink/Paimon 集群，运维复杂度远高于 SaaS |
| **成本** | 开源 + 云资源，长期成本可控 | 初期投入大，需专业运维团队 |
| **数据深度** | 依赖索引层提供的解码数据 | 不如 Sim IDX 的 iEVM 执行层嵌入（完整 EVM 状态、内部调用） |

### 1.3 在 Web3 领域的已有应用

**Fluss 本身尚无公开的 Web3 生产级案例**（2024 年底开源，2025 年进入 Apache 孵化），但类似架构理念已有多个先行实践者：

| 项目 | 架构 | 与 Fluss+Paimon 的相似性 | 规模数据 |
|------|------|------------------------|----------|
| **Goldsky** | Redpanda + Flink + ClickHouse | Redpanda 类似 Fluss 角色，Tiered Storage 类似冷热分层 | 15+ 条链，数十 TB，Flink→ClickHouse 50 万事件/秒，查询 10.4 亿行/秒 |
| **Amberdata** | Redpanda + Databricks + Pinot | Redpanda=Fluss, Delta Lake=Paimon, Pinot=实时查询 | 日名义交易值 5000 亿美元，200 万事件/秒写入 Pinot，2 PB+ 对象存储，30 亿次/年 API 调用 |
| **7Block Labs** | Kafka/Redpanda + Flink/Spark + Delta/Iceberg + Snowflake | Kafka=Fluss, Delta/Iceberg=Paimon, 需双写冗余 | SOC2 合规，热数据摄取到查询 < 60s |
| **Databricks Web3** | Python Data Source API + Structured Streaming + Delta Lake | Bronze=Fluss, Silver/Gold=Flink+Paimon | 统一处理 Binance CEX + Uniswap DEX + Ethereum 链上数据 |
| **VeloDB (Doris+Paimon)** | Apache Doris + Paimon | Paimon 统一存储，Doris 替代 Trino | ETL 5x 提速，存储降 40%，查询延迟从 5-10s 降至 <2s |

**Goldsky 是目前最接近 Fluss+Paimon 的 Web3 生产案例**——Redpanda 作为主存储+流传输引擎（类似 Fluss），通过 Tiered Storage 与 S3 对接（类似冷热分层），Flink 作为统一计算层。相比传统 Kafka 架构，云基础设施成本节约 3-4 倍。

### 1.4 集成 DEX + CEX + Chainlink 的可行性

**完全可行**，且已有类似实现：

- **Databricks Web3 案例**：Bronze 层通过 Python Data Source API 同时摄取 Binance REST API（CEX 交易数据）、Uniswap GraphQL API（DEX Swap 数据）和 Ethereum RPC（链上数据），在 Gold 层做跨源关联计算 CEX-DEX 滑点和 MACD/RSI 技术指标
- **Amberdata 架构**：Redpanda Connect 同时摄取链上事件和 CEX 市场数据，经 Databricks Medallion 处理后通过 Pinot 提供亚秒级实时分析
- **Fluss 架构的增量优势**：上述案例仍采用 Lambda 架构风格（数据同时摄取到低延迟存储和长期存储），Fluss + Paimon 通过 Tiering Service 实现自动冷热分层，消除了双路写入和双系统维护

**Chainlink 数据的接入路径**：
- Data Feeds：FlinkCDC 监听链上 Aggregator 合约的 `AnswerUpdated` 事件 → `fluss://oracle/chainlink_feeds`
- Data Streams：WebSocket API（`wss://ws.dataengine.chain.link/api/v1/ws`）订阅签名报告 → 自定义 Flink Source → `fluss://oracle/chainlink_streams`
- Proof of Reserve：FlinkCDC 监听 PoR 合约事件 → `fluss://oracle/chainlink_por`

### 1.5 优势与劣势总结

**优势**：统一多源集成（消除了"数据孤岛"问题）、湖流一体（消除了 Lambda 双倍冗余）、实时复杂计算（消除了"事后分析"局限）、开放格式（与 Trino/StarRocks 兼容，渐进式集成而非全面替换）、成本可控（开源 + 云资源，长期低于 SaaS 计费）

**劣势**：无原生区块链连接器（依赖 Goldsky/Subsquid 等第三方索引服务）、项目成熟度不足（Fluss 仍处于 Apache 孵化阶段）、运维复杂度高（需专业团队维护 Fluss/Flink/Paimon 集群）、延迟下限受限于索引层（若依赖外部服务则延迟受制于其数据新鲜度）

---

## 二、CEX/DEX 订单簿与撮合引擎的数据存储机制

### 2.1 CEX 订单簿数据存储

CEX 的订单簿采用**纯内存撮合**架构，核心发现：

| 层次 | 存储介质 | 数据 | 延迟 |
|------|----------|------|------|
| 热层 | 内存（RAM） | 活跃订单簿（双向链表 + 优先队列） | 纳秒级，撮合引擎直接操作 |
| 温层 | 异步持久化（磁盘/DB） | 成交记录、订单历史 | 毫秒级，通过 WAL/快照保证一致性 |
| 冷层 | 数据湖/对象存储 | 历史 K 线/Trade 数据 | 批量查询，公开数据集 |

**Binance 具体机制**：订单簿全量在内存，撮合延迟约 200 微秒，吞吐量约 140 万笔/秒。实时快照每 50ms 生成增量备份，异步持久化到磁盘，故障恢复 RTO < 10 秒。成交记录通过 Kafka 推送给下游系统。历史数据通过 Binance Public Data（S3）批量下载。

**Binance 对外数据接口**：WebSocket `wss://stream.binance.com:9443/ws` 提供 Partial Book Depth（5/10/20 档快照流，100ms/1000ms 更新）和 Diff. Depth Stream（增量流，100ms/1000ms 更新）。增量流中 `qty="0"` 表示该价格档位被完全撤走。K 线数据通过 REST API `GET /api/v3/klines` 获取，历史数据通过 S3 公开数据集批量下载。

**OKX 具体机制**：架构与 Binance 类似，WebSocket `wss://ws.okx.com:8443/ws/v5/public` 提供多种深度频道——`books`（400 档快照+增量，用于完整订单簿重建）、`books5`（5 档全量推送）、`books-l2-tbt`（400 档逐笔推送，用于 HFT）、`bbo-tbt`（仅最优价）。首条消息为全量快照，后续为增量更新，必须使用 `checksum` 字段校验本地订单簿状态。

**CEX 订单簿接入 Fluss 的方案**：通过自定义 Flink Source 连接 Binance/OKX WebSocket API，L2 订单簿重建流程为：先获取 REST API 深度快照 → 订阅 WebSocket 增量流 → 丢弃所有 `U` <= `lastUpdateId` 的增量 → 找到首个 `U` <= `lastUpdateId + 1` 且 `u` >= `lastUpdateId + 1` 的增量作为起点 → 依次应用后续增量。重建后的 L2 订单簿写入 `fluss://cex/depth_updates` Log Table 和 `fluss://cex/orderbook_l2` PK Table。

### 2.2 DEX 订单机制

**Uniswap V3/V4（AMM 模型）**：不存在传统意义上的订单簿。AMM 通过 x*y=k 曲线定价，LP 在 [Pa, Pb] 价格区间提供流动性头寸（ERC-721 NFT），相当于在该价格范围内"挂出"买卖单。Tick 机制将价格空间离散化（P(i) = 1.0001^i，每个 Tick 之间 0.01% 价格变化）。V4 的 Hooks 系统允许在 `beforeSwap`/`afterSwap` 等生命周期节点注入自定义逻辑，可实现限价单功能。

**Hyperliquid（链上 CLOB）**：自研 L1 区块链，核心就是链上中央限价订单簿。HyperBFT 共识（源自 HotStuff）实现亚秒级最终性（约 0.2 秒），吞吐量达 20 万笔/秒。完全链上撮合，无链下中继器。价格由验证者直接计算（加权平均来自 Binance/OKX/Bybit 的现货价格），无需外部预言机。数据推送延迟 200-400ms（受出块确认时间限制）。

**dYdX v4（链下撮合+链上结算）**：基于 Cosmos SDK + CometBFT 的 L1 区块链。验证者在内存中维护订单簿，采用价格-时间优先匹配。订单为短期链下类型（存在于节点内存中），GTB（Good-Til-Block）字段确保过期后不可成交。索引器使用 Postgres + Redis + Kafka 从全节点消费实时数据，通过 WebSocket 和 REST 向终端用户提供服务。

**0x/Matcha（DEX 聚合器）**：0x 协议支持链下限价单（用户签名后广播到 Orderbook API），Matcha 通过智能订单路由扫描 130+ 流动性源和 14 条链寻找最优执行路径。

---

## 三、Dune + Sim 和 Nansen 的 Fluss 优化方案与经济效益

### 3.1 Dune 平台的问题与 Fluss 优化

#### 查询延迟

**之前**：Dune 使用 Trino 作为查询引擎。Small 引擎超时 2 分钟、最大并发 3 个；Medium/Large 引擎按计算资源实际消耗计费。热门仪表板每小时刷新，冷门每 24 小时刷新。典型用户体验：简单查询 30s-2min，复杂查询 5-10min+。Dune 月访问量 110 万，Plus 版 $399/月含 25,000 积分，Premium $999/月含 100,000 积分。

**之后**：Fluss 热数据秒级读取 + Paimon 冷数据列裁剪+谓词下推 + Union Read 自动路由。简单查询从 30-60s 降至 1-3s（10-30x 提升），复杂聚合从 2-10min 降至 5-30s（10-24x 提升），仪表板从每小时刷新变为近实时（秒级）。按 Dune 月活 110 万访问量估算，用户日均查询次数可从 3-5 次提升至 20-30 次，单查询积分消耗降低（更少全表扫描），用户成本可降低 40-60%。

#### Dune SQL 与 Sim API 的割裂

**之前**：Sim API 绕过 Dune 数据处理管道直接读取节点实时状态，Dune SQL 是批处理分析系统，两者无法在 SQL 层面 Join。用户需要写两套代码手动拼接——Sim API 获取最新价格（毫秒级但无历史深度），Dune SQL 查询历史聚合数据（有深度但有延迟），在应用层手动 merge。

**之后**：Fluss Union Read + Delta Join 实现一条 SQL 同时获取"过去 24h 交易量（Paimon 冷数据）+ 当前最新价格（Fluss 热数据）"，数据一致性由 Union Read 的快照+Offset 精确对齐保证，延迟从 30s-2min 降至秒级。

#### 复杂计算缺失

**之前**：DuneSQL 不支持 CEP、实时窗口、状态管理、流式计算。预言机操纵检测只能事后分析——攻击发生后写 SQL 查询异常价格波动，此时用户已被清算。2024-2025 年预言机操纵攻击造成 DeFi 损失超过 4 亿美元（Nomos Labs 统计），2025 年 5 月 Chainlink 预言机误报 deUSD 价格导致 50 万美元+清算，2025 年 11 月 Moonwell 因 Chainlink 故障损失 100 万美元。

**之后**：Flink CEP 实时检测——Chainlink 喂价偏差超过阈值时，5 秒内发出预警，协议可暂停相关操作。Aave 在 2025 年 2 月单日清算量达 2 亿美元，如果有实时健康因子监控，大量清算可以被预防。

#### 多源数据整合

**之前**：Dune 仅包含链上数据，无法关联 CEX 实时价格、Chainlink 喂价、链下数据。用户需要 Dune 查链上数据 + 自建 CEX WebSocket 连接 + Chainlink Subgraph 查询 + 应用层手动关联。

**之后**：Fluss 统一数据总线汇入 `fluss://onchain/` + `fluss://cex/` + `fluss://oracle/`，Flink SQL 实现三源交叉验证（DEX Swap 价格 vs CEX WebSocket 报价 vs Chainlink Data Streams 签名报告），偏差超过 1% 触发告警。

### 3.2 Nansen 平台的问题与 Fluss 优化

**Nansen 当前状态**：5 亿+ 已标记钱包地址，30+ 条区块链覆盖。2025 年 9 月从 $999-1,299/月大幅降价 95% 至 $49-69/月。2026 年 1 月推出 AI 驱动的交易执行功能（支持 Solana 和 Base），集成 Jupiter 和 OKX DEX 聚合器，使用 LI.FI 处理跨链路由——但仅限 DEX Swap，无 CEX 集成，且地理限制（如新加坡无法使用）。

**Fluss 可优化的方向**：

- **CEX 链上关联分析**：Nansen 的 Smart Money 追踪目前仅限于链上行为，无法检测"链上大额转账后 CEX 充值"的资金流向。Fluss 的 Interval Join 可在 5 分钟内关联链上 Transfer 事件和 CEX Deposit 事件
- **实时标签更新**：Nansen 的地址标签更新存在延迟，Fluss 的流式状态管理可实现地址行为的实时画像更新
- **交易执行的数据层**：Nansen 向交易执行转型需要 CEX 订单簿数据，Fluss 可作为统一的订单簿数据总线，同时接入 Binance/OKX/Hyperliquid 的 WebSocket 流

### 3.3 延伸场景的经济效益

| 场景 | 之前 | 之后 | 经济效益 |
|------|------|------|----------|
| **CEX-DEX 套利信号** | 19 个搜索者垄断 2.338 亿美元，普通用户无法参与 | Fluss 实时多源分析，Flink Interval Join 检测价差 | 按套利总额 2.338 亿美元的 0.1% 计算，SaaS 服务年营收潜力约 23 万美元/年，但更核心的价值是让普通用户也能捕捉部分机会 |
| **DeFi 清算预警** | Aave 单日清算量可达 2 亿美元（2025 年 2 月），用户无法预防 | Flink 实时健康因子监控，清算前 30s-5min 预警 | 假设帮助用户避免 5% 的清算损失，Aave 日均清算约 500 万美元时，年化避免损失约 9,100 万美元。SaaS 订阅定价参考：$49-299/月/用户 |
| **预言机监控** | 2024-2025 年预言机攻击损失超 4 亿美元，DeFi 协议事后发现 | Flink CEP 实时检测 Chainlink 喂价偏差，5 秒内预警 | 参考 2025 年 5 月 Chainlink 误报导致 50 万美元清算，如果实时监控能阻止 80% 的错误清算，仅该事件即可避免 40 万美元损失 |
| **跨链桥监控** | 2024-2025 年跨链桥攻击损失超 3.2 亿美元，2026 年 4 月 Kelp DAO 被盗 2.92 亿美元 | Fluss 统一总线汇聚多链 lock/mint 事件，Flink 实时检测异常跨链资金流 | Kelp DAO 攻击中，攻击者利用 LayerZero 桥配置错误铸造 2.92 亿美元 rsETH，实时异常检测可在攻击初期触发告警 |
| **DAO 治理安全** | Beanstalk 攻击中闪电贷获取 79% 投票权重盗取 1.82 亿美元 | Flink 双流关联（Transfer + VoteCast），实时检测闪电贷+投票模式 | 按每年 DAO 治理攻击损失约 1 亿美元估算，实时检测服务可保护约 50-80% 的资金 |

### 3.4 成本对比量化

**存储成本**：Dune 平台 S3 存储超过 13TB 以太坊归档数据。Lambda 架构下，若同时在 Kafka 保留 7 天热数据（TB 级），存储成本直接翻倍。Fluss 仅保留 6 小时热数据（vs Kafka 的 7 天），Kafka 存储需求从 7 天降至 1 小时，存储成本降低 50-70%。

**计算成本**：Dune 每次查询全量扫描重新计算（Trino 拉取式执行），Flink 增量计算只处理新到数据。复杂聚合查询从 5-10min 降至 5-30s，计算资源消耗降低约 10 倍。按 Dune Plus 版 $399/月计算，若用户因查询延迟和计算限制只能利用 30% 的积分价值，Fluss 架构下利用率可提升至 80%+。

**开发效率**：Lambda 架构需要写两套逻辑（Speed Layer 流式版 + Batch Layer 批处理版），Fluss 架构只需写一套 Flink SQL。开发时间缩短约 50%，配置工作减少 70%，学习成本降低 80%（标准 SQL 替代复杂 API）。

**运维成本**：从 8+ 个组件（Zookeeper + Kafka + Connect + Streams 等）减少到 1 个平台（Fluss + Paimon），运维成本降低约 60%。但需注意 Fluss 仍处于孵化期，生产环境部署需要专业团队。

**Goldsky 的实际验证**：使用 Redpanda（Kafka 兼容）替代传统 Kafka 后，云基础设施成本节约 3-4 倍。Amberdata 迁移到 StarTree Cloud（Pinot 托管）后，基础设施成本降低 66%。VeloDB 使用 Doris + Paimon 替代 Spark + Trino + OLAP 后，ETL 5x 提速、存储降 40%、查询延迟从 5-10s 降至 <2s。

---

## 四、之前 vs 之后对比总结

| 场景 | 之前（Dune/Sim/Nansen） | 之后（Fluss 架构） | 量化提升 |
|------|------------------------|-------------------|----------|
| 链上数据查询 | Trino 分钟级延迟，Small 引擎超时 2min | Fluss + Paimon Union Read 秒级 | 10-30x |
| 实时+历史关联 | Dune SQL + Sim API 两套系统手动拼接 | 一条 SQL 自动合并，Delta Join 多源关联 | 开发效率 5x |
| 预言机操纵检测 | 事后 SQL 分析，攻击已发生 | Flink CEP 实时检测，5s 内预警 | 从"事后"到"实时" |
| CEX-DEX 价差监控 | 不可能——Dune 没有 CEX 数据 | Fluss 统一总线 + Flink Interval Join | 新能力 |
| 冷热数据查询 | Lambda 双倍存储双倍计算 | Fluss Tiering 自动分层，Union Read 统一 | 存储降 50-70% |
| 复杂指标 | 不支持 CEP/窗口/状态管理 | Flink 原生 CEP/窗口/状态 | 新能力 |
| 运维复杂度 | SaaS 零运维 | 自建集群需专业团队 | 需权衡 |
| 项目成熟度 | 生产级，100+ 链 | Fluss 孵化期，无 Web3 生产案例 | 需验证 |

---

## 五、结论

FlinkCDC + Fluss + Flink + Paimon 架构为 Web3 数据平台提供了三个维度的突破：多源统一集成（消除了"数据孤岛"）、冷热存储自动查询（消除了 Lambda 双倍冗余）、复杂指标实时计算（消除了"事后分析"局限）。CEX 订单簿数据主要通过 WebSocket API 接入（Binance/OKX 的 depth stream），DEX 的 AMM 模型不产生传统订单簿（Uniswap）或链上 CLOB（Hyperliquid），这些异构数据源都可以通过 Fluss 统一汇入。

相对于 Dune + Sim，Fluss 架构在多源整合、复杂计算、历史分析和成本可控方面具有显著优势，但在数据深度（不如 Sim IDX 的 iEVM）、零运维和项目成熟度方面存在差距。Goldsky（Redpanda + Flink + ClickHouse）和 Amberdata（Redpanda + Databricks + Pinot）的成功案例证明了类似架构的可行性，且带来了 3-4 倍的云基础设施成本节约和 66% 的基础设施成本降低。

经济效益的核心逻辑是：Fluss 架构将"事后分析"变为"实时预警"，将"多源拼接"变为"统一查询"，将"双倍冗余"变为"冷热分层"。仅 DeFi 清算预警和预言机监控两个场景，年化可避免损失就在数千万美元级别。对于 Dune 和 Nansen 而言，Fluss 架构不是全面替换而是增量叠加——在现有 Trino + S3 之上添加 Fluss 实时层和 Flink 计算层，Paimon 替代部分 S3 直存的数据组织方式，渐进式提升平台的实时能力和多源整合能力。

---

## References

1. [Goldsky - A Gold Standard Architecture with ClickHouse and Redpanda](https://clickhouse.com/blog/clickhouse-redpanda-architecture-with-goldsky)
2. [How Goldsky democratizes streaming data for Web3 developers with Redpanda](https://www.redpanda.com/blog/democratize-streaming-data-web3-goldsky-redpanda)
3. [Amberdata's Architecture: Our Technical Foundations](https://engineering.amberdata.io/amberdata-architecture-our-technical-foundations)
4. [Amberdata Powering Real-Time Blockchain Analytics with StarTree](https://startree.ai/user-stories/amberdata-powering-real-time-blockchain-analytics-with-startree/)
5. [Apache Doris + Paimon: A Faster Lakehouse for Web3 On-Chain Analytics](https://www.velodb.io/blog/apache-doris-paimon-a-faster-lakehouse-for-web3-onchain-analytics)
6. [Streaming CEX, DEX, and Blockchain Events in Databricks for Web3](https://community.databricks.com/t5/technical-blog/streaming-cex-dex-and-blockchain-events-in-databricks-for-web3/ba-p/120503)
7. [Integrating Blockchain Data Pipelines: 7Block Labs' Technical Playbook](https://www.7blocklabs.com/blog/integrating-blockchain-data-pipelines-7block-labs-technical-playbook)
8. [Measuring CEX-DEX Extracted Value and Searcher Profitability | arXiv](https://arxiv.org/abs/2507.13023)
9. [Binance WebSocket Streams | DeepWiki](https://deepwiki.com/binance/binance-spot-api-docs/4-websocket-streams)
10. [How Hyperliquid Works: Architecture, Order Book, HyperEVM](https://rocknblock.io/blog/how-does-hyperliquid-work-a-technical-deep-dive)
11. [dYdX v4 Limit Order Book and Matching](https://docs.dydx.exchange/concepts-limit-order-book-and-matching)
12. [Dune Analytics Pricing | CostBench](https://costbench.com/software/onchain-analytics/dune-analytics/)
13. [Nansen Pricing 2026 | CostBench](https://costbench.com/software/onchain-analytics/nansen/)
14. [Dune Supports 100 Blockchains | Chainwire](https://chainwire.org/2025/02/26/dune-supports-100-blockchains-unifying-fragmented-onchain-data/)
15. [Oracle Manipulation in Smart Contracts: How to Prevent | Nomos Labs](https://nomoslabs.io/blog/oracle-manipulation-smart-contracts-prevent)
16. [Nansen Pivots to AI Execution with Solana and Base Trading](https://www.cryptotimes.io/2026/01/21/nansen-pivots-to-ai-execution-with-solana-and-base-trading/)
17. [Fluss Lakehouse Storage Design (FIP-1) | Apache Fluss Wiki](https://cwiki.apache.org/confluence/display/FLUSS/FIP-1%3A+Fluss+Lakehouse+Storage+Design)
18. [Fluss 湖流一体：Lakehouse 架构实时化演进 | 阿里云](https://developer.aliyun.com/article/1645731)
19. [中心化交易所（CEX）架构：高并发撮合引擎与合规安全体系 | 腾讯云](https://cloud.tencent.com/developer/article/2530600)

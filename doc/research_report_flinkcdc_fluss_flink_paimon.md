# FlinkCDC → Fluss → Flink → Paimon 架构在区块链数据场景的应用研究

## 报告摘要

本报告探索了 FlinkCDC → Fluss（替代Kafka）→ Flink → Paimon 这一新型流式 Lakehouse 架构在 Dune/区块链数据平台中的设计方式、适用场景和核心优势。通过分析各组件的技术特性与区块链适配能力，设计端到端数据流架构，并与 Sim IDX、传统 Kafka+Flink、Dune 现有架构进行多维度对比，揭示了该架构在实时 DEX 分析、DeFi 风控、跨链数据聚合、MEV 检测、NFT 监控等六大场景中的独特价值：Fluss 的列式流存储和 Delta Join 将 Flink 状态减少 10 倍，湖流一体的 Union Read 实现秒级新鲜度的全量数据查询，Paimon 的 LSM 主键表天然适配区块重组导致的数据回滚更新。

---

## 一、架构各组件与区块链适配分析

### 1.1 FlinkCDC：数据摄入层

FlinkCDC 是基于 Apache Flink 的分布式实时数据集成框架（最新版 3.6.0），采用 Pipeline API（YAML 声明式）、SQL API、DataStream API 三层架构。其核心能力包括全增量一体化读取、Schema Evolution 自动同步、端到端 Exactly-Once 语义。

**区块链适配关键发现**：FlinkCDC 当前没有原生的 Ethereum/区块链连接器。其设计哲学基于数据库日志 CDC（Binlog/WAL），而区块链数据变更是 Append-only + 偶尔回滚（Reorg）模式。适配有三条路径：一是自定义 FlinkCDC Source Connector（实现 `Source`/`SourceReader` 接口，封装 Ethereum RPC/WS 调用）；二是通过 Flink DataStream API 直接集成 Web3j；三是间接集成（推荐）——使用现有索引服务（Goldsky、Subsquid、The Graph Firehose）将解码后的区块链数据输出到 Fluss，FlinkCDC 再从 Fluss 消费。Goldsky 已采用 Flink + Redpanda（Kafka 兼容）构建了流优先的区块链数据管道，证明了这条路径的可行性。

### 1.2 Fluss：实时流存储层（替代 Kafka）

Apache Fluss（孵化中）是面向实时分析的列式流存储系统，于 2024 年 11 月开源后捐赠 Apache。核心架构采用 CoordinatorServer + TabletServer 模式，支持双存储引擎——Log Table（仅追加，Arrow IPC 列式格式）和 Primary Key Table（Log + RocksDB KV 索引，流表二象性）。三层存储架构实现冷热分层：热存储（本地磁盘，<1ms 延迟）→ 温存储（远程对象存储，~100ms）→ 冷存储（Paimon/Iceberg 湖格式，~1s）。

**替代 Kafka 的核心优势**：列式流存储支持服务端列裁剪，裁剪 90% 列时读取吞吐提升 10 倍；主键表支持 Upsert 和 Changelog 生成，免除下游去重；Delta Join 将 Flink 双流 Join 状态外置（淘宝案例：状态从 100TB 降至 0，检查点时间从 90 秒降至 1 秒，资源消耗降低 10 倍）；湖流一体的 Tiering Service 自动将 Fluss 热数据沉降到 Paimon，Union Read 实现热冷数据统一查询。

### 1.3 Flink：实时计算层

Apache Flink 作为通用流处理引擎，提供事件时间处理、状态管理（RocksDB 后端支持 TB 级状态）、窗口计算（滚动/滑动/会话窗口）、CEP 复杂事件处理、Exactly-Once 语义等核心能力。在区块链场景中，Flink 的窗口计算可处理区块时间乱序问题，状态管理可维护每个用户的实时 DeFi 持仓，CEP 可检测预言机操纵等模式。

### 1.4 Paimon：数据湖存储层

Apache Paimon 是流原生的湖格式，创新性地将湖存储格式与 LSM 结构结合。主键表支持实时流式更新（查询延迟 <1 分钟），多种 Merge 引擎（去重/部分更新/聚合/首行保留），Deletion Vector 高效标记删除。与 Flink 深度集成支持流式读写和 CDC 集成。Schema Evolution 完整支持列增删改，Time Travel 可回溯任意历史版本。多引擎查询能力（Flink、Spark、Trino、StarRocks）使其成为区块链数据湖的理想存储。

**区块链 Reorg 适配**：Paimon 的主键表 + Upsert 能力天然适配区块重组场景——当链发生重组时，受影响区块的数据可通过主键 `(chain_id, block_number, tx_hash, log_index)` 进行幂等更新，无需重写整个分区。LSM 的 Compaction 机制在后台合并更新，对查询性能影响最小。

---

## 二、端到端架构设计

### 2.1 整体数据流

```
┌─────────────────────────────────────────────────────────────┐
│                      数据源层                                │
│  Ethereum · L2(Base/OP/Arb) · Solana · Indexing Services   │
│  (geth/erigon)    (RPC/WS)     (RPC)    (Goldsky/Subsquid)  │
└──────────────────────┬──────────────────────────────────────┘
                       │ FlinkCDC / 自定义 Source
                       ▼
┌─────────────────────────────────────────────────────────────┐
│               Fluss 实时数据总线（替代 Kafka）                │
│  ┌─────────────┐ ┌──────────────┐ ┌─────────────────┐      │
│  │ raw_blocks  │ │ decoded_logs │ │ decoded_events  │      │
│  │ (Log Table) │ │ (PK Table)   │ │ (PK Table)      │      │
│  │ chain_id    │ │ chain_id,    │ │ chain_id,       │      │
│  │ partition   │ │ block_number │ │ contract_addr   │      │
│  └──────┬──────┘ └──────┬───────┘ └────────┬────────┘      │
│         │               │                   │               │
│         │    Tiering Service (自动沉降)      │               │
└─────────┼───────────────┼───────────────────┼───────────────┘
          │               │                   │
          ▼               ▼                   ▼
┌─────────────────────────────────────────────────────────────┐
│                     Flink 计算层                              │
│  ┌────────────┐ ┌──────────────┐ ┌──────────────────┐      │
│  │ 事件解码    │ │ 实时聚合      │ │ 模式检测(CEP)    │      │
│  │ ABI解析    │ │ 窗口计算      │ │ 巨鲸追踪         │      │
│  │ Reorg处理  │ │ 指标计算      │ │ MEV检测          │      │
│  └─────┬──────┘ └──────┬───────┘ └────────┬─────────┘      │
│        │               │                   │                │
└────────┼───────────────┼───────────────────┼────────────────┘
         │               │                   │
         ▼               ▼                   ▼
┌─────────────────────────────────────────────────────────────┐
│               Fluss 结果流 + Paimon 数据湖                    │
│  ┌─────────────────────┐  ┌───────────────────────────┐     │
│  │ Fluss 热数据层       │  │ Paimon 冷数据层            │     │
│  │ (毫秒级实时查询)     │  │ (历史分析, Trino/Spark)   │     │
│  │ 实时指标/告警结果    │  │ 区块链全量历史数据          │     │
│  │ 最新区块/交易状态    │  │ 解码数据, Spellbook 视图   │     │
│  └──────────┬──────────┘  └───────────┬───────────────┘     │
│             │    Union Read (统一查询)  │                     │
│             └──────────┬───────────────┘                     │
│                        ▼                                     │
│              统一逻辑表（对用户透明）                           │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 区块链数据 Schema 设计

**原始区块表 (raw_blocks)**：Log Table，按 `chain_id` 分区，保留 6 小时热数据

| 字段 | 类型 | 说明 |
|------|------|------|
| chain_id | INT | 链标识 (1=Ethereum, 8453=Base) |
| block_number | BIGINT | 区块高度 |
| block_hash | STRING | 区块哈希 |
| parent_hash | STRING | 父区块哈希 |
| timestamp | BIGINT | 区块时间戳 |
| is_finalized | BOOLEAN | 是否已最终确认 |
| reorg_depth | INT | 重组深度(0=规范链) |
| transactions | ARRAY<ROW> | 交易列表 |

**解码事件表 (decoded_events)**：Primary Key Table，主键 `(chain_id, block_number, tx_hash, log_index)`

| 字段 | 类型 | 说明 |
|------|------|------|
| chain_id | INT | 链标识 |
| contract_address | STRING | 合约地址 |
| event_name | STRING | 事件名称 |
| block_number | BIGINT | 区块高度 |
| tx_hash | STRING | 交易哈希 |
| log_index | INT | 日志索引 |
| is_finalized | BOOLEAN | 最终确认标记 |
| params | MAP<STRING, STRING> | 解码后参数 |

### 2.3 Reorg 与 Finality 处理方案

采用双层 Finality 策略：Fluss 热数据层接收未最终确认数据（延迟 <1 秒），标记 `is_finalized=FALSE`；Tiering Service 仅将 `is_finalized=TRUE` 的数据沉降到 Paimon。Paimon 主键表基于 `(chain_id, block_number, tx_hash, log_index)` 业务键实现幂等 Upsert，重组发生时受影响区块重新写入即可自动覆盖旧数据。不同链的 Finality 延迟不同：Ethereum L1 需 12-64 区块确认（约 2.5-13 分钟），Base/OP Stack 四阶段最终性（200ms/2s/2m/20m），仪表板可使用 safe head 数据，风控账本必须使用 finalized 数据。

---

## 三、六大区块链应用场景

### 3.1 实时 DEX 交易监控与分析

场景：Uniswap/PancakeSwap 等每秒产生大量 Swap 事件，需实时计算价格、滑点、套利机会。L2 每 200ms-2s 产出区块，数据流速极高。

数据流：链上事件 → FlinkCDC（区块摄入）→ Fluss（Swap 事件流，列式存储）→ Flink（VWAP 价格计算/滑点分析/跨 DEX 价差检测）→ Fluss 结果流 → Tiering → Paimon（历史交易数据湖）。

Fluss 替代 Kafka 的理由：Swap 事件包含 20+ 字段但价格计算只需 5-6 个，Fluss 服务端列裁剪可减少约 80% 网络传输；同一交易池储备状态需实时更新，Fluss KV Tablet 支持主键 Upsert 而 Kafka 只能追加需下游去重；计算套利需关联当前 Token 价格维表，Fluss 支持毫秒级主键点查而 Kafka 无法实现。

### 3.2 DeFi 风控实时监控

场景：Aave/Compound 健康因子低于 1.0 触发清算，需实时监控健康因子临界预警、流动性突变、预言机价格操纵。

数据流：链上事件 → Fluss（借贷事件流 + 预言机价格流）→ Flink（滑动窗口健康因子变化率计算 + CEP 预言机操纵模式检测 + 状态管理维护每个用户实时持仓）→ 告警系统 → Paimon（风险事件历史）。

为何需要 Flink 的窗口计算和状态管理：流动性突变检测需在滑动窗口内聚合大额交易，Flink 原生 Event Time 窗口处理区块时间乱序；健康因子计算需维护每个用户实时抵押品和借贷状态（RocksDB 后端支持 TB 级）；预言机操纵需 CEP 模式识别——"短时间多次价格更新 + 大额借贷紧随"；Fluss Delta Join 将双流 Join 状态外置，参考淘宝案例卸载 95TB 状态。

### 3.3 跨链数据聚合分析

场景：DeFi 协议部署在 Ethereum、Arbitrum、Optimism、Base 等多条链，需统一处理跨链数据，监控跨链桥异常。

数据流：多链节点 → FlinkCDC → Fluss（统一数据总线，`chain_id` 分区，统一 Schema）→ Flink（跨链桥 lock/mint 事件 Join、同用户多链同时大额操作检测、全链 TVL 实时聚合）→ Paimon（多链统一数据湖，按 chain 分区）。

Fluss 统一数据总线的理由：以表为第一公民自带 Schema，多链数据统一到一张逻辑表（`chain_id` 分区），无需为每条链维护独立 Topic 和 Schema 映射；跨链桥监控需将源链 lock 事件与目标链 mint 事件 Join，Fluss Delta Join 将状态外置避免巨大 Join 状态；Union Read 支持跨链历史回溯（如某次跨链桥攻击的完整资金流向），列裁剪降低多链数据量 70-90%。

### 3.4 链上行为实时追踪（巨鲸 + MEV）

场景：监控巨鲸地址资金流向，检测三明治攻击（攻击者在受害者交易前买入后卖出获利）。

数据流：链上交易/mempool → Fluss（交易流，按 from/to 索引）→ Flink（巨鲸地址集合状态匹配 + LAG/LEAD 窗口函数分析同区块相邻交易序列 + Gas 价格排序验证攻击者优先级）→ Fluss（检测结果流）→ 告警/可视化。

FlinkCDC + Fluss 低延迟的关键性：三明治攻击在区块确认后即已发生，但实时检测可用于标记被攻击交易提醒用户、统计 MEV 提取量、为 Flashbots 防护系统提供数据。FlinkCDC 毫秒级摄入 + Fluss 亚秒级延迟，使检测延迟从传统批处理分钟级降至秒级。巨鲸追踪需维护动态地址集合维表，Fluss KV Tablet 支持主键更新可实时关联。

### 3.5 NFT 市场实时监控

场景：追踪 ERC-721/1155 铸造、交易、转移事件，实时计算地板价，检测洗盘交易和稀有度陷阱。

数据流：链上事件 → Fluss（NFT 事件流，主键 collection+token_id）→ Flink（按 collection 分组取最低 listing 价格 + 洗盘交易检测：同一 NFT 24h 内同地址反向买卖 + 稀有度陷阱检测）→ Paimon。

Fluss 的独特价值：NFT 地板价计算需实时更新每个 collection 的最低挂牌价，Fluss 主键表支持实时 Upsert 比传统方案（Kafka 追加 + 外部 DB 更新）大幅简化；洗盘交易检测需要关联同一 NFT 的历史交易（同一 token_id 的买卖事件），Fluss 点查能力可高效检索特定 NFT 的交易历史。

### 3.6 链上数据湖建设

场景：历史链上数据的批量分析和回填，解码数据标准化存储，Dune Spellbook 类型的精编数据集。

数据流：Fluss 实时数据 → Tiering Service 自动沉降 → Paimon 数据湖 → Trino/Spark/StarRocks 多引擎查询。

Paimon 数据湖的独特优势：流原生湖格式支持实时更新和历史数据回填同时进行；主键表 + Upsert 天然处理区块重组的数据修正；Z-Order 排序优化 `contract_address` + `block_date` 等多维查询；Deletion Vector 高效标记被重组影响的数据行；开放格式支持 Trino/StarRocks 等现有查询引擎直接读取，与 Dune 的 DuneSQL（Trino 分支）天然兼容。

---

## 四、与现有方案对比

### 4.1 vs Sim IDX（Dune 收购方案）

| 维度 | Sim IDX | FlinkCDC-Fluss-Flink-Paimon |
|------|---------|-----------------------------|
| 核心定位 | 区块链原生实时索引（iEVM 执行层嵌入） | 通用流式 Lakehouse 架构 |
| 延迟 | 亚秒级（iEVM 执行时索引） | 亚秒级（Fluss 列式流 + Flink 计算） |
| 数据深度 | 最深（完整 EVM 状态、内部调用、存储槽） | 中等（依赖索引层提供解码数据） |
| 运维模式 | 全托管 SaaS，零运维 | 开源自建，需维护 Fluss/Flink/Paimon 集群 |
| 多数据源 | 仅区块链 | 多源统一（链上 + 链下 + 传统数据库） |
| 复杂计算 | 有限（Solidity 监听器逻辑） | 强大（Flink SQL/CEP/窗口/状态管理） |
| 历史分析 | 需单独查询引擎 | Union Read 统一实时+历史 |
| 成本模型 | SaaS 计费（CU 消耗） | 开源 + 云资源成本 |

**互补性**：Sim IDX 擅长极致实时索引和深度 EVM 状态访问，可作为数据源层将解码后的事件推送到 Fluss；新架构擅长复杂计算、多源整合和历史分析。两者组合形成"Sim IDX 实时索引 → Fluss → Flink → Paimon"的混合架构，兼具深度和广度。

### 4.2 vs 传统 Kafka + Flink 架构

| 维度 | Kafka + Flink + HDFS/S3 | Fluss + Flink + Paimon |
|------|--------------------------|------------------------|
| 存储格式 | 行式（二进制/JSON） | 列式（Arrow IPC） |
| 列裁剪 | 不支持，全量读取 | 服务端列裁剪，吞吐提升 10 倍 |
| 数据更新 | 仅追加，需下游去重 | 主键 Upsert + Changelog |
| 状态管理 | Flink 维护大状态 | Delta Join 状态外置，状态降 10 倍 |
| Schema | 外部 Schema Registry | 原生表结构，自动对齐 Paimon |
| 流湖集成 | 双系统割裂，需人工拼接 | 湖流一体，Union Read 自动合并 |
| 存储成本 | Kafka+湖双份存储 | 冷热分层，单份存储成本降 10 倍 |
| 一致性 | 需人工管理位点 | Tiering Service 自动对齐 Offset |

### 4.3 vs Dune 现有架构（Trino + S3）

Dune 现有架构以 DuneSQL（Trino 分支）查询引擎为核心，数据存储在 S3 上，偏批处理模式。新架构可显著提升实时能力：Fluss 提供毫秒级实时数据层，补充 Trino 的分钟级查询延迟；Paimon 作为 S3 上的数据湖格式可被 Trino 直接读取（Trino 已有 Paimon 连接器），无需替换现有查询引擎；Union Read 机制使 DuneSQL 查询既能获取历史数据又能获得最新区块数据，实现秒级新鲜度。集成策略是增量叠加而非全面替换——在现有 Trino + S3 之上添加 Fluss 实时层和 Flink 计算层，Paimon 替代部分 S3 直存的数据组织方式。

---

## 五、架构独特优势与局限

### 5.1 核心优势

**流湖一体统一体验**：Fluss + Paimon 通过 Tiering Service 和 Union Read 实现流存储与数据湖的深度融合，用户无需关心数据存储位置，一个 SQL 查询即可获得从实时到历史的全量数据。区块链数据天然有"最新区块"和"历史归档"两层需求，此架构完美匹配。

**列式流存储优化分析**：区块链事件（如 Swap 20+ 字段、ERC-20 Transfer 10+ 字段）在分析时通常只需部分字段。Fluss 服务端列裁剪减少 70-90% 网络传输，对多链高吞吐场景的带宽节省尤为显著。

**Delta Join 解决 Flink 状态瓶颈**：区块链分析频繁需要双流 Join（如交易流 + 价格流、抵押品流 + 借贷流），传统 Flink 需维护 TB 级状态。Fluss Delta Join 将状态外置到存储层，淘宝实测资源消耗降低 10 倍。

**天然适配 Reorg 语义**：Paimon 主键表 + Fluss Upsert + 幂等业务键 = 区块链重组数据自动修正，无需额外的 Reorg 处理管道。双 Finality 策略（热数据未确认/冷数据已确认）兼顾实时性和可靠性。

**开放格式多引擎查询**：Paimon 数据可被 Trino、Spark、StarRocks 直接读取，与 Dune 的 DuneSQL 天然兼容。不会形成技术锁定。

### 5.2 局限性

**FlinkCDC 无原生区块链连接器**：需要自行开发 Source Connector 或依赖第三方索引服务（Goldsky/Subsquid），增加了架构复杂度和数据新鲜度依赖链。

**Fluss 项目成熟度**：目前处于 Apache 孵化阶段，Schema Evolution 仅支持 ADD COLUMN，KvTablet 不支持副本，生产环境大规模部署案例相对有限。

**运维复杂度**：相比 Sim IDX 的全托管模式，自建 Fluss + Flink + Paimon 集群需要专业运维团队，涉及 ZooKeeper/Coordination、TabletServer 集群、Flink 作业管理、Paimon Compaction 等多组件维护。

**延迟下限受限于索引层**：架构的端到端延迟取决于数据摄入方式。若依赖外部索引服务（如 Goldsky），延迟受制于该服务的数据新鲜度；若自建节点 + FlinkCDC，则需承担节点运维成本。

---

## 六、结论

FlinkCDC → Fluss → Flink → Paimon 架构为区块链数据平台提供了一套从实时到历史的全链路解决方案。其核心价值在于：Fluss 的列式流存储和湖流一体设计解决了 Kafka 在分析场景的根本短板（无法列裁剪、不支持更新、流湖割裂）；Paimon 的 LSM 主键表天然适配区块链 Reorg 语义；Flink 的窗口计算和 CEP 满足了 DeFi 风控等复杂分析需求；Delta Join 将 Flink 状态瓶颈降低一个数量级。

与 Sim IDX 相比，该架构在数据深度和零运维上存在差距，但在多源整合、复杂计算、历史分析和成本可控方面具有显著优势。两者可形成互补：Sim IDX 作为实时索引层，Fluss-Flink-Paimon 作为计算存储层。与 Dune 现有 Trino + S3 架构的集成是增量式的——添加 Fluss 实时层和 Paimon 数据湖格式，而非全面替换，可渐进式提升平台的实时能力。

---

## 参考文献

1. [Apache Fluss™ (Incubating) 官方主页](https://fluss.apache.org/)
2. [Fluss Architecture | Apache Fluss 官方文档](https://fluss.apache.org/docs/concepts/architecture/)
3. [Paimon Integration | Apache Fluss 官方文档](https://fluss.apache.org/docs/streaming-lakehouse/integrate-data-lakes/paimon/)
4. [湖流一体：基于 Fluss+ Paimon 的实时湖仓数据底座 | 阿里云开发者社区](https://developer.aliyun.com/article/1709004)
5. [Fluss湖流一体列式流存储赋能Flink实时分析 | 阿里云开发者社区](https://developer.aliyun.com/article/1644885)
6. [Apache Fluss vs. Apache Paimon: Two Engines for the Real-Time Lakehouse | Flink Forward Asia](https://asia.flink-forward.org/blog/apache-fluss-vs.-apache-paimon-two-engines-for-the-real-time-lakehouse)
7. [Understanding Apache Fluss | Jack Vanlightly](https://jack-vanlightly.com/blog/2025/9/2/understanding-apache-fluss)
8. [Introducing Fluss: Streaming Storage for Real-Time Analytics | Alibaba Cloud](https://www.alibabacloud.com/blog/introducing-fluss-streaming-storage-for-real-time-analytics_601921)
9. [Apache Flink CDC 3.6.0 Release Announcement](https://flink.apache.org/2026/03/30/apache-flink-cdc-3.6.0-release-announcement/)
10. [Flink CDC 项目介绍 | Apache Flink CDC 文档](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.4/zh/docs/get-started/introduction/)
11. [Two-Phase Commit Protocol | Apache Flink CDC DeepWiki](https://deepwiki.com/apache/flink-cdc/8.3-two-phase-commit-protocol)
12. [Apache Paimon 官方主页](https://paimon.apache.org/)
13. [How to Build a Real-Time Web3 Analysis Infrastructure with Apache Doris and Flink | VeloDB](https://www.velodb.io/blog/build-real-time-web3-analysis-with-apache-doris-and-flink)
14. [Blockchain Data Streaming: From Nodes to Real-Time SQL | RisingWave](https://risingwave.com/blog/blockchain-data-streaming-nodes-to-sql/)
15. [Integrating Blockchain Data Pipelines | 7Block Labs](https://www.7blocklabs.com/blog/integrating-blockchain-data-pipelines-7block-labs-technical-playbook)
16. [Real-Time MEV Detection with Streaming SQL | RisingWave](https://risingwave.com/blog/mev-detection-streaming-sql/)
17. [Goldsky 基于 Flink、Redpanda 和 Kubernetes 的区块链数据流优先架构 | InfoQ](https://segmentfault.com/p/1210000046312920)
18. [The State of EVM Indexing | Dune Blog](https://dune.com/blog/the-state-of-evm-indexing)
19. [DuneSQL: A query engine for Blockchain data | Trino Fest 2023](https://trino.io/blog/2023/07/14/trino-fest-2023-dune.html)
20. [Meet the Streamhouse Trio | Jaehyeon Kim](https://jaehyeon.me/blog/2025-05-06-streamhouse-trio/)
21. [Data Types and Schema Evolution | Apache Fluss DeepWiki](https://deepwiki.com/alibaba/fluss/3.4-data-types-and-schema-evolution)
22. [Architecture | Apache Fluss DeepWiki](https://deepwiki.com/apache/fluss/2-architecture)

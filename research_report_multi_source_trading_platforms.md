# 多源数据交易项目/平台调研报告：DEX + CEX + 私有数据融合的现状与 Dune/Nansen 战略分析

## 执行摘要

经过对英语区和中文区市场的广泛调研，目前尚未发现任何平台完整地将 DEX 链上数据、CEX 订单/深度数据和私有数据库三类数据源融合后做场景化交易。市场上存在多个"部分融合"的项目：Amberdata 和 Databricks Web3 在技术架构上同时摄入 CEX 和链上数据，但面向机构合规/分析而非交易执行；Nansen 已于 2025-2026 年开始向交易执行转型，但仍仅聚合 DEX 聚合器，未接入 CEX 数据；Solidus Labs HALO 是少数真正融合链上+链下数据的平台，但专注合规监控而非交易。Dune/Nansen 不做交易的核心原因是商业选择（约 60%），技术限制约占 40%。Dune 的 DuneSQL（Trino 分支）从根本上不支持实时流处理，Sim 是独立的实时 API 产品，两者无法跨 Join；Nansen CEO 公开承认监管问题是阻碍交易功能的关键因素。

## 背景

Web3 数据分析领域长期存在"链上与链下数据割裂"的痛点。Dune、Nansen 等主流平台仅能查询链上数据，无法关联 CEX 的价格深度和订单簿。与此同时，arXiv 2025 年的研究显示，仅 19 个主要搜索者就从超过 720 万次 CEX-DEX 套利中提取了 2.338 亿美元价值，但普通用户几乎无法实时捕捉这些机会。本调研旨在回答三个核心问题：市面上是否有将 DEX、CEX 和私有数据库融合做场景化交易的平台？如果有，它们的软件架构如何？以及 Dune/Nansen 为什么不做交易——是技术受限还是商业选择？

## 一、英语区：部分融合的多源数据平台

### 1.1 Amberdata：机构级链上+CEX 数据融合

Amberdata 是目前最接近"多源融合"的机构级平台。其技术架构采用 Redpanda（Kafka 兼容消息队列）作为实时流总线，将链上事件和 CEX 市场数据统一摄入；中间层使用 Databricks/Delta Lake 的 Medallion 架构（Bronze-Silver-Gold 三层），对原始数据逐层清洗和聚合；查询层使用 Apache Pinot 提供实时 OLAP 分析。Amberdata 同时覆盖 100+ 条链的链上数据和主要 CEX 的市场数据，但定位是"数字资产数据基础设施"而非交易平台，主要为对冲基金和资管机构提供数据馈送和风险监控。

### 1.2 Databricks Web3 Streaming：Bronze-Silver-Gold 流式架构

Databricks 在 2024 年发布的技术博客展示了 Web3 流式数据架构方案，同时摄入 Binance CEX 数据（WebSocket）、Uniswap DEX 数据（链上事件）和 Ethereum 区块链数据（Python Data Source API + Structured Streaming），采用 Bronze-Silver-Gold Medallion 架构分层处理。这是少见的公开架构方案，将三类数据源在同一平台内统一处理，但本质是 Databricks 平台的用例展示，并非面向交易的独立产品。

### 1.3 Nansen：从数据分析向交易执行的转型

Nansen 的战略动向最具参考价值。2025-2026 年间，Nansen 推出了 AI 驱动的交易执行功能，在 Solana 和 Base 链上聚合 DEX 聚合器（Jupiter、LiFi、OKX DEX），允许用户直接在 Nansen 平台内执行 Swap。然而这一转型有两个关键局限：第一，仍然没有 CEX 数据集成，交易执行仅限链上 DEX；第二，交易功能有地理限制，新加坡等地用户被屏蔽。Nansen CEO Alex Svanevik 公开表示监管合规是交易功能面临的核心挑战。

### 1.4 Arkham Intelligence：链上情报+CEX 市场数据+交易所

Arkham 的 Ultra AI 系统已建立 3 亿+地址标签，同时拥有 CEX 市场数据（交易量、资金费率等），并在 2024 年推出了 Arkham Exchange。但其"融合"更多是在产品层面的数据展示，而非底层数据架构的统一。Arkham 的链上标签与 CEX 市场数据是独立的数据管道，通过应用层拼接展示。

### 1.5 Solidus Labs HALO：合规监控领域的真正融合

Solidus Labs 的 HALO 平台是少数真正在底层融合链上和链下（CEX）数据的系统，主要用于交易监控和市场滥用检测（如内幕交易、价格操纵、洗盘交易）。HALO 同时监控 DEX 和 CEX 的交易行为，跨市场关联分析异常模式。但其定位是合规/监管科技（RegTech），而非交易执行。

### 1.6 CEX-DEX 套利：学术与开源实践

arXiv 2025 年的论文《Measuring CEX-DEX Extracted Value and Searcher Profitability》量化了 CEX-DEX 套利市场：19 个搜索者从 720 万次套利中提取 2.338 亿美元。开源方面，GitHub 上的 DexCex_bot 是一个多链 CEX-DEX 套利机器人，同时连接链上 RPC 和 CEX API，实时监控价差并自动执行套利。这是最接近"多源数据+交易执行"的开源实现，但仅覆盖套利单一场景，且为个人工具级别。

## 二、中文区：量化工具与数据平台

### 2.1 FMZ Quant：量化交易自动化平台

FMZ Quant（发明者量化）是中文区最具代表性的多源数据交易工具平台。它支持同时连接多个 CEX 交易所（币安、OKX 等）和链上 RPC 节点，提供统一的策略编写框架（JavaScript/Python/C++），可执行跨市场套利、做市等策略。但其"多源融合"是策略层面的——用户在策略代码中自行处理不同数据源的关联，平台本身不提供统一的数据管道或融合查询能力。私有数据通过平台内的数据库功能存储，但与链上/CEX 数据无原生关联机制。

### 2.2 AiCoin：行情数据+指标分析

AiCoin 提供多交易所行情聚合和技术指标分析，覆盖主要 CEX 的 K 线、深度、资金费率等数据，也提供链上大额转账监控等部分链上数据。但核心定位是"行情工具"而非"多源融合交易平台"，链上数据仅作为辅助信号展示，无法与 CEX 深度数据做联合分析。

### 2.3 Coinglass：衍生品数据聚合

Coinglass 聚合多所 CEX 的衍生品数据（未平仓合约、清算数据、资金费率、多空比），是 CEX 多源数据聚合的典型案例，但不涉及链上数据。

### 2.4 其他中文区项目

0xScope 是 Web3 知识图谱协议，使用加权聚合算法将地址关联为实体，融合链上和 Web2 社交数据，但定位为数据层而非交易平台。Edgen 自称"Web3 Bloomberg Terminal"，融合交易洞察、社交情绪和实时链上数据，使用 EDGM 模型做推理，但仍处于早期阶段。ChainThink（链思）定位链上智能分析，但网站内容较薄，产品形态尚不明确。

## 三、软件架构对比分析

| 平台 | DEX 数据 | CEX 数据 | 私有数据 | 交易执行 | 核心架构 |
|------|---------|---------|---------|---------|---------|
| Amberdata | 100+ 链 | 市场数据 | 部分 | 否 | Redpanda → Databricks/Delta Lake (Medallion) → Apache Pinot |
| Databricks Web3 | Uniswap 等链上 | Binance WS | 否 | 否 | Structured Streaming → Bronze-Silver-Gold → Delta Lake |
| Nansen | 多链链上 | 仅市场展示 | 否 | DEX Swap（Solana/Base） | 自建索引 + 实时流 |
| Arkham | 链上+3 亿标签 | 市场数据 | 否 | Arkham Exchange | 独立管道，应用层拼接 |
| Solidus HALO | DEX 交易 | CEX 交易 | 合规规则 | 否（合规监控） | 融合管道，跨市场关联 |
| FMZ Quant | 链上 RPC | 多 CEX API | 策略数据库 | 是（策略执行） | 策略层融合，无统一管道 |
| DexCex_bot | 多链 RPC | CEX API | 否 | 是（套利执行） | 链上+CEX 直连，极简架构 |

从架构模式看，市场上存在三种融合路径：

第一种是"统一管道"模式（Amberdata、Databricks、Solidus），在数据摄入层就统一不同来源的数据，通过 Medallion 架构逐层处理，底层共享存储和计算引擎。这种模式技术最成熟，但开发成本最高，目前无一做到交易执行。

第二种是"应用层拼接"模式（Arkham、Nansen），不同数据源走独立管道，在展示/应用层做关联。开发速度快，但数据一致性和联合分析能力受限。

第三种是"策略层融合"模式（FMZ、DexCex_bot），由用户/策略在运行时自行拉取和关联不同数据源。灵活性最高，但平台本身不提供数据融合能力。

## 四、Dune/Nansen 为什么不做交易

### 4.1 技术限制（约 40%）

Dune 的核心查询引擎 DuneSQL 是 Trino 的分支，本质是批处理 OLAP 引擎，查询延迟在分钟级。Dune 的 Materialized Views 只能做有限预计算加速，无法实现亚秒级响应。Dune 于 2024-2025 年推出的 Sim 是一个独立的实时 API 产品，基于自建的实时流处理管道，延迟小于 1 秒，支持 60+ 链，提供 REST API 访问。但关键限制是：Sim 和 Dune SQL 是两个完全独立的系统——独立账户、独立定价、独立 API，两者之间无法跨 Join。这意味着 Sim 的实时数据无法与 Dune SQL 中的历史链上数据做联合分析，更不可能与 CEX 数据做实时关联。

Nansen 的技术架构是自建的链上索引系统，实时性优于 Dune 但仍非流式架构。Nansen 在 2025 年推出的交易执行功能已经证明了技术可行性，但它仅聚合 DEX 聚合器（Jupiter、LiFi），并未接入 CEX 订单簿。接入 CEX 交易执行需要与交易所建立 API 合作、处理订单路由和撮合逻辑、管理用户资金托管——这些都不是 Data Infra 团队的核心能力。

### 4.2 商业选择（约 60%）

从商业角度看，Dune 和 Nansen 选择"只做 Data Infra"有明确的经济逻辑。全球加密货币交易所的市场规模是数据/分析市场的 20-50 倍。2024 年币安年收入估计超过 100 亿美元，而 Dune 的 ARR 约 1000-2000 万美元，Nansen 的 ARR 约 500-1000 万美元。但交易所市场是赢者通吃、监管极严的红海，而 Data Infra 是低风险、高毛利的利基市场。

Nansen CEO Alex Svanevik 在多个场合表示，监管合规是交易功能面临的核心挑战。Nansen 的交易功能在新加坡等地区被屏蔽，这印证了合规风险的严重性。Dune 作为一个开放平台（任何人可查询、可 Fork），做交易执行需要 KYC/AML 合规，与其开放精神冲突。

Dune 的商业模式基于查询收费（Dune SQL 按计算量计费，Sim 按请求量计费），利润率远高于交易执行（交易需要做市、流动性提供、资金成本）。Nansen 的订阅制（Standard $150/月、VIP $2000/月、Alpha $20000/月）已经建立了稳定的经常性收入。

### 4.3 "只做 Data Infra 就够了"的商业逻辑

Dune 和 Nansen 的战略选择可以概括为"数据层即护城河"。Dune 通过社区贡献的 Spellbook（20,000+ 抽象表）建立了极高的数据壁垒，Nansen 通过 3 亿+地址标签建立了情报壁垒。两者都在 Data Infra 层建立了不可替代性，而交易层是低差异化、高竞争的领域。

但 Nansen 的转型说明"只做 Data Infra"的边界正在被打破。随着 AI Agent 和自动化交易的需求增长，数据分析与交易执行之间的界限正在模糊。Nansen 从"数据工具"向"交易助手"的转型，既是商业扩张，也是应对 Data Infra 市场天花板的选择。

## 五、对 sim-flink 项目的启示

### 5.1 市场空白确认

本次调研确认了一个明确的市场空白：目前没有任何平台在底层架构层面完整融合 DEX 链上数据、CEX 订单/深度数据和私有数据库，并在此基础上提供场景化交易能力。Amberdata 和 Solidus Labs 在技术架构上最接近，但定位是机构数据服务/合规监控；Nansen 在产品层面最接近，但 CEX 数据和交易执行尚未打通。

### 5.2 架构差异化

sim-flink 的架构（FlinkCDC → Fluss → Flink → Paimon → Fluss/Milvus）相比现有平台有三个核心差异化优势：

第一，Fluss 统一流数据总线解决了 Amberdata 架构中 Kafka（Redpanda）+ Delta Lake + Pinot 三层系统的复杂度问题。Fluss 的列式流存储、主键 Upsert、Delta Join 和湖流一体 Union Read 将流处理、存储和湖仓统一到一个系统中。

第二，多源时序对齐是现有平台的普遍短板。Flink 的 Event Time + Watermark + Watermark Alignment 机制可以精确对齐链上（12 秒级）、CEX（毫秒级）和私有数据的时间戳，而 Amberdata 的 Redpanda 和 Nansen 的自建管道在多源时序对齐上缺乏原生支持。

第三，AI 向量存储层（Fluss 或 Milvus）使语义搜索成为原生能力，这是所有现有平台都不具备的。Dune 的 AI 功能仅限于辅助 SQL 生成，Nansen 的 Smart Money 标签基于规则而非语义。

### 5.3 需要注意的挑战

Nansen 在交易执行上遇到的合规问题是前车之鉴。sim-flink 如果要走向交易执行，需要从架构层面考虑合规：私有数据的 ACL 隔离（Fluss 的 Database/Table 级权限控制）、用户资金的非托管方案（如订单签名而非资金存入）、以及地理限制的配置化支持。Dune 的 Sim 与 Dune SQL 割裂也提醒我们，实时和历史数据的统一查询（Fluss + Paimon Union Read）是架构层面的关键设计决策。

## 结论

市面上不存在完整融合 DEX+CEX+私有数据做场景化交易的平台。最接近的竞品（Amberdata、Solidus、Nansen）要么定位数据/合规服务而非交易执行，要么仅覆盖部分数据源。Dune/Nansen 不做交易主要是商业选择（监管风险、市场定位、利润模式），技术限制是次要因素。DuneSQL（Trino）从根本上无法做实时流处理，Sim 是独立系统，两者无法融合；Nansen 已开始交易转型但仍受限于合规。sim-flink 的 Fluss 统一总线架构在多源融合、时序对齐和 AI 语义搜索三个维度上有明确的技术差异化，市场空白真实存在。关键挑战不在于技术可行性，而在于从 Data Infra 向交易场景延伸时的合规策略和产品定位。

## 局限性

部分平台（如 ChainThink）的公开信息有限，无法深入分析其架构。Nansen 的交易功能仍在早期阶段，未来可能扩展到 CEX 集成。开源套利机器人（如 DexCex_bot）迭代速度快，可能快速演进为更完整的多源交易工具。中文区的量化交易工具有大量未公开的私有部署实例，本调研可能遗漏了部分非公开项目。

## References

1. [Amberdata Official Platform](https://amberdata.io)
2. [Streaming CEX, DEX, and Blockchain Events in Databricks for Web3 | Databricks](https://community.databricks.com/t5/technical-blog/streaming-cex-dex-and-blockchain-events-in-databricks-for-web3/ba-p/120503)
3. [Nansen AI Trading Launch | Nansen Blog](https://nansen.ai/blog)
4. [Solidus Labs HALO Platform](https://soliduslabs.com)
5. [Arkham Intelligence Official](https://platform.arkhamintelligence.com)
6. [Measuring CEX-DEX Extracted Value and Searcher Profitability | arXiv](https://arxiv.org/abs/2507.13023)
7. [DexCex_bot GitHub Repository](https://github.com/Hypnorabbit/DexCex_bot)
8. [FMZ Quant Platform](https://www.fmz.com)
9. [0xScope Protocol](https://0xscope.com)
10. [Dune Sim Official Documentation](https://docs.dune.com)
11. [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
12. [Apache Pinot Real-time OLAP](https://pinot.apache.org)
13. [Coinglass Derivatives Data](https://www.coinglass.com)

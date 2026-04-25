# 研究计划：FlinkCDC-Fluss-Flink-Paimon-AI向量数据库 多源实时分析架构

## 研究目标
在已有 FlinkCDC → Fluss → Flink → Paimon 架构基础上，扩展设计：
1. 加入 AI 向量数据库层（Milvus/Qdrant/Weaviate）
2. 整合链上事件流 + CEX 订单数据 + 私有数据
3. 实现实时 Analytics 的完整技术方案

## 子任务分配

### 子代理1：AI向量数据库与流式架构集成
- Flink → 向量数据库的实时 Embedding 写入路径
- Milvus/Qdrant/Weaviant 在实时场景的对比
- Paimon 与向量数据库的数据同步（冷热分层）
- Fluss 如何作为向量数据库的实时数据源
- RAG + 实时链上数据的结合模式

### 子代理2：CEX订单流 + 私有数据集成
- CEX（Binance/OKX）WebSocket/API 数据摄入到 Fluss
- CEX 订单簿实时重建（Flink 状态管理）
- 私有数据（内部交易信号、OTC数据）的安全集成
- 链上+CEX+私有数据的多源 Join 和对齐
- 时序对齐问题（链上12s区块 vs CEX ms级 vs 私有数据不定）

### 子代理3：痛点分析与场景设计
- 当前 Web3 数据分析的核心痛点
- 多源实时分析的具体场景（套利检测、风控、Alpha 信号等）
- 每个场景的架构如何工作、解决什么问题
- 各组件在场景中的具体作用

## 使用工具
- web_search / web_fetch: 技术文档和架构资料
- wechat-article-search: 中文技术社区资料

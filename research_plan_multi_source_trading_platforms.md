# 研究计划：多源数据交易项目/平台调研

## 研究目标
1. 更正架构文档：AI 向量数据库可用 Fluss 存储（不仅仅是 Milvus）
2. 确认 Dune Sim 的实时能力（实时与非实时是分开的）
3. 调研市面上（英语区+中文区）已有的将 DEX+CEX+私有数据库结合的多源数据交易项目/平台
4. 如果有，分析其软件架构
5. 分析 Dune/Nansen 为什么不做交易场景——是技术受限还是商业选择？他们是否只做 Data Infra 层就够

## 子任务分配

### 子代理1：英语区多源数据交易平台调研
- 搜索结合 DEX+CEX+私有数据做交易的英文项目/平台
- 关键词：multi-source trading platform, DEX CEX arbitrage platform, on-chain off-chain data fusion, cross-market analytics, Web3 trading intelligence
- 重点关注：是否真的结合了三类数据源（链上DEX、CEX订单/深度、私有数据）
- 分析其软件架构（数据摄入→处理→存储→应用层）
- 检查项目：Amberdata, Nansen, Chainalysis, Dune Sim, Arkham, DEX Screener, DeFi Llama, 1inch, Jain Security 等
- 关注 Sim (Dune 的实时平台) 的具体能力和局限

### 子代理2：中文区多源数据交易平台调研
- 搜索中文区的相关项目/平台
- 关键词：链上链下数据融合, DEX CEX 套利平台, 多源数据分析, Web3 交易智能, 链上数据分析平台
- 关注：Mest, OKX 链上数据, Footprint Analytics, HashKey, TokenInsight, 币安研究院等
- 也搜索是否有华人团队做的类似项目
- 使用 wechat-article-search 搜索微信公众号文章

### 子代理3：Dune/Nansen 为什么不做交易场景分析
- 深入分析 Dune 和 Nansen 的商业模式和战略定位
- Dune Sim 的实时能力分析——实时和非实时的架构差异
- 技术限制：实时数据处理的技术门槛、CEX 数据接入的法律/合规问题、私有数据处理的安全挑战
- 商业选择：Data Infra 层的市场规模、做交易场景的合规风险、用户群体差异
- 分析"只做 Data Infra 就够了"的商业逻辑
- 搜索 Dune/Nansen 的融资、收入模式、用户数据等
- 搜索 Dune 和 Nansen 的创始人和高管对实时/交易场景的公开表态

## 使用工具
- web_search / web_fetch: 技术文档和架构资料
- wechat-article-search skill: 中文技术社区资料
- RAG_search: 微信小程序相关（如有）

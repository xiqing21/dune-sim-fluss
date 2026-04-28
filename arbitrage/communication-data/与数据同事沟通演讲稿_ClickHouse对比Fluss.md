# 与 ForthTech 数据同事沟通演讲稿 v2

## 核心策略调整（相比 v1 的根本转变）

v1 的隐含假设：他们在用 ClickHouse 做一切，需要说服他们引入 Flink。
**实际情况：他们大概率已经在用 Kafka + Flink + 某存储 + ClickHouse。**

所以叙事要从"你需要 Flink"变成"你已经有 Flink 了，但 Kafka 作为流中间件限制了你的 Flink 能做什么"。Fluss 替换 Kafka 层，解锁 7 个新策略能力。

---

## 架构认知对齐

**他们现在的架构（推测）**：

```
CEX/链上数据 → Kafka → Flink → ??? → ClickHouse
                              ↑
                        状态存在 RocksDB
                        /内存里，TB 级
```

**我们提议的架构**：

```
CEX/链上数据 → Fluss → Flink → ClickHouse
                    ↑
              PK Table 替 Redis
              Delta Join 清零状态
              列裁剪省带宽
              Union Read 统一回测实盘
```

**变化只有一个：Kafka → Fluss。Flink 和 ClickHouse 不动。**

---

## 坑位预警

### 坑 1：说 Kafka 不行

**绝对不要说**。Kafka 是他们现有架构的骨干，跑了几年了，可能还有专门的 Kafka 运维团队。你说 Kafka 不行，等于打他们整个基础设施的脸。

**正确说法**："Kafka 作为消息队列做得很好——高吞吐、持久化、多 Consumer。但有些场景 Kafka 的'全量消息广播'模式会变成瓶颈，特别是 Flink 消费 Kafka 做双流 Join 的时候。Fluss 在保持 Kafka 协议兼容的同时，加了列裁剪、PK Table、Delta Join 这几个能力，让 Flink 的计算效率提升很多。"

### 坑 2：上来就讲 Fluss 特性

数据同事大概率没听过 Fluss。上来讲 Delta Join、列裁剪、Union Read——他会觉得你在卖药。

**正确做法**：从他们的 Flink 使用痛点出发。先问他 Flink 作业的状态管理、Kafka Consumer 的带宽占用、Flink+ClickHouse 之间的数据一致性——让他说出痛点，然后"正好 Fluss 解决的就是这个"。

### 坑 3：说"你们架构有问题"

他亲手搭的架构，你说有问题就是打脸。

**正确说法**："你们这套 Kafka+Flink+ClickHouse 是业界标准架构，淘宝、字节早期也是这么搭的。后来淘宝碰到状态膨胀到 100TB 的问题，才演化出 Fluss。你们的规模可能还没到那个程度，但有些场景——比如多源信号关联——用 Fluss 替换 Kafka 可以提前避开这些坑。"

### 坑 4：忽略他已经在用 Flink

如果你讲"ClickHouse 做不了流式 Join，所以你需要 Flink"，他会觉得你根本不了解他的系统。

**正确做法**：开场就问"你们 Flink 作业大概有多少个？状态最大的作业有多大？"——展示你理解他有 Flink，然后谈的是"Flink + Kafka"的优化，不是"Flink vs ClickHouse"。

### 坑 5：被问"Fluss 和 Kafka 有什么区别，为什么不继续用 Kafka"

他一定问这个。

**核心回答**：Fluss 是 Kafka 的超集。协议兼容（Kafka Consumer 可以直接读 Fluss），但加了三个 Kafka 没有的能力：PK Table（替代 Redis 做状态）、Delta Join（Flink 双流 Join 零状态）、列裁剪（Consumer 只读需要的列）。Kafka 继续能做的事 Fluss 都能做，但 Fluss 能做的一些事 Kafka 做不了——特别是跟 Flink 配合的场景。

### 坑 6：被问"Fluss 成熟度够吗"

Fluss 还在 Apache 孵化阶段。

**诚实回答**："Fluss 确实还在 Apache 孵化阶段，生产级案例主要在阿里体系内。这也是为什么我不建议把核心交易路径从 Kafka 迁移到 Fluss——风险太大。我建议的路径是：非核心路径先跑 Fluss，比如 Alpha 信号生成、多源数据关联这些不影响交易的场景。如果跑稳了，再考虑核心路径的迁移。Kafka 和 Fluss 可以并行跑，不冲突。"

---

## 演讲结构（预计 20-30 分钟）

### 【开场 2 分钟：建立技术对话，不是汇报】

"你好，Darren 让我过来跟你们数据团队聊聊。上次跟 Darren 聊的时候他给我一个反馈——说我太偏数据后端了，要往前走一步，往策略和 Alpha 方向靠。

所以我这几天做了两件事：一是研究了市面上最前沿的几个实盘策略——推特上有人用 AI 分析了 221 个合约币种找到妖币做空策略，有人做了清算热力图实时预警，有人用费率反转+OI 做信号——我把这些策略的技术需求拆了一下；二是用 Binance API 跑了 7 天真实数据，准备做一个Funding Rate 和 CEX-DEX 价差的回测。

先问一下——你们现在 Flink 用得多吗？主要做什么类型的作业？"

**（等他回答，了解他 Flink 的使用深度）**

"OK，那我对齐一下我的理解——你们大概是 Kafka 做数据总线、Flink 做实时计算、ClickHouse 做 OLAP 查询和存储，对吧？这是很标准的架构。我之前在国网也是这么搭的。

今天我想聊的不是'你们架构有问题'，而是'我在研究这些新策略的时候发现，有些场景下 Kafka 作为 Flink 的上游会变成瓶颈'。然后我发现 Fluss——Apache 的一个新项目——恰好解决的就是 Kafka+Flink 的这个瓶颈。"

### 【第一段 5 分钟：Kafka+Flink 的实际痛点——不是不行，是不够】

"你们用 Flink 跑双流 Join 的时候，状态管理是怎么做的？

我猜可能是这样——两个 Kafka Topic 分别做 Source，Flink 内部用 RocksDB 做状态后端，存右侧流的全量数据。淘宝早期也是这么干的，后来状态膨胀到 100TB，检查点时间 90 秒。

**Kafka + Flink 的三个结构性瓶颈**：

**瓶颈 1：双流 Join 的状态膨胀**

Kafka 是 append-only 的日志，没有主键概念。Flink 消费两个 Kafka Topic 做双流 Join，必须在 Flink 内部维护两个流的全量状态。如果你们做链上 Swap 和 CEX 深度的实时关联——两个流加起来每秒几万条——Flink 的状态会持续膨胀。

淘宝实测：2300 CU 的 Flink 作业，100TB 状态，检查点 90 秒。后来他们用 Fluss 的 Delta Join，状态降到 0，资源降到 200 CU。

**瓶颈 2：最新状态的维护**

Flink 作业需要频繁读取'最新值'——比如某个币种的最新 Funding Rate、最新标记价格、最新 OI。这些数据在 Kafka 里是追加的，没有主键，Flink 每次都要扫描或者自己维护一个 HashMap/RocksDB 状态。

你们有没有在 Flink 之外用 Redis 做状态缓存？如果有，那就是两个系统要维护一致性——Flink 的状态和 Redis 的状态可能不同步。

**瓶颈 3：全量消费的带宽浪费**

Kafka 的 Consumer 必须读完整消息。你们的 CEX 深度更新每条可能 500 字节、20+ 字段，但一个 Spread 策略只需要 5-6 个字段。3 万行/秒 × 500 字节 = 15MB/s/Consumer。5 个策略就是 75MB/s 的带宽，但其实 75% 的数据是浪费的。"

### 【第二段 5 分钟：Fluss 替换 Kafka 层——不是推翻，是升级】

"Fluss 不是另一个消息队列。它跟 Kafka 的关系是：**协议兼容 + 三个新能力**。

**协议兼容**：Kafka Consumer 可以直接读 Fluss Topic。你们的 Kafka→Flink 链路不需要改代码，只改连接地址就行。

**新能力 1：Primary Key Table——替代 Redis 做状态**

Kafka 只有 Log Table（append-only）。Fluss 加了 PK Table——同一个主键多次写入只保留最新值。这等于 Kafka + Redis 合二为一。

对 Flink 的直接价值：Flink 作业读取 Fluss PK Table，不需要自己维护状态。最新 Funding Rate、最新标记价格、最新 OI——全部点查 Fluss PK Table，亚毫秒级返回，比 Redis 还快（因为 Fluss 热数据在本地 SSD，Redis 是网络调用）。

而且 Fluss PK Table 的数据可以被 Flink SQL 直接查询和 Join，不需要像 Redis 那样额外同步。

**新能力 2：Delta Join——Flink 双流 Join 零状态**

传统 Flink 双流 Join：左流 + 右流都存状态 → TB 级。
Fluss Delta Join：左流正常消费，右流改成 Fluss PK Table 点查 → 零状态。

你们的链上 Swap 和 CEX 深度关联，如果右流（CEX 深度最新状态）放在 Fluss PK Table 里，Flink 只需要消费左流（Swap 事件），到达一条就点查一次 Fluss 拿到最新 CEX 价格，计算价差。状态从 TB 降到 0。

**新能力 3：列裁剪——Consumer 只读需要的列**

Kafka Consumer 必须读整条消息。Fluss Consumer 可以指定只读某些列，服务端只返回这些列的数据。

3 万行/秒，20 字段变 5 字段：75MB/s → 3.6MB/s，带宽节省 95%。而且 Fluss 是服务端裁剪，不是客户端——多个 Consumer 共享同一数据流，不像 Kafka 每个 Consumer 独立消费全量。"

### 【第三段 5 分钟：Kafka+Fluss+Flink+ClickHouse 的分工】

"所以完整的架构是四层，各司其职：

```
数据摄入层：Fluss（替代 Kafka）
  - Log Table：全量事件流（Kafka 兼容）
  - PK Table：最新状态（替代 Redis）
  - 列裁剪：Consumer 按需读取

计算层：Flink（不动）
  - 双流 Join → Delta Join（零状态）
  - 窗口聚合、特征计算
  - CEP 复杂事件检测

历史层：Paimon（通过 Fluss Tiering 自动归档）
  - 全量历史明细
  - 回测数据源

查询层：ClickHouse（不动）
  - OLAP 即席查询
  - 批量回测
  - Dashboard / 报表
```

**核心分工原则**：

- Flink 做计算，不做存储（状态外置到 Fluss）
- Fluss 做实时数据和状态，不做 OLAP
- ClickHouse 做 OLAP 和历史查询，不做流式 Join
- Paimon 做全量历史，通过 Union Read 和 Fluss 透明合并

**迁移路径**：

1. 第一阶段：Fluss 和 Kafka 并行跑。新作业走 Fluss，旧作业不动
2. 第二阶段：Flink 的双流 Join 作业逐步从 Kafka Source 切到 Fluss Delta Join
3. 第三阶段：Redis 缓存层逐步用 Fluss PK Table 替代
4. 全程 Kafka 协议兼容，回滚零成本"

### 【第四段 8 分钟：7 个新策略能力——为什么它们需要 Fluss 而不是 Kafka】

"前面讲了架构。现在聊点更有意思的——我研究了推特上几个正在实盘跑的策略，把它们的技术需求拆了一下，发现一个规律：**这些策略用 Kafka+Flink 做起来很痛苦，用 Fluss+Flink 就很自然**。

我挑 3-4 个最有代表性的讲：

---

**策略 1：Short Squeeze 三信号检测**

推特上有人做了妖币雷达，用三个信号做 Short Squeeze 检测：资金费率极端负值 + OI 大幅增长 + 成交量爆发。三个信号对齐 = 高置信度入场，回测胜率 62-71%。

**技术需求**：三个信号来自完全不同的数据源（FR 1h/8h、OI 日级、Volume 实时），需要实时关联。Kafka 方案：三个 Kafka Topic → Flink 三路 Regular Join → 状态存三个流的全量数据。Fluss 方案：FR 和 OI 放 PK Table，Volume 做 Log Table，Flink 用 Delta Join 点查 FR 和 OI 最新值，零状态。

**为什么 Kafka 做得痛苦**：三路 Regular Join 的状态是三个流全量数据的笛卡尔积，200 个币种可能膨胀到几十 GB。而且 FR/OI 是低频更新（1h/8h），大部分时间 Flink 在扫描一堆没变化的数据。Fluss 的 PK Table 点查就高效得多——FR 没变就不需要处理。

---

**策略 2：Golden Funding 终结点检测**

这个更有意思。推特上有个策略叫'费率反转+OI'——当资金费率从极端负值回归正常，同时 OI 在高位增长，就是做空信号。crypto-resources.com 把它定义为'Golden Funding 结束后的做空'。

**技术需求**：不是简单的阈值判断，而是**状态变化检测**——'FR情绪 先到极端，再回归正常'。这是一个时序模式。Kafka 方案：需要 Flink 维护每个币种的 FR 历史状态，自己写状态机逻辑。Fluss 方案：用 Flink CEP（复杂事件处理）+ Fluss PK Table 做状态存储。CEP 检测'极端→正常'的模式转换，PK Table 存储 FR 最新值和变化方向。

**为什么 Kafka 做得痛苦**：**CEP 本身就需要状态**——Flink 要记住'这个币种 2 小时前 FR 是 -1.5%，现在是 -0.05%'。Kafka 的 append-only 模式意味着 Flink 要自己维护这个状态。Fluss PK Table 天然就是'每个币种的最新值'，Flink 只需要对比当前值和上一条值的差异就行，状态极小。

---

**策略 3：清算热力图实时构建**

推特上做妖币策略最厉害的一个人——加密韋驮——明确说过：'更好的顶底信号很可能和清算热力图直接相关。上方如果已经没有更多空单对手盘，庄就没动力再继续往上爆空。'

现在的清算热力图工具（Coinglass、MethodAlgo 的 Grim Reaper）都是用历史 OI 数据+假设杠杆分布来估算，延迟 5-15 分钟。

**技术需求**：实时 OI 变化 → 实时更新各价位的清算风险。Kafka 方案：OI 变化写 Kafka → Flink 消费+计算 → 结果写 Kafka → 消费者读结果。每步都有 Kafka 的全量传输开销。Fluss 方案：OI 变化实时写入 Fluss PK Table（symbol→最新OI），Flink Delta Join 点查标记价格，UDF 计算各杠杆清算价位，结果写 Fluss PK Table（symbol+price_bucket→清算风险），Dashboard 点查显示。

**为什么 Kafka 做得痛苦**：清算热力图需要**频繁读取最新值**（最新 OI、最新标记价格）+**频繁写入最新值**（各价位清算风险）。Kafka 不支持点查和 Upsert，所有最新值读取都要走 Flink 状态或 Redis。Fluss PK Table 天然就是做这个的——写入自动 Upsert，读取亚毫秒。

---

**策略 4：多源信号融合评分**

妖币雷达 V2 用 6 个维度的信号做综合评分：解锁风险、资金费率、KOL 关注数变化、声量 velocity、上所信息、K线模式。三层信号共振 = 高置信度入场。

**技术需求**：6 个数据源实时关联+评分计算。Kafka 方案：6 个 Kafka Topic → Flink 六路 Join → 巨大状态。Fluss 方案：5 个维度放 PK Table，K线做 Log Table，Flink Delta Join 点查 5 个 PK Table，SQL 内 CASE WHEN 计算评分，一次完成。

**为什么 Kafka 做得痛苦**：六路 Join 的状态是天文数字。而且大部分数据源是低频更新（上所信息一周可能只有 1-2 次，解锁数据更慢），Kafka 的全量广播模式意味着 Flink 每次处理事件都要扫描大量没变化的数据。Fluss 的 PK Table 点查只读最新值，不受数据量影响。

---

**我跑过的数据**：

我也用 Binance API 跑了 7 天的 Funding Rate 和 CEX-DEX 价差。几个发现：

- 80 个活跃合约里，10 个 7 天 Funding 年化超过 50%，最高 DAMUSDT 218%。但高 Funding 伴随高波动（24h 区间 108%），不一定可 carry
- RAVEUSDT 年化 144%、24h 区间 10.2%——风险收益形态更干净
- 负 Funding 更夸张：ORCAUSDT 7 天 -14.2%，年化 -741%。但反向 carry 需要 short spot，实际很难执行
- CEX-DEX 价差在主流币上几乎没有（ETH 0.017%）。真正值得做的是抓短窗口——大额 swap 打穿 DEX、清算潮后永续和现货偏离

这些数据的具体分析我已经整理了一份文档，可以发给你。"

### 【第五段 3 分钟：请他纠正我的猜测】

"以上是我基于行业经验和公开信息的推测。但我可能猜错了，所以想直接问你：

1. 你们的 Flink 作业，状态最大的有多大？双流 Join 是不是主要的状态消耗？
2. 有没有用 Redis 做 Flink 的状态缓存？如果有的话，数据一致性怎么处理？
3. Kafka Consumer 的带宽占用，3 万行/秒的情况下，有没有碰到过瓶颈？
4. 回测和实盘的数据一致性，你们怎么保证？快表慢表之间有偏差吗？
5. 你们有没有在尝试多源信号关联这类场景？如果有的话，现在怎么做？

这些问题的答案会决定 Fluss 在你们架构里的定位——是能解决一个痛点，还是暂时不需要。我更希望听到真实的反馈。"

### 【收尾 2 分钟：下一步建议】

"不管 Fluss 适不适合你们，我觉得有几个方向是可以深入聊的：

1. **如果双流 Join 状态是痛点**：我可以搭一个 Fluss + Flink 的 Demo，用你们场景的数据跑一个 Spread 信号实时生成，对比 Kafka+Flink 和 Fluss+Flink 的状态大小和资源消耗。2 周内出结果。迁移路径是并行的——Kafka 不停，Fluss 并行跑。
2. **如果带宽是痛点**：Fluss 的列裁剪可以量化对比——同一个 3 万行/秒的消费场景，Kafka 全量 vs Fluss 列裁剪的带宽差。
3. **如果多源信号关联是你们感兴趣的**：我可以做一个三信号 Short Squeeze 检测的 Demo，展示 Fluss Delta Join + Flink CEP 怎么做多源实时关联和状态变化检测。

当然，如果这些都不是痛点，那我也学到了真实的边界在哪里。"

---

## 关键话术备忘

### 要说的

- "Kafka 作为消息队列做得很好"——先承认
- "Fluss 是 Kafka 的超集，协议兼容"——降低切换恐惧
- "淘宝实测数据是……但你们场景可能不一样"——诚实
- "你们 Flink 的状态管理是怎么做的？"——理解他的痛点
- "我跑了一周 Binance 数据，发现了这些信号"——有实物
- "Fluss 不动你们的 Flink 和 ClickHouse，只替换 Kafka 层"——最小化变更范围

### 不要说的

- "Kafka 不适合实时场景"——他用了几年了
- "Fluss 可以替代你们的 Kafka + ClickHouse"——ClickHouse 不动
- "淘宝用了 Fluss 效果很好"——场景不同，需要他场景的验证
- "你们的数据架构有这些痛点"——你没看过他的系统
- "理论上 Fluss 的延迟是亚毫秒级"——他不关心理论，他关心实测

### 如果被问到你答不上来的

- "这个我确实没深入研究，但我可以回去查一下 Fluss 的文档和社区讨论"——诚实比编造好
- "你说的这个场景我之前没考虑到，能展开讲讲吗？"——把球踢回去
- "Fluss 确实还在孵化阶段，生产级案例主要在阿里体系内"——承认局限

---

## 架构角色速查

给数据同事的一张表，让他对齐理解：

| 层        | 当前                 | 提议                      | 变化                                        |
| --------- | -------------------- | ------------------------- | ------------------------------------------- |
| 流中间件  | Kafka                | Fluss                     | Kafka 协议兼容，新增 PK Table + 列裁剪      |
| 实时计算  | Flink                | Flink                     | **不动**，但状态从 RocksDB 迁到 Fluss |
| 实时状态  | Redis?               | Fluss PK Table            | 少维护一个系统                              |
| 历史存储  | ClickHouse 慢表      | Paimon                    | 通过 Fluss Tiering 自动归档                 |
| OLAP 查询 | ClickHouse           | ClickHouse                | **不动**                              |
| 回测      | ClickHouse 快表+慢表 | Fluss Union Read + Paimon | 一个 SQL，偏差 <2%                          |

**总变更：Kafka→Fluss，Redis→Fluss PK Table，其他不动。**

---

## 这次沟通的核心心态

Darren 是老板，他看方向和战略；数据同事是工程师，他看可行性和细节。

**Darren 关心的是"你能帮我赚多少钱"，数据同事关心的是"你能不能帮我解决具体的工程问题，而且别给我增加运维负担"。**

所以这次要：

- 少讲愿景，多讲工程
- 少讲 Fluss 特性，多讲他 Kafka+Flink 的痛点
- **先问他 Flink 的状态管理、Redis 的使用、Kafka 的带宽**
- 每个技术点都有他场景下的具体例子
- **强调"Fluss 替换 Kafka，Flink 和 ClickHouse 不动"**——最小化变更恐惧

**目标不是让他觉得 Fluss 很牛，而是让他觉得"这个人理解我的 Kafka+Flink 架构，而且有一个最小变更的升级路径"。**

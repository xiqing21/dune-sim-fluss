# 国网 AI+大数据平台设计文档

## 基于 Flink CDC → Fluss → Paimon 湖流一体架构与 Dune/Sim MCP/Skill 模式

---

## 报告摘要

本文档以 Dune 和 Sim 的实际 MCP 工具设计与 Skill CLI 命令模式为参考，结合国家电网业务场景，设计一套以 Flink CDC → Fluss（替换 Kafka）→ Paimon（湖流一体）为数据底座的 AI+大数据平台。核心设计包括：Fluss 三种表类型（Log Table / PrimaryKey Table / Log-Lakehouse Table）在国网5大数据域的映射；参考 Dune 12工具分层模式（数据发现→查询生命周期→可视化→账户管理）设计的国网 MCP Server 工具集；参考 Sim `dune sim evm <action>` 命名模式设计的 `sgc <domain> <action>` Skill CLI；以及与国网四安全区对齐的权限控制方案。

---

## 一、参考设计：Dune/Sim 实际 MCP 与 Skill 架构

### 1.1 Dune MCP 工具设计分析

Dune 通过单一远程 MCP 端点（`https://api.dune.com/mcp/v1`）暴露 12 个工具，按功能和成本自然分为四层：

| 层级 | 工具 | 操作类型 | 认证 | 计费 |
|------|------|---------|------|------|
| 数据发现 | searchDocs, searchTables, listBlockchains, searchTablesByContractAddress, getTableSize | 只读 | OAuth2/API Key | 无 |
| 查询生命周期 | createDuneQuery, getDuneQuery, updateDuneQuery, executeQueryById, getExecutionResults | 读写+执行 | 需认证 | executeQueryById 消耗信用点 |
| 可视化 | generateVisualization | 写入 | 需认证 | 无 |
| 账户管理 | getUsage | 只读 | 需认证 | 无 |

Dune 的关键设计模式：
- 工具粒度适中，不追求 CRUD 对齐，而是按用户工作流组织（发现→创建→执行→可视化）
- 高成本操作（executeQueryById）与低成本操作（searchTables）通过信用点计费隔离
- 速率限制按操作类型分层：读操作 40-1000 RPM，写操作 15-350 RPM，按订阅计划（Free/Plus/Enterprise）区分
- 认证双模：OAuth 2.0（浏览器环境）+ API Key（无浏览器环境）

### 1.2 Sim Skill CLI 设计分析

Sim 的 Skill CLI 采用 `dune sim <chain-type> <action>` 命名模式：

```
dune sim evm balances      — 多链代币余额（CU: N，N=链数）
dune sim evm activity      — 钱包活动流（CU: 3，固定）
dune sim evm transactions  — 交易历史（CU: 1，固定）
dune sim evm token-info    — 代币元数据（CU: 2，固定）
dune sim evm token-holders — 持有人排行（CU: 2，固定）
dune sim evm collectibles  — NFT持仓（CU: N）
dune sim evm defi-positions— DeFi持仓（CU: 10N，最昂贵）
dune sim evm stablecoins   — 稳定币余额
dune sim svm balances      — Solana余额（beta）
```

Sim 的关键设计模式：
- 命名空间按链类型（evm/svm）分层，动作（balances/activity/...）为末端
- CU 计费透明：固定型端点（1-3 CU）vs 链依赖型端点（N CU），DeFi 最贵（10N CU）
- 建议显式指定 chain_ids 控制成本，不依赖默认集合
- 警告系统：部分成功时 HTTP 200 + warnings 数组（不因部分失败报错）

### 1.3 Dune + Sim 的配合模式

DuneSQL（历史分析、跨表 JOIN、聚合统计）与 Sim（实时查询、单地址状态、即时返回）互补：

| 需求 | 选择 | 原因 |
|------|------|------|
| 钱包实时余额 | Sim | 即时返回，无需 SQL |
| 历史30天交易量趋势 | DuneSQL | Sim 不支持时序 |
| DeFi持仓快照 | Sim | 跨协议聚合 |
| 跨地址聚合分析 | DuneSQL | Sim 只查单地址 |

这一模式对应到国网：**Fluss 实时流 = Sim**（即时查询、当前状态），**Paimon 数据湖 = DuneSQL**（历史分析、复杂 JOIN）。

---

## 二、数据底座：Flink CDC → Fluss → Paimon 湖流一体架构

### 2.1 为什么用 Fluss 替换 Kafka

在国网实时数仓场景下，Kafka 存在核心痛点：

| Kafka 痛点 | Fluss 解决方案 | 国网收益 |
|------------|---------------|---------|
| 不支持更新 | PrimaryKey Table + KV 索引（RocksDB），原生支持 Upsert/Partial Update | 设备状态去重、电表最新读数、台账变更同步 |
| 无法数据探查 | 主键点查 + 前缀查询 + LIMIT/COUNT | AI Agent 实时探查数据，无需写 SQL |
| 数据回溯成本高 | 湖流一体：实时数据自动 Tiering 到 Paimon，Union Read 秒级回溯 | 历史故障复盘、负荷曲线回放 |
| 网络成本高（行存全量读取） | 列式存储（Arrow）+ 服务端列裁剪，节省 90% 网络带宽 | 数亿电表数据降低传输成本 |
| 双流 Join 状态膨胀 | Delta Join：将状态外置到 Fluss，无状态关联 | SCADA 实时数据与设备台账关联，状态减少 10 倍 |

### 2.2 Fluss 三种表类型与国网数据域映射

Fluss 提供三种表类型，需根据数据特征选择：

| 表类型 | 特点 | 适用条件 |
|--------|------|---------|
| **Log Table** | 仅追加，无主键，类似 Kafka Topic | 事件流、日志数据，无需更新 |
| **PrimaryKey Table** | 支持更新/删除，主键点查，Changelog | 需要去重、更新、点查的业务数据 |
| **Log-Lakehouse Table** | Log Table + 自动 Tiering 到 Paimon，Union Read | 需要秒级新鲜度 + 历史回溯的核心业务表 |

国网5大数据域的表类型选择：

**电网运行域（安全区Ⅰ/Ⅱ）**

| 表名 | 表类型 | 理由 |
|------|--------|------|
| scada_measurement | Log-Lakehouse | SCADA 量测数据，需实时监控+历史回溯 |
| pmu_phasor | Log Table | PMU 相量数据，纯追加，极高吞吐（100次/秒） |
| ems_dispatch_order | PrimaryKey Table | 调度指令需更新状态（待执行→已执行） |
| relay_protection_event | Log Table | 保护事件，纯追加 |
| load_forecast_result | PrimaryKey Table | 预测结果按区域+时间点更新 |

**设备监测域（安全区Ⅱ/Ⅲ）**

| 表名 | 表类型 | 理由 |
|------|--------|------|
| device_status | PrimaryKey Table | 设备实时状态，按设备ID更新（在线/离线/告警） |
| sensor_reading | Log-Lakehouse | 传感器读数，需实时+历史趋势分析 |
| fault_event | Log-Lakehouse | 故障事件流，需实时告警+历史故障分析 |
| inspection_record | Log Table | 巡检记录，纯追加 |
| device_registry | PrimaryKey Table | 设备台账（维表），Delta Join 的右侧维度表 |

**用电采集域（安全区Ⅲ）**

| 表名 | 表类型 | 理由 |
|------|--------|------|
| meter_reading | Log-Lakehouse | 电表读数，海量（数亿电表），需实时+历史 |
| load_curve | Log-Lakehouse | 负荷曲线，时序分析核心表 |
| power_quality | Log Table | 电能质量数据，纯追加 |
| meter_registry | PrimaryKey Table | 电表台账（维表），Delta Join 维度表 |

**营销服务域（安全区Ⅲ/Ⅳ）**

| 表名 | 表类型 | 理由 |
|------|--------|------|
| customer_bill | PrimaryKey Table | 电费账单，需更新状态（未缴→已缴） |
| service_ticket | PrimaryKey Table | 95598工单，需更新处理状态 |
| theft_suspect | PrimaryKey Table | 窃电嫌疑，需更新研判结果 |
| customer_profile | PrimaryKey Table | 客户画像（维表） |

**气象环境域（安全区Ⅳ）**

| 表名 | 表类型 | 理由 |
|------|--------|------|
| weather_forecast | PrimaryKey Table | 气象预报，按区域+时间更新 |
| weather_observation | Log Table | 气象观测，纯追加 |
| geological_alert | Log Table | 地质灾害预警，纯追加 |

### 2.3 端到端数据流

```
数据源层                          Flink CDC 采集层              Fluss 流存储层
┌──────────┐                    ┌──────────────┐           ┌──────────────────────┐
│ SCADA/EMS│ ──正向隔离穿越──→  │              │           │ 电网运行域            │
│ (Ⅰ区)    │                    │              │           │  scada_measurement    │
├──────────┤                    │ Flink CDC    │           │  (Log-Lakehouse)      │
│ PMS/在线 │ ──正向隔离穿越──→  │ Source       │──写入──→  │  ems_dispatch_order   │
│ 监测(Ⅱ区)│                    │ (MySQL/      │           │  (PrimaryKey)         │
├──────────┤                    │  Oracle CDC) │           ├──────────────────────┤
│ 采集终端 │ ──────────────────→│              │           │ 设备监测域            │
│ (Ⅲ区)    │                    │              │           │  device_status(PK)    │
├──────────┤                    │              │           │  device_registry(PK)  │
│ CRM/95598│ ──────────────────→│              │           ├──────────────────────┤
│ (Ⅲ/Ⅳ区) │                    │              │           │ 用电采集域            │
├──────────┤                    │              │           │  meter_reading        │
│ 气象站   │ ──────────────────→│              │           │  (Log-Lakehouse)      │
│ (Ⅳ区)    │                    └──────────────┘           │  meter_registry(PK)   │
└──────────┘                                                └──────────┬───────────┘
                                                                       │
                                                            Tiering Service
                                                            (Flink实现,分钟级)
                                                                       │
                                                                       ▼
                                                          Paimon 数据湖层
                                                          ┌──────────────────────┐
                                                          │ ODS: 原始数据镜像     │
                                                          │ DWD: Delta Join 宽表  │
                                                          │   SCADA + 设备台账    │
                                                          │   电表读数 + 台账      │
                                                          │ DWS: 聚合指标         │
                                                          │   负荷统计/线损率/     │
                                                          │   故障率/客户画像      │
                                                          │ ADS: 应用数据服务     │
                                                          └──────────┬───────────┘
                                                                     │
                                                          Union Read / 批查询
                                                                     │
                                                                     ▼
                                                          AI Agent 层 (MCP Server + Skill CLI)
```

### 2.4 Delta Join 在国网场景的应用

Delta Join 将传统双流 Join 的状态外置到 Fluss，实现无状态关联，资源降低 10 倍。国网典型场景：

**场景1：SCADA 量测 + 设备台账**

```sql
-- 传统方式：需要将 device_registry 全量存入 Flink 状态
-- Delta Join：通过 Fluss 点查获取设备信息，零状态

SELECT s.measurement_value, s.timestamp, d.device_name, d.voltage_level, d.substation
FROM scada_measurement s
INNER JOIN device_registry d  -- Fluss PrimaryKey Table, bucket.key = device_id
ON s.device_id = d.device_id;
```

**场景2：电表读数 + 用户台账**

```sql
SELECT m.reading_value, m.reading_time, c.customer_name, c.customer_type
FROM meter_reading m
INNER JOIN meter_registry c  -- Fluss PrimaryKey Table
ON m.meter_id = c.meter_id;
```

### 2.5 安全区穿越与 Fluss 部署

国网安全分区对 Fluss 集群部署有严格约束：

```
安全区Ⅰ/Ⅱ（生产控制大区）        安全区Ⅲ/Ⅳ（管理信息大区）
┌──────────────────┐             ┌──────────────────────┐
│ Fluss Cluster A  │             │ Fluss Cluster B      │
│ (Ⅰ/Ⅱ区数据)     │             │ (Ⅲ/Ⅳ区数据)          │
│                  │             │                      │
│ ·scada_meas     │  正向隔离   │ ·meter_reading       │
│ ·ems_dispatch   │ ──────────→ │ ·customer_bill       │
│ ·device_status  │  (只允许    │ ·service_ticket      │
│ ·pmu_phasor     │  Ⅰ→Ⅲ方向)  │ ·weather_forecast    │
└──────────────────┘             └──────────────────────┘
                                         │
                                         │ Union Read
                                         ▼
                                 Paimon 数据湖 (Ⅲ区)
                                 (Tiering 目标存储)
```

关键设计：
- 生产控制大区（Ⅰ/Ⅱ）部署独立 Fluss 集群，与管理信息大区物理隔离
- 通过正向隔离装置实现 Ⅰ/Ⅱ → Ⅲ 区的单向数据传输（Flink CDC Source 在Ⅰ/Ⅱ区读取，Sink 写入Ⅲ区 Fluss）
- 反向隔离装置仅允许Ⅲ → Ⅰ/Ⅱ区的有限控制指令回传
- MCP Server 部署在Ⅲ区，通过 OPA 策略引擎控制跨区访问

---

## 三、MCP Server 工具设计（参考 Dune 12 工具分层模式）

### 3.1 设计原则

参考 Dune 的工具分层（数据发现→查询生命周期→可视化→账户管理）和 Block 公司60+ MCP Server 的经验（自上而下、从工作流出发、减少链式调用），国网 MCP Server 工具设计遵循：

1. 按工作流组织，不按 CRUD 对齐
2. 高成本操作与低成本操作隔离
3. 工具总数控制在 12-15 个（参考六工具模式，但国网场景更复杂）
4. 只读工具与写入工具不混合（参考 Block 最佳实践）

### 3.2 工具分层设计

参考 Dune 的四层工具分层，映射到国网 Fluss + Paimon 架构：

**第一层：数据发现（只读，无需认证或低权限）**

| 工具 | 对标 Dune | 映射 Fluss/Paimon 能力 | 说明 |
|------|----------|----------------------|------|
| `searchCatalog` | searchTables | 查询 Fluss Catalog 元数据 | 按数据域/表名/分区搜索 |
| `listDomains` | listBlockchains | 枚举国网数据域及表统计 | 返回5大数据域的表数量 |
| `searchByDevice` | searchTablesByContractAddress | 按设备ID/电表号查关联表 | 模拟 Dune 按合约地址查表 |
| `getTableStats` | getTableSize | 估算 Fluss/Paimon 表数据量 | 参考 Dune 的数据扫描量估算 |

**第二层：实时查询（只读，需认证，对应 Sim 的即时查询模式）**

| 工具 | 对标 Sim | 映射 Fluss 能力 | 说明 |
|------|---------|----------------|------|
| `queryRealtime` | sim evm balances | Fluss 主键点查/前缀查询 | 设备状态、电表最新读数等 |
| `queryStream` | sim evm activity | Fluss 流式消费（latest模式） | 实时数据流浏览（LIMIT N） |
| `queryHistory` | — | Paimon 批查询 / Union Read | 历史数据查询，类似 DuneSQL |

**第三层：AI 分析（高成本，需认证+信用点）**

| 工具 | 对标 Dune | 映射能力 | 说明 |
|------|----------|---------|------|
| `runAnalysis` | executeQueryById | Flink SQL 提交执行 | 异步模式，返回 execution_id |
| `getAnalysisResult` | getExecutionResults | 获取 Flink 作业结果 | 轮询模式获取结果 |
| `predictLoad` | — | 负荷预测模型调用 | 封装专业模型，高信用点消耗 |
| `diagnoseFault` | — | 故障诊断模型调用 | 多模态输入（红外+振动+声纹） |

**第四层：资源管理与订阅**

| 工具 | 对标 Dune | 映射能力 | 说明 |
|------|----------|---------|------|
| `subscribeAlert` | — | Fluss 流式消费 + Webhook | 参考Sim的Webhook推送模式 |
| `getUsage` | getUsage | 查询信用点消耗 | 与 Dune 一致 |
| `saveQuery` | createDuneQuery | 保存常用查询 | 供后续复用 |

### 3.3 工具详细设计

#### `queryRealtime` — 实时点查（对标 Sim balances）

```
参数:
  table_name: string (必选) — Fluss 表名
  key: object (必选) — 主键值，如 {"device_id": "DEV_001"}
  columns: string[] (可选) — 需要返回的列，默认全部（列裁剪优化）

权限: 角色=业务用户+ | 数据域=参数指定 | 操作级=只读
信用点: 1 CU/次（固定，参考 Sim Transactions）

返回:
  row: object | null — 单行数据
  freshness_ms: int — 数据新鲜度（毫秒）

示例:
  queryRealtime("device_status", {"device_id": "DEV_001"}, ["status", "last_heartbeat"])
  → {"row": {"status": "online", "last_heartbeat": "2026-04-15T12:00:00"}, "freshness_ms": 50}
```

#### `queryStream` — 实时流浏览（对标 Sim activity）

```
参数:
  table_name: string (必选) — Fluss 表名
  startup_mode: "latest" | "earliest" | "timestamp" (默认 latest)
  limit: int (默认 20, 最大 100) — 返回条数
  columns: string[] (可选) — 列裁剪

权限: 角色=普通分析师+ | 数据域=参数指定 | 操作级=只读
信用点: 3 CU/次（参考 Sim Activity）

返回:
  rows: array — 数据行
  schema: array — 列元数据
  has_more: boolean

示例:
  queryStream("scada_measurement", "latest", 10, ["device_id", "value", "timestamp"])
  → {"rows": [...], "schema": [...], "has_more": true}
```

#### `queryHistory` — 历史数据查询（对标 DuneSQL）

```
参数:
  sql: string (必选) — Paimon/Flink SQL 查询
  data_domain: string (必选) — 数据域标识
  max_rows: int (默认 100, 最大 10000)
  timeout_seconds: int (默认 30)

权限: 角色=普通分析师+ | 数据域=参数指定 | 操作级=只读
信用点: 按扫描数据量计费（参考 Dune 动态计费）

安全: SQL 注入防护（参数化+白名单）、结果自动脱敏
返回:
  columns: array, rows: array, row_count: int, execution_time_ms: int, warnings: array

示例:
  queryHistory("SELECT date_trunc('hour', ts) as hour, avg(value) FROM scada_measurement WHERE device_id = 'DEV_001' AND ts >= TIMESTAMP '2026-04-14' GROUP BY 1", "dispatch")
```

#### `predictLoad` — 负荷预测（无 Dune 对标，国网专属）

```
参数:
  area_code: string (必选) — 区域编码
  forecast_type: "short_term" | "medium_term" | "long_term" (必选)
  time_horizon_hours: int (必选) — 预测时长
  model_version: string (可选) — 模型版本

权限: 角色=高级分析师+ | 数据域=电网运行 | 操作级=模型执行
信用点: 10 CU/次（参考 Sim DeFi Positions，最昂贵端点）

返回: 
  forecast_result: array — 预测值序列(MW)
  confidence_interval: array — 置信区间
  model_metrics: object — 模型评估指标

异步模式: 是（预测耗时10-60秒），返回 task_id，通过 getAnalysisResult 轮询
```

#### `subscribeAlert` — 告警订阅（对标 Sim Webhook）

```
参数:
  table_name: string (必选) — 监控的 Fluss 表
  alert_type: "threshold" | "anomaly" | "fault" (必选)
  filter: object (可选) — 过滤条件，如 {"device_id": "DEV_001", "value_gt": 220}
  callback_url: string (必选) — Webhook 回调地址

权限: 角色=高级分析师+ | 数据域=参数指定 | 操作级=写入
信用点: 2 CU/事件（参考 Sim Webhook 计费）

返回:
  subscription_id: string
  status: "active"
```

---

## 四、Skill CLI 设计（参考 Dune/Sim 命令模式）

### 4.1 命名规范

参考 Sim 的 `dune sim evm <action>` 三段式命名，国网采用 `sgc <domain> <action>`：

```
sgc          — 命名空间（State Grid Corporation）
  <domain>   — 数据域（dispatch/device/meter/market/weather/knowledge）
    <action> — 具体操作
```

对比：

| Dune/Sim | 国网 | 说明 |
|----------|------|------|
| `dune sim evm balances` | `sgc device status` | 设备状态=电力场景的"余额" |
| `dune sim evm activity` | `sgc dispatch stream` | 调度活动流=电力场景的"链上活动" |
| `dune sim evm defi-positions` | `sgc meter reading` | 电表读数=电力场景的"资产持仓" |
| `dune query run-sql` | `sgc data query` | SQL查询=通用分析 |
| `dune dataset search` | `sgc catalog search` | 数据目录搜索 |

### 4.2 完整命令列表

**调度域（sgc dispatch）**

```
sgc dispatch load-forecast    — 负荷预测（短期/中期/长期）
  --area-code <code>          (必选) 区域编码
  --type <short|medium|long>  (必选) 预测类型
  --horizon <hours>           (必选) 预测时长
  --model <version>           (可选) 模型版本

sgc dispatch stream           — 实时调度数据流浏览
  --limit <n>                 (默认20) 返回条数
  --from <timestamp>          (可选) 起始时间

sgc dispatch transfer-decision — 负荷转供决策
  --area-code <code>          (必选)
  --target-load <mw>          (必选) 目标负荷
```

**设备域（sgc device）**

```
sgc device status             — 设备实时状态（对标 sim evm balances，点查）
  --device-id <id>            (必选) 设备ID
  --columns <col1,col2>       (可选) 指定返回列

sgc device fault-diagnose     — 故障诊断
  --device-id <id>            (必选)
  --fault-type <type>         (可选) 故障类型
  --include-history           (可选) 包含历史故障

sgc device inspect            — 巡检记录查询
  --device-id <id>            (必选)
  --limit <n>                 (默认10)
```

**用电域（sgc meter）**

```
sgc meter reading             — 电表最新读数（对标 sim evm balances）
  --meter-id <id>             (必选) 电表号
  --columns <col1,col2>       (可选)

sgc meter load-curve          — 负荷曲线查询
  --meter-id <id>             (必选)
  --from <timestamp>          (必选) 起始时间
  --to <timestamp>            (必选) 截止时间
  --resolution <15m|1h|1d>   (默认1h) 时间粒度
```

**营销域（sgc market）**

```
sgc market bill-query         — 电费账单查询
  --customer-id <id>          (必选)
  --period <month>            (可选) 账期

sgc market theft-detect       — 窃电检测
  --area-code <code>          (必选)
  --suspect-level <high|medium|low> (可选)

sgc market customer-profile   — 客户画像
  --customer-id <id>          (必选)
```

**数据域（sgc data）**

```
sgc data query                — SQL 查询（对标 dune query run-sql）
  --sql <query>               (必选) SQL语句
  --domain <name>             (必选) 数据域
  --max-rows <n>              (默认100)

sgc data catalog              — 数据目录搜索（对标 dune dataset search）
  --keyword <term>            (必选) 搜索关键词
  --domain <name>             (可选) 限定数据域
  --type <log|pk|lakehouse>   (可选) 表类型过滤

sgc data stats                — 表数据量统计（对标 dune dataset search-by-contract）
  --table <name>              (必选) 表名
```

**知识域（sgc knowledge）**

```
sgc knowledge search          — 知识问答（对标 dune docs search）
  --query <question>          (必选) 查询问题
  --kb <regulation|standard|case> (可选) 知识库范围
  --top-k <n>                 (默认5)

sgc knowledge doc-retrieve    — 文档检索
  --doc-id <id>               (必选) 文档ID
```

**监控域（sgc monitor）**

```
sgc monitor alert-subscribe   — 告警订阅（对标 Sim Webhook）
  --table <name>              (必选) 监控表
  --type <threshold|anomaly|fault> (必选)
  --callback <url>            (必选) 回调地址

sgc monitor alert-list        — 当前告警列表
  --area-code <code>          (可选)
  --severity <level>          (可选)
  --limit <n>                 (默认20)

sgc monitor usage             — 信用点消耗（对标 dune usage）
  --period <month>            (可选) 查询周期
```

### 4.3 Skill 与 MCP 工具的映射

| Skill CLI | MCP 工具 | Fluss/Paimon 操作 |
|-----------|---------|------------------|
| `sgc device status` | queryRealtime | Fluss PrimaryKey Table 点查 |
| `sgc dispatch stream` | queryStream | Fluss Log Table 流式消费（latest） |
| `sgc data query` | queryHistory | Paimon 批查询 / Union Read |
| `sgc data catalog` | searchCatalog | Fluss Catalog 元数据查询 |
| `sgc dispatch load-forecast` | predictLoad | 模型服务调用 |
| `sgc monitor alert-subscribe` | subscribeAlert | Fluss 流消费 + Webhook |
| `sgc monitor usage` | getUsage | 账户管理 |

---

## 五、权限控制与安全分区对齐

### 5.1 三维权限模型

参考 Dune 的工具权限分层（只读→写入→执行）和 Sim 的 CU 计费，结合国网四安全区：

**角色维度**：

| 角色 | Skill 访问范围 | 典型用户 |
|------|---------------|---------|
| 系统管理员 | 全部 + 配置管理 | 信息中心 |
| 调度专工 | dispatch/device/monitor (Ⅰ/Ⅱ区) | 调度中心 |
| 运维工程师 | device/meter/data (Ⅱ/Ⅲ区) | 检修公司 |
| 业务人员 | market/knowledge (Ⅲ/Ⅳ区) | 客服中心 |
| 外部用户 | data(脱敏)/knowledge (Ⅳ区) | 数据要素消费者 |

**数据域→安全区映射**：

| 数据域 | 安全区 | Fluss 集群 | MCP 访问策略 |
|--------|--------|-----------|-------------|
| 电网运行 | Ⅰ/Ⅱ | Cluster A (生产控制区) | 仅 dispatch/device Skill 可访问，VPN+纵向加密 |
| 设备监测 | Ⅱ/Ⅲ | Cluster A + Cluster B | 需审批跨区访问 |
| 用电采集 | Ⅲ | Cluster B (管理信息区) | 中等管控，脱敏后可共享 |
| 营销服务 | Ⅲ/Ⅳ | Cluster B | 个人信息脱敏 |
| 气象环境 | Ⅳ | Cluster B | 常规管控 |

**操作级→计费**：

| 操作级 | Skill 示例 | 信用点 | 速率限制 |
|--------|-----------|--------|---------|
| 只读点查 | sgc device status | 1 CU | 200 RPM |
| 流浏览 | sgc dispatch stream | 3 CU | 100 RPM |
| 历史查询 | sgc data query | 按扫描量 | 70 RPM |
| 模型执行 | sgc dispatch load-forecast | 10 CU | 30 RPM |
| 告警订阅 | sgc monitor alert-subscribe | 2 CU/事件 | 20 RPM |

### 5.2 认证与鉴权

```
用户 → SG-IAM统一认证(SAML/OIDC) → RBAC角色映射
    → OPA策略引擎(数据域权限 + 安全区约束)
    → MCP Server工具过滤
    → Fluss/Paimon 行列级脱敏
    → 全链路审计日志
```

### 5.3 错误处理（参考 Dune/Sim 模式）

| 错误码 | 含义 | 处理建议 | 参考来源 |
|--------|------|---------|---------|
| 200 | 成功（可能含 warnings） | 检查 warnings 数组 | Sim 部分成功模式 |
| 400 | 参数错误 | 检查参数格式 | Dune 标准 |
| 401 | 未认证 | 确认 SG-IAM Token | Dune 标准 |
| 402 | 信用点不足 | 联系管理员充值 | Dune 信用点 |
| 403 | 权限不足/安全区越界 | 检查角色-数据域-安全区权限 | 国网特有 |
| 404 | 资源不存在（设备/表不存在） | 确认 device_id / table_name | Dune 标准 |
| 429 | 频率超限 | 指数退避重试（2s→4s→8s） | Dune 速率限制 |
| 安全区越界 | 跨安全区访问被拒 | 使用正向隔离穿越机制申请 | 国网特有 |
| Fluss 不可用 | 流存储集群故障 | 短暂延迟后重试，降级到 Paimon 查询 | Fluss 特有 |

---

## 六、AI 能力集成

### 6.1 大模型 + 专业模型混合架构

```
┌─────────────────────────────────────┐
│ 通用大模型层                          │
│ 光明电力大模型(千亿) / DeepSeek       │
│ 自然语言理解、逻辑推理、多模态         │
├─────────────────────────────────────┤
│ 专业模型层                            │
│ DeepSeek-Load: 负荷预测(误差2.3%)    │
│ DeepSeek-OM: 设备运维(提前47天预警)   │
│ YOLO/CNN: 缺陷识别(91.24%准确率)     │
├─────────────────────────────────────┤
│ 模型服务层                            │
│ vLLM推理引擎 + K8s弹性伸缩           │
│ 模型版本管理 + A/B测试 + 漂移检测     │
└─────────────────────────────────────┘
```

### 6.2 RAG 知识库

| 层次 | 技术选型 | 说明 |
|------|---------|------|
| 数据接入 | OCR+版面分析+智能切片 | 规程、标准、工单、案例文档 |
| 知识存储 | Milvus(向量) + 知识图谱 + 结构化存储 | 混合检索：向量+关键词+图谱 |
| 推理执行 | 光明电力大模型 | Agentic RAG：多步检索、迭代推理 |
| 接口服务 | sgc knowledge * | 支持多轮对话、来源追溯 |

### 6.3 MCP Server 分领域设计

| MCP Server | 工具数 | 部署安全区 | Fluss 集群 |
|------------|--------|-----------|-----------|
| 调度MCP | 3 (load-forecast, stream, transfer-decision) | Ⅲ区（通过正向隔离访问Ⅰ/Ⅱ区数据） | Cluster A |
| 设备MCP | 3 (status, fault-diagnose, inspect) | Ⅲ区 | Cluster A + B |
| 用电MCP | 2 (reading, load-curve) | Ⅲ区 | Cluster B |
| 营销MCP | 3 (bill-query, theft-detect, customer-profile) | Ⅲ/Ⅳ区 | Cluster B |
| 数据MCP | 3 (query, catalog, stats) | Ⅲ区 | Cluster B |
| 知识MCP | 2 (search, doc-retrieve) | Ⅲ区 | Cluster B |
| 监控MCP | 3 (alert-subscribe, alert-list, usage) | Ⅲ区 | Cluster A + B |

---

## 七、实施路线图

### 第一阶段：数据底座 + 基础工具（6个月）

- 部署 Flink CDC + Fluss + Paimon 湖流一体底座
- 完成5大数据域的 Fluss 表建模（Log/PrimaryKey/Log-Lakehouse 选择）
- 实现 Delta Join 宽表（SCADA+设备台账、电表+用户台账）
- 开发数据MCP（searchCatalog, queryRealtime, queryHistory）+ 对应 Skill CLI
- 完成 SG-IAM 认证集成 + OPA 策略引擎

### 第二阶段：核心场景落地（6个月）

- 调度MCP 开发（负荷预测 Skill + 实时流 Skill）
- 设备MCP 开发（状态点查 + 故障诊断）
- 监控MCP 开发（告警订阅 + 实时告警列表）
- "角色-数据域-安全区"权限体系全面上线
- 安全区穿越机制联调测试

### 第三阶段：生态扩展（6个月）

- 营销MCP（窃电检测、客户画像）
- 知识MCP（RAG 知识问答）
- 光明电力大模型 + 专业模型集成
- Skill 市场化：开放第三方 Skill 开发
- 数据要素流通：隐私计算 + 联邦学习

---

## 八、局限性

- 国网核心系统架构细节属企业敏感信息，本文基于公开资料推导，实际架构可能更复杂
- Fluss 作为 Apache Incubating 项目，部分特性（如 Prefix Lookup Join）仍在演进
- 安全区穿越机制（正向/反向隔离）对 MCP Server 的网络部署有严格约束，生产部署需专项设计
- Delta Join 在 Flink 2.1 仅支持 INNER JOIN，2.2 扩展了 CDC 源，LEFT JOIN 尚未支持
- 信用点计费模型需结合国网内部核算体系定制，不能直接照搬 Dune/Sim 的商业化计费

---

## References

1. [Dune MCP - Dune Docs](https://docs.dune.com/api-reference/agents/mcp)
2. [Credit System - Dune Docs](https://docs.dune.com/learning/how-tos/credit-system)
3. [Rate Limits - Dune Docs](https://docs.dune.com/api-reference/overview/rate-limits)
4. [Compute Units - Sim by Dune](https://docs.sim.dune.com/compute-units)
5. [Sim API Documentation](https://docs.sim.dune.com/)
6. [Fluss湖流一体列式流存储赋能Flink实时分析 - 阿里云](https://developer.aliyun.com/article/1644885)
7. [湖流一体：基于 Fluss+ Paimon 的实时湖仓数据底座 - 阿里云](https://developer.aliyun.com/article/1709004)
8. [Fluss 湖流一体：Lakehouse 架构实时化演进 - InfoQ](https://www.infoq.cn/article/o2F4ZfMBZOEA4Rg4Tl46)
9. [Flink Delta Joins - Apache Fluss](https://fluss.apache.org/docs/engine-flink/delta-joins/)
10. [Delta Join：为超大规模流处理实现计算与历史数据解耦 - SegmentFault](https://segmentfault.com/a/1190000047434418)
11. [Primary Key Tables: Unifying Log and Cache for Streaming - Apache Fluss](https://fluss.apache.org/blog/pk-key-tables-log-cache-streaming/)
12. [The Six-Tool Pattern: MCP Server Design That Scales](https://www.mcpbundles.com/blog/mcp-tool-design-pattern)
13. [Block's Playbook for Designing MCP Servers](https://engineering.block.xyz/blog/blocks-playbook-for-designing-mcp-servers)
14. [电力监控系统安全防护规定 - 国家能源局](https://zfxxgk.ndrc.gov.cn/upload/images/202411/20241111172728.pdf)
15. [电网安全区划分详解](https://www.cnblogs.com/xiangdongzhang/p/14965839.html)
16. [基于Fluss的流式湖仓架构 - 博客园](https://www.cnblogs.com/bigdata1024/p/18675266)
17. [Fluss主键表数据查询 - 阿里云](https://help.aliyun.com/zh/flink/realtime-fluss/user-guide/primary-key-tables)
18. [国家电网数据中台 - 帆软](https://www.fanruan.com/blog/article/642977/)
19. [国网信通：电网企业数据中台建设与运营方案](https://www.sohu.com/a/800806101_121740962)
20. [光明电力大模型 - 新华网](http://www3.xinhuanet.com/energy/20241219/caa68c8595384ca0b4a49ae5bdc34a22/c.html)
21. [电力"人工智能+"白皮书解读 - CSDN](https://blog.csdn.net/zkrb777/article/details/148056579)
22. [DeepSeek赋能电网 - 百度开发者中心](https://developer.baidu.com/article/detail.html?id=4899453)
23. [Paimon流式湖仓架构方案 - 阿里云](https://www.alibabacloud.com/help/zh/flink/realtime-flink/use-cases/scheme-of-streaming-lakehouse-based-on-paimon/)
24. [Flink + Fluss + Paimon 构建实时湖仓一体化实践 - CSDN](https://blog.csdn.net/weixin_29054399/article/details/159570323)
25. [Tool-level security for remote MCP servers](https://memo.d.foundation/ai/tool-level-security-for-remote-mcp-servers)

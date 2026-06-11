# Datus OSI 设计与使用

`datus-semantic-osi` 让 OSI(Open Semantic Interchange)成为指标的 source of truth。核心边界一句话:

> **你(或 LLM)只描述业务语义,执行引擎语法由 Datus 编译器生成。**

你写 OSI core metric expression;`measure_proxy`、`type_params`、`measures` 这些 MetricFlow 语法,编译器自己推断、补齐、生成。

---

## 1. 设计动机

直接 "OSI → 各执行后端" 有两个问题:每加一个后端(MetricFlow、原生 SQL、Snowflake…)就要重写一遍转换;校验逻辑也无处统一安放。

解决办法是中间插一层稳定契约 —— **Datus Semantic IR**,把链路切成两段:

```
authoring          编译期                          执行期
─────────   ──────────────────────   ──────────────────────────
OSI YAML ─▶ Datus Semantic IR ─▶ Backend lowering ─▶ MetricFlow YAML / 原生 SQL / …
(业务语义)   (后端无关、可校验)        (各后端各自实现)
```

- 前端只管 "翻到 IR",后端只管 "从 IR 翻出去",互不影响。
- 校验只在 IR 上做一次;加新后端只实现 `IR → 它的产物`,不碰 OSI 与 LLM。

---

## 2. 分层架构

| 层 | 职责 |
|---|---|
| **OSI core authoring** | 用户/LLM 写的严格 OSI core schema:`version` + `semantic_model[]`、datasets、fields、relationships、metrics(+ Datus `custom_extensions`) |
| **Datus executable profile** | OSI core 转换后的可执行子集 + 校验规则(单语义模型、source 可映射、字段为 row-level、关系仅 m:1/1:1、歧义即报错) |
| **Datus OSI Compiler** | 用 sqlglot 解析指标表达式 → 推断 measure / 指标类型 → 生成 IR;只在能安全推断时推断,否则返回业务语义错误 |
| **Datus Semantic IR** | 稳定、结构化、后端无关的执行语义(见 §4) |
| **Backend lowering** | 先做能力校验,再把 IR 降到具体产物;默认 `MetricFlowBackend`,另有 `DatusNativeBackend`(IR→SQL) |
| **Execution backend** | 实现 `list_metrics / get_dimensions / query_metrics / validate_semantic` |

---

## 3. 关键设计决策

**LLM 不写 measure,编译器推断 measure。** 这是最重要的边界。OSI 里只有业务表达式,编译器解析后自动抽取 backing measures、判定指标类型(aggregate / expression / ratio / cumulative / derived),并把表达式改写成基于 measure 名的形式。

**业务过滤建模为 filtered dataset。** 像"新品活动"这种业务过滤,不塞进后端的 constraint 语法,而是建成一个带 `filters` 的逻辑 dataset,再在其上定义聚合指标。既贴近 OSI 语义,又天然通过各后端校验。

**错误是业务语义,不是后端语法。** 缺少信息时,编译器返回"请声明 ratio 的分子/分母 / 累计窗口 / 时间维度"这类业务提示,而不是 `MetricFlow expr requires type_params.measures` 这种引擎细节。

**后端可替换,且 lower 前做能力校验。** 每个后端声明 capabilities(支持哪些指标类型、join、时间粒度、dry-run 形式)。IR 在 lowering 前先比对能力,不支持就提前报错,而不是生成出非法产物。

**生成格式可配置,且不破坏旧路径。** 把语义适配器配成 `osi`,LLM 就产出 OSI、校验走 OSI 编译器;配回默认的 `metricflow`,则保持原 MetricFlow 生成与校验。两条路径并存、互不影响——OSI 模式用独立 prompt 模板,不改动既有模板。

---

## 4. Datus Semantic IR 是什么

IR = Intermediate Representation(编译器术语,指输入与目标之间那层中立的结构化表示)。Datus Semantic IR 是它在指标体系里的实现 —— 一组 Pydantic 对象,长期稳定、不绑定任何执行引擎。

核心对象:

```
SemanticModelIR
  datasets:      DatasetIR[]      # name, source(table|query), fields, identifiers, filters, primary_time_dimension
  relationships: RelationshipIR[] # many_to_one / one_to_one 的 join 路径
  metrics:       MetricIR[]       # kind, measures(编译器抽取), expression, numerator/denominator,
                                  # inputs(derived 引用的其它指标), filters, window, time_dimension, …
```

它是内存结构,也可序列化成 JSON(`datus-osi compile --ir` 的输出),但用户不直接编辑 IR —— 只编辑 OSI source。golden 测试把 IR 序列化存档,锁定编译行为不漂移。

> `lower`(lowering)也是编译器术语:把高层、抽象的表示降到更贴近执行目标的低层形式。IR → MetricFlow YAML 就是一次 lowering。

---

## 5. 指标类型与边界

| OSI 写法 | 推断结果 |
|---|---|
| `SUM(x)` / `COUNT(DISTINCT x)` / `AVG(x)` | aggregate(自动抽 1 个 measure) |
| `SUM(a) / COUNT(b)` | ratio(抽 2 个 measure) |
| `SUM(a) - SUM(b)` | expression(抽 measure + 改写表达式) |
| OSI expression + DATUS `window` hint | cumulative |
| DATUS `metric_kind: derived` + 引用其它指标名 | derived |

**不是指标**(会报业务错误,建议建成 dataset/view):明细列表(`SELECT DISTINCT …`)、窗口/排名(`RANK() / ROW_NUMBER() OVER`)。

最小 authoring 示例:

```yaml
version: 0.2.0.dev0
semantic_model:
  - name: shop
    datasets:
      - name: paid_orders
        source: orders
        primary_key: [order_id]
        fields:
          - name: order_date
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: order_date
            dimension: {is_time: true}
            custom_extensions:
              - vendor_name: DATUS
                data: '{"type":"time","time_granularity":"day"}'
        custom_extensions:
          - vendor_name: DATUS
            data: '{"filters":[{"expression":"status = ''paid''","scope":"dataset"}]}'
    metrics:
      - name: paid_order_count
        description: "已支付订单数"
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: "COUNT(DISTINCT order_id)"
        custom_extensions:
          - vendor_name: DATUS
            data: '{"dataset":"paid_orders","time_dimension":"order_date"}'
```

业务 hints 只能放进 OSI 原生的 `custom_extensions`(`vendor_name: DATUS`, `data` 为 JSON object string)。不要把 `dataset` / `time_dimension` / `window` / `numerator` / `format` 等 Datus 字段写成 OSI core 顶层字段。

---

## 6. 试用入口

三种由浅入深的验证方式:

1. **跑测试套件**(最快,含 baisheng 实连 StarRocks 的端到端用例)
   `uv run pytest datus-semantic-osi/tests/`

2. **CLI**:手写一份 OSI,编译成 IR + MetricFlow 产物
   `datus-osi compile --input model.yaml --output out/ --ir ir.json`
   迁移旧 MetricFlow YAML:`datus-osi migrate --input legacy.yaml --output osi.yaml`

3. **真实 LLM 生成**:在 Datus-agent 把 `services.semantic_layer` 配成 `osi`,用 `gen_metrics` 子代理跑一次,产物即 OSI 格式(无 `measure_proxy`/`type_params`)。

执行后端二选一:
- `metricflow`(默认)—— 连数仓做完整校验 + 真实查询,需装 `datus-semantic-metricflow`;
- `native` —— 直接生成 SQL,DB-free 做结构校验 + dry-run,无需 MetricFlow。

每种指标类型与执行后端的端到端用法见 `tests/` 下的用例(golden、`integration/`、`unit/`)。

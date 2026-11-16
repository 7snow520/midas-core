# 变更记录

记录「做了什么、什么时候做的、为什么这么做」。格式参考 Keep a Changelog，版本号遵循 SemVer（0.x 表示接口尚未冻结）。

| 版本 | 日期 | 参与方 | 说明 |
|---|---|---|---|
| 0.1.0 | 2026-07-15 | 本人独立完成 | Forage 课程的原始实现，手写并跑通 |
| 0.2.0 | 2026-11-16 | 本人提出需求与取舍，AI 协助生成改动 | 工程加固：金额精度、并发扣款、幂等、失败审计、可观测性 |

## [Unreleased]

（暂无）

## [0.2.0] - 2026-11-16

背景：0.1.0 是 Forage 课程的实现，目的是演示链路跑通；本次按真实金融系统的要求做工程加固。

### Added
- `TransactionService`：交易处理服务层，负责事务边界、幂等查重、失败留痕
- `TransactionOutcome` 枚举：显式表达 `SUCCESS` / `DUPLICATE` / `INVALID_AMOUNT` / `SENDER_NOT_FOUND` / `INSUFFICIENT_FUNDS` 等结果，替代原先的字符串判断
- `TransactionStatus`：流水状态（`SUCCESS` / `FAILED`）
- `TransactionRecord`：新增 `txId`（唯一约束）、`failReason`、`createdAt`
- `HttpClientConfig`：`RestTemplate` 连接 / 读超时（默认 2s / 3s，可配置）
- `KafkaConsumerConfig`：`DefaultErrorHandler` + `DeadLetterPublishingRecoverer`，重试 2 次后进 `transactions.DLT`
- `ApiExceptionHandler`：参数校验失败返回 400 + RFC 9457 `ProblemDetail`
- 分环境配置 `application-dev.yml` / `application-prod.yml`；`db/migration/V1__init.sql`（Flyway）
- `Dockerfile`、`docker-compose.yml`（Kafka + 激励服务 + 应用）
- `.github/workflows/ci.yml`：push 到 main / 提 PR 时执行 `mvn -B verify`
- 自动化测试：`TransactionServiceTest`（9）、`KafkaEndToEndTest`（3）、`ConcurrentTransferTest`（2）

### Changed
- 金额由 `float` 改为 `BigDecimal`，数据库列改为 `NUMERIC(19,4)`
- 扣款由「读-改-写」改为带条件的原子 `UPDATE ... WHERE balance >= :amount`
- 激励服务返回 `Optional<BigDecimal>`，地址与超时外部化；调用失败按 0 处理，不阻断主链路
- 激励 HTTP 调用移出事务：原来夹在 debit / credit 之间，会把发送方的行锁和数据库连接占满整个 RPC 时长
- `/balance` 用户不存在返回 404（原来返回 0），非法参数返回 400
- `Task*Tests` 默认不参与构建（无断言的闯关验证器），需要时用 `-Pforage-verifier`
- 配置文件从仓库根目录移入 `src/main/resources`：原先不在 classpath，只有从仓库根目录启动才读得到，进容器或换目录就丢配置

### Fixed
- `UserRecord.toString()` 缺结尾的 `]`
- `JsonDeserializer` 依赖 `__TypeId__` 头：外部系统投递裸 JSON 无法消费，且 `SerializationException` 无法被 `DefaultErrorHandler` 处理，导致同一条消息无限重试、分区卡死、死信也兜不住（现由 `ErrorHandlingDeserializer` + 默认类型 + `use.type.headers=false` 兜底）
- 同一 `txId` 并发投递时，后到的线程抛 `DataIntegrityViolationException`（现转为 `DUPLICATE` 返回，不再把异常抛给监听器触发无意义重试）
- 清理 yml 重复 key、pom 中的死 properties、生产环境误用 `ddl-auto: create-drop`、主键策略不一致
- 拆分提交期间存在「中间提交无法构建」的问题，最终状态已可构建

### Removed
- `DatabaseConduit`：业务逻辑收敛到 `TransactionService`

### 验证
- `mvn -B verify`：14 个测试全绿（9 业务 + 3 Kafka 端到端 + 2 并发）
- 真实 PostgreSQL 14：Flyway 迁移 + Hibernate `validate` 通过，`/balance` 的 200 / 404 / 400 实测正确，并发测试在 PG 上同样通过
- 嵌入式 Kafka（进程内真实 broker）：消息消费落库、外部裸 JSON 可消费、坏消息进 `transactions.DLT` 且不阻塞分区
- 手工：真实激励服务 HTTP 链路（`java -jar services/transaction-incentive-api.jar --server.port=18081`）

### 未覆盖
- Docker 镜像未实际构建（开发机无 Docker 环境）
- 多分区 / 多消费者组未测
- 未做长时间压测与故障注入（下游超时抖动、Kafka 抖动）

## [0.1.0] - 2026-07-15

由本人独立完成（7 月按 Forage 课程任务要求手写实现并跑通），**无 AI 参与**。

### Added
- Kafka 消费交易消息、三道校验（账户存在性 / 余额充足性）、调用激励 API、更新余额、`/balance` 查询
- 5 个 `Task*Tests` 闯关验证器（打印日志人工核对，无断言）

## 关于 AI 参与的说明

适用范围只说 0.2.0，不涉及 0.1.0：

- **0.1.0（2026-07，本人手写）**：无 AI 参与。代码、配置、README 全部由本人独立完成。
- **0.2.0（2026-11-16）**：由 AI 助手（豆包）按本人提出的需求生成改动，本人决定范围与取舍
  （P0 + P1 + P2 全量、激励调用移出事务、金额超 4 位小数拒绝、最终直接同步到 main），
  并对结果负责。
- 验证由本人在自己的环境实际执行（本地 Maven 构建、真实 PostgreSQL 14、真实激励服务进程、
  `@EmbeddedKafka` 真实 broker），不是「只生成代码、没有运行」。
- 如实记录过程：期间出现过一次括号缺失导致 CI 失败、以及中间提交不可构建的问题，
  均由本人定位并修复，已记入上方 `Fixed`。

## 维护约定

1. 每次改动在 `[Unreleased]` 下补一条，格式：`- 日期 类型 内容（验证方式）`；
2. 只记「行为变化」与「关键决策」，不逐行记录代码；
3. 发版时把 `[Unreleased]` 提升为版本号并写日期；
4. 若改动由 AI 生成，条目标注 `[AI]` 并写清验证方式；
5. 已知限制与未覆盖项随版本一起更新，避免文档比代码乐观。

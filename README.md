# Midas Core — 金融交易处理系统

> 基于 Spring Boot + Kafka + JPA 的金融交易处理核心服务：交易校验、外部激励 API 集成、余额查询全链路。

## 项目背景

本项目最初是 JPMC Software Engineering Virtual Experience（Forage）的模拟后端项目，随后按真实金融系统的要求做了工程加固：金额精度、并发扣款、消息幂等、失败可审计、异常隔离。

链路：Kafka 消费交易消息 → 校验（金额/账户/余额）→ 原子扣款 → 调用激励 API → 入账 → 落库流水 → REST 查询余额。

## 技术栈

| 类别 | 技术 | 版本 |
|------|------|------|
| 语言 | Java | 17 |
| 框架 | Spring Boot | 3.2.5 |
| 消息队列 | Spring Kafka | 3.1.4 |
| 持久化 | Spring Data JPA + H2 / PostgreSQL | 3.2.5 / 2.2.224 |
| 迁移 | Flyway | Boot 托管 |
| 测试 | JUnit 5 + Mockito + Embedded Kafka | - |
| 构建 | Maven | 3.9+ |

## 关键设计决策

### 1. 金额用 `BigDecimal`，不用 `float`
所有金额字段（余额、交易额、激励）统一 `BigDecimal` + 数据库 `NUMERIC(19,4)`。`float` 是二进制浮点，`0.1 + 0.1` 不等于 `0.2`，对账必然出尾差。测试 `decimalAmountsHaveNoFloatingPointDrift` 专门守这条线。

另外**不做静默舍入**：金额超过 4 位小数直接拒绝，而不是四舍五入——金融系统里悄悄改金额比报错更危险。

### 2. 扣款是数据库原子操作，不是「读-改-写」
```java
@Query("update UserRecord u set u.balance = u.balance - :amount " +
       "where u.id = :id and u.balance >= :amount")
int debit(long id, BigDecimal amount);
```
把「查余额 → 判断够不够 → 写回」压成一条带条件的 UPDATE，由数据库保证原子性，影响行数为 0 即余额不足。原实现在并发下两笔交易会同时通过余额校验，造成透支（double spend）。

### 3. 幂等：`txId` + 唯一约束
Kafka 是「至少一次」投递，同一条消息可能被重复消费。每笔交易带唯一业务流水号 `txId`（消息未携带时服务端生成 UUID 并告警），消费前查 `existsByTxId`，表上再加唯一约束兜底并发窗口。

### 4. 一次交易 = 一个事务
扣款、入账、写流水在同一个事务边界内（`TransactionTemplate`），任一失败整体回滚，不会出现「扣了钱没加钱」。

### 5. 失败交易同样落库
被拒绝的交易（余额不足、账户不存在、金额非法）落一条 `status=FAILED` 的流水并记录 `fail_reason`。金融系统里「没有记录」比「错误记录」更危险——对账和审计都依赖这份流水。

> 因此 `TransactionRecord` 保存的是原始 `senderId` / `recipientId`，而不是 `@ManyToOne` 外键关联：交易被拒时用户可能根本不存在，外键约束会让这类流水写不进去。

### 6. 激励服务是「可失败的附加收益」
- 地址、连接超时、读超时全部外部化配置（默认 2s / 3s），避免下游卡死拖垮消费线程；
- 返回 `Optional<BigDecimal>`，明确区分「激励为 0」和「调用失败」；
- 调用失败按 0 处理，不阻断主交易链路。

### 7. 外部调用刻意放在事务之外
激励服务是 HTTP 调用（超时 3s）。如果放在 `debit` 与 `credit` 之间，发送方那一行会被锁住整个 RPC 时长，数据库连接也一起被占着——下游一抖动，消费吞吐立刻塌。所以顺序是「事务外取激励 → 事务内只做数据库写」，代价是少数被拒交易会多调一次只读接口。

### 8. Kafka 消费有死信兜底
`DefaultErrorHandler` + `DeadLetterPublishingRecoverer`：重试 2 次（1s 间隔）仍失败则投递到 `transactions.DLT`，防止一条毒消息堵死分区。业务校验失败不走异常路径，只在日志里记录结果，避免无意义重试。

## 项目结构

```
src/main/java/com/jpmc/midascore/
├── MidasCoreApplication.java
├── BalanceController.java          # GET /balance
├── handler/ApiExceptionHandler.java # 参数校验失败 → 400 ProblemDetail
├── component/
│   ├── TransactionListener.java    # Kafka 监听器
│   └── IncentiveService.java       # 激励 API 客户端
├── config/
│   ├── HttpClientConfig.java       # RestTemplate 超时
│   └── KafkaConsumerConfig.java    # 重试 + 死信
├── service/
│   ├── TransactionService.java     # 交易处理核心（事务边界）
│   └── TransactionOutcome.java     # 处理结果枚举
├── entity/                         # UserRecord / TransactionRecord / TransactionStatus
├── foundation/                     # Transaction / Balance / Incentive DTO
└── repository/                     # 原子扣款与入账
```

## 快速开始

### 方式一：Docker Compose（Kafka + 激励服务 + 应用）

```bash
mvn -DskipTests package
docker compose up --build
```

### 方式二：本地

```bash
# 1. 起 Kafka（默认 localhost:9092）
# 2. 起激励服务（默认 localhost:8080）
java -jar services/transaction-incentive-api.jar
# 3. 起应用
mvn spring-boot:run
```

查询余额：`curl "http://localhost:33400/balance?userId=1"`，用户不存在返回 404（而不是余额 0）。

## 配置

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `SPRING_PROFILES_ACTIVE` | `dev` | `dev`=H2 内存库；`prod`=PostgreSQL + Flyway |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka 地址 |
| `KAFKA_TOPIC` | `transactions` | 交易 topic |
| `INCENTIVE_BASE_URL` | `http://localhost:8080/incentive` | 激励服务地址 |
| `SERVER_PORT` | `33400` | HTTP 端口 |
| `DB_URL` / `DB_USERNAME` / `DB_PASSWORD` | - | `prod` 环境数据库 |

## 测试

```bash
mvn test                 # 断言式测试，9 个用例覆盖成功与全部拒绝路径
mvn test -Pforage-verifier   # Forage 的 Task*Tests 闯关验证器（仅打印结果，无断言）
```

`TaskOneTests`~`TaskFiveTests` 是原课程提供的验证脚本，靠打印日志人工核对、没有断言，因此默认不参与构建，需要时用 `-Pforage-verifier` 单独跑。

## 已知限制

- `services/transaction-incentive-api.jar` 是课程提供的二进制件，仍随仓库分发（要从 git 历史里彻底移除需要重写历史，暂未处理）；
- 接口无鉴权，`/balance` 任何人可查任意账户，生产需接入统一认证网关；
- 交易流水只写不读，缺少 `/transactions` 查询接口与按时间维度对账的报表查询。

# 项目说明文档

## 版本演进

| 版本 | 时间 | 这一版做了什么 | 参与方式 |
|---|---|---|---|
| `0.1.0` | 2026-07-15 | Forage 课程版：Kafka 消费 → 三道校验 → 激励 API → 更新余额 → `/balance` | 本人独立完成 |
| `0.2.0` | 2026-11-16 | 工程加固：金额精度、并发扣款、消息幂等、失败交易留痕、外部调用隔离、死信兜底 | 本人提出需求与取舍，AI 协助生成 |
| `0.2.1` | 2026-11-18 | 第三轮自查修复：拒绝路径幂等、入账 0 行回滚、txId 改指纹、激励精度对齐、失败分支统一 ProblemDetail | 本人提出需求与取舍，AI 协助生成 |

逐条改动原因见 [`CHANGELOG.md`](CHANGELOG.md)；设计取舍见 [`README.md`](README.md) 的「关键设计决策」。

## 项目来源

本项目是基于 JPMC Software Engineering Virtual Experience（Forage）完成的模拟后端项目。

## 业务场景

模拟一个大规模金融交易处理系统的核心服务。在真实企业场景中，类似的系统需要：
- 从消息队列接收高吞吐的金融交易
- 验证交易合法性（余额、账户存在性）
- 集成外部服务（如风控、激励计算）
- 持久化所有交易记录
- 提供查询接口给前端/用户

## 技术实现

### 1. 消息队列集成（Kafka）
- 使用 Spring Kafka 消费 transactions topic 的消息
- 配置 JSON 序列化/反序列化
- 使用嵌入式 Kafka 进行集成测试

### 2. 数据库设计（JPA + H2）
- UserRecord：用户实体，包含 id、name、balance（BigDecimal，NUMERIC(19,4)）
- TransactionRecord：交易流水，包含 txId（唯一约束）、senderId/recipientId（原始 id）、
  amount、incentive、status、fail_reason、created_at。成功与失败的交易都落库，
  因此不使用 @ManyToOne 外键关联（被拒交易的用户可能不存在）
- dev 环境用 H2 内存库，prod 环境用 PostgreSQL + Flyway 迁移脚本

### 3. 外部 API 集成（RestTemplate）
- 调用激励 API 获取交易奖励金额，地址与超时全部外部化配置
- 容错设计：连接/读取超时 + 返回 Optional 区分「0 激励」与「调用失败」
- 激励只加给收款方，调用失败不阻断主链路

### 4. REST API 开发
- GET /balance?userId={id} 查询用户余额
- 用户不存在返回 404 + ProblemDetail（detail 指明 userId），避免与「余额为 0」混淆
- 非法参数（非数字 / 非正数 / 缺参数）返回 400 ProblemDetail，与 404 共用同一套错误格式

### 5. 自动化测试
- 26 个自动化测试：
  - 12 个业务断言（TransactionServiceTest）：成功路径与全部拒绝路径，含「重复的拒绝消息不抛异常」「超长 txId 拒绝且不合并」两条回归
  - 1 个回滚一致性（TransactionRollbackTest）：入账命中 0 行时扣款一并回滚
  - 3 个激励精度规则（IncentiveServiceTest）：超 4 位小数、负数一律按 0
  - 3 个嵌入式 Kafka 端到端（KafkaEndToEndTest）
  - 5 个接口契约（BalanceControllerTest）：200 返余额、404（ProblemDetail）、400（非数字 / 非正数 / 缺参数）
  - 2 个并发（ConcurrentTransferTest）
- 原课程提供的 Task*Tests 为无断言的闯关验证器，默认排除，用 -Pforage-verifier 单独执行

## 架构设计原则

- 分层架构：Controller → Service → Repository → Entity
- 单一职责：每个类只做一件事
- 依赖注入：构造器注入
- 面向接口：Repository 接口使数据库切换零成本
- 交易处理以服务方法为事务边界：扣款、入账、写流水同事务
- 幂等：txId 去重 + 唯一约束；并发安全：数据库原子 UPDATE 代替读-改-写

## 运行说明

### 启动主服务
mvn spring-boot:run

### 启动激励 API（单独窗口）
java -jar services/transaction-incentive-api.jar

### 运行测试
mvn test

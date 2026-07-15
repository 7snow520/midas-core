# Midas Core — 金融交易处理系统

> 基于 Spring Boot + Kafka + JPA 的金融交易处理核心服务，完成交易验证、外部 API 集成、余额查询等完整业务链路。

## 项目背景

本项目模拟了一个大规模金融交易系统的核心模块。系统从 Kafka 接收交易消息，验证交易有效性后调用外部激励 API，最终更新用户余额并持久化到数据库，同时提供 REST API 供用户查询余额。

## 技术栈

| 类别 | 技术 | 版本 |
|------|------|------|
| 语言 | Java | 17 |
| 框架 | Spring Boot | 3.2.5 |
| 消息队列 | Spring Kafka | 3.1.4 |
| 持久化 | Spring Data JPA + H2 | 3.2.5 / 2.2.224 |
| 测试 | JUnit 5 + Embedded Kafka | - |
| 构建 | Maven | 3.9+ |

## 已实现功能

- Kafka 消息消费：从 transactions topic 消费交易消息
- 交易验证：发送方存在性、接收方存在性、余额充足性三道校验
- 数据持久化：JPA @ManyToOne 关联用户与交易记录
- 外部 API 集成：RestTemplate 调用激励服务，容错设计
- 余额查询 API：GET /balance 端点
- 自动化测试：5 个集成测试覆盖完整链路

## 项目结构

src/main/java/com/jpmc/midascore/
├── MidasCoreApplication.java       # 启动类
├── BalanceController.java          # REST 控制器
├── component/
│   ├── DatabaseConduit.java        # 核心业务逻辑
│   ├── IncentiveService.java       # 激励 API 调用
│   └── TransactionListener.java    # Kafka 监听器
├── entity/
│   ├── UserRecord.java             # 用户实体
│   └── TransactionRecord.java      # 交易实体
├── foundation/
│   ├── Transaction.java            # 交易 DTO
│   ├── Balance.java                # 余额 DTO
│   └── Incentive.java              # 激励 DTO
└── repository/
├── UserRepository.java         # 用户数据访问
└── TransactionRepository.java  # 交易数据访问

## 快速开始

### 环境要求
- JDK 17
- Maven 3.9+

### 构建
mvn clean install

### 运行
mvn spring-boot:run

### 测试
mvn test

## 遇到的问题

1. YAML 配置格式错误导致启动失败 — 通过堆栈定位行号解决
2. Kafka consumer 缺少 group.id — 补充配置解决
3. 序列化类型不匹配 — 配置 JsonSerializer/JsonDeserializer
4. 外部 API 容错 — try-catch + 超时控制

## 作者
GitHub: [7snow520](https://github.com/7snow520)



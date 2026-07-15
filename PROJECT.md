# 项目说明文档

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
- UserRecord：用户实体，包含 id、name、balance
- TransactionRecord：交易实体，通过 @ManyToOne 关联用户
- H2 内存数据库简化本地开发

### 3. 外部 API 集成（RestTemplate）
- 调用激励 API 获取交易奖励金额
- 容错设计：超时控制 + try-catch
- 激励只加给收款方

### 4. REST API 开发
- GET /balance?userId={id} 查询用户余额
- 返回 JSON 格式的 Balance 对象

### 5. 自动化测试
- 5 个集成测试覆盖完整业务链路
- 使用嵌入式 Kafka 模拟消息队列

## 架构设计原则

- 分层架构：Controller → Component → Repository → Entity
- 单一职责：每个类只做一件事
- 依赖注入：构造器注入
- 面向接口：Repository 接口使数据库切换零成本

## 运行说明

### 启动主服务
mvn spring-boot:run

### 启动激励 API（单独窗口）
java -jar services/transaction-incentive-api.jar

### 运行测试
mvn test
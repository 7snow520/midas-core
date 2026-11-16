package com.jpmc.midascore;

import com.jpmc.midascore.component.IncentiveService;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * 端到端：进程内真实 Kafka broker（@EmbeddedKafka，不是 mock）。
 * 激励服务用 @MockBean 替换，保证 CI 里可离线运行；真实 HTTP 链路的验证方式见 README。
 *
 * <p>覆盖三个真实生产会遇到的问题：
 * 1. 应用内生产（带 __TypeId__ 头）→ 正常入账；
 * 2. 外部系统投递裸 JSON（无头）→ 必须也能消费；
 * 3. 投递一条彻底无法解析的坏消息 → 不能卡死分区，后续消息仍能处理。
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.group-id=e2e-group"
})
@EmbeddedKafka(partitions = 1, topics = {"transactions"})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaEndToEndTest {

    @Autowired
    private KafkaTemplate<String, Transaction> kafkaTemplate;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;

    @MockBean
    private IncentiveService incentiveService;

    private long alice;
    private long bob;

    @BeforeEach
    void setUp() {
        transactionRepository.deleteAll();
        userRepository.deleteAll();
        when(incentiveService.getIncentive(any())).thenReturn(Optional.of(new BigDecimal("2.0000")));
        alice = userRepository.save(new UserRecord("Alice", new BigDecimal("100.0000"))).getId();
        bob = userRepository.save(new UserRecord("Bob", new BigDecimal("0.0000"))).getId();
    }

    @Test
    @Order(1)
    @DisplayName("端到端：真实 Kafka 消息 → 监听器 → 扣款/入账/落库（激励服务 mock）")
    void messageFlowsThroughKafkaListenerAndRealIncentiveHttp() {
        Transaction tx = new Transaction("e2e-" + UUID.randomUUID(), alice, bob, new BigDecimal("10.0000"));
        kafkaTemplate.send("transactions", 0, tx.getTxId(), tx);

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    List<TransactionRecord> found = transactionRepository.findAll();
                    assertThat(found).hasSize(1);
                    assertThat(found.get(0).getTxId()).isEqualTo(tx.getTxId());
                });

        TransactionRecord record = transactionRepository.findAll().get(0);
        BigDecimal aliceBalance = userRepository.findById(alice).orElseThrow().getBalance();
        BigDecimal bobBalance = userRepository.findById(bob).orElseThrow().getBalance();
        assertThat(aliceBalance).isEqualByComparingTo("90.0000");
        assertThat(bobBalance).isEqualByComparingTo(new BigDecimal("12.0000"));
        assertThat(record.getIncentive()).isEqualByComparingTo("2.0000");

        System.out.println("[E2E-1] OK incentive=" + record.getIncentive()
                + "，Alice=" + aliceBalance + "，Bob=" + bobBalance);
    }

    @Test
    @Order(2)
    @DisplayName("端到端：外部系统裸 JSON（无 __TypeId__ 头）必须可直接消费")
    void plainJsonFromExternalProducerIsConsumed() {
        String txId = "raw-" + UUID.randomUUID();
        rawSend(txId, "{\"txId\":\"" + txId + "\",\"senderId\":" + alice
                + ",\"recipientId\":" + bob + ",\"amount\":5.00}");

        Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(transactionRepository.findAll()).hasSize(1));

        TransactionRecord record = transactionRepository.findAll().get(0);
        assertThat(record.getTxId()).isEqualTo(txId);
        assertThat(record.getAmount()).isEqualByComparingTo("5.0000");
        assertThat(userRepository.findById(alice).orElseThrow().getBalance()).isEqualByComparingTo("95");

        System.out.println("[E2E-2] OK 裸 JSON 成功入账，Alice="
                + userRepository.findById(alice).orElseThrow().getBalance());
    }

    @Test
    @Order(3)
    @DisplayName("端到端：一条无法解析的坏消息不能卡死分区，后续消息仍能处理")
    void poisonMessageDoesNotBlockPartition() {
        rawSend("poison-" + UUID.randomUUID(), "this-is-not-json{{");

        String goodTxId = "after-poison-" + UUID.randomUUID();
        rawSend(goodTxId, "{\"txId\":\"" + goodTxId + "\",\"senderId\":" + alice
                + ",\"recipientId\":" + bob + ",\"amount\":1.00}");

        Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(transactionRepository.findAll())
                        .extracting(TransactionRecord::getTxId).contains(goodTxId));

        System.out.println("[E2E-3] OK 坏消息未阻塞分区，后续消息入账；DLT 消息数=" + countMessages("transactions.DLT"));
    }

    private void rawSend(String key, String json) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> raw = new KafkaProducer<>(props)) {
            raw.send(new ProducerRecord<>("transactions", 0, key, json)).get();
        } catch (Exception e) {
            throw new IllegalStateException("裸 producer 发送失败", e);
        }
    }

    private long countMessages(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "probe-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 5000;
            long count = 0;
            while (System.currentTimeMillis() < deadline) {
                count += consumer.poll(Duration.ofMillis(300)).count();
            }
            return count;
        }
    }
}

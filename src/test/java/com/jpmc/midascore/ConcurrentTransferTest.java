package com.jpmc.midascore;

import com.jpmc.midascore.component.IncentiveService;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.TransactionStatus;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.service.TransactionOutcome;
import com.jpmc.midascore.service.TransactionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * 并发安全测试：多线程同时抢同一个付款账户。
 *
 * <p>守两条线：
 * 1. 不会透支——余额 100、每笔 10、30 个并发请求，只能成功 10 笔；
 * 2. 账实相等——成功笔数 × 金额 == 付款方减少额 == 收款方增加额，流水中成功/失败条数可核对。
 */
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
class ConcurrentTransferTest {

    @Autowired
    private TransactionService transactionService;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @MockBean
    private IncentiveService incentiveService;

    private long alice;
    private long bob;

    @BeforeEach
    void setUp() {
        transactionRepository.deleteAll();
        userRepository.deleteAll();
        when(incentiveService.getIncentive(any())).thenReturn(Optional.empty());
        alice = userRepository.save(new UserRecord("Alice", new BigDecimal("100.0000"))).getId();
        bob = userRepository.save(new UserRecord("Bob", new BigDecimal("0.0000"))).getId();
    }

    @Test
    @DisplayName("30 个并发请求抢 100 元余额：只能成功 10 笔，账实相等")
    void concurrentTransfersNeverOverdraw() throws Exception {
        int threads = 30;
        BigDecimal amount = new BigDecimal("10.0000");

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneGate = new CountDownLatch(threads);
        Map<TransactionOutcome, AtomicInteger> outcomes = new ConcurrentHashMap<>();
        AtomicInteger unexpected = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            final String txId = "conc-" + i + "-" + UUID.randomUUID();
            pool.submit(() -> {
                try {
                    startGate.await();
                    TransactionOutcome outcome = transactionService.processTransaction(
                            new Transaction(txId, alice, bob, amount));
                    outcomes.computeIfAbsent(outcome, k -> new AtomicInteger()).incrementAndGet();
                } catch (Exception e) {
                    unexpected.incrementAndGet();
                    System.out.println("[CONC] 意外异常: " + e.getClass().getSimpleName() + " - " + e.getMessage());
                } finally {
                    doneGate.countDown();
                }
            });
        }

        startGate.countDown();
        assertThat(doneGate.await(60, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();

        int success = outcomes.getOrDefault(TransactionOutcome.SUCCESS, new AtomicInteger()).get();
        int insufficient = outcomes.getOrDefault(TransactionOutcome.INSUFFICIENT_FUNDS, new AtomicInteger()).get();

        System.out.println("[CONC] outcomes=" + summarize(outcomes)
                + "，意外异常=" + unexpected.get());

        assertThat(unexpected.get()).isZero();
        assertThat(success).isEqualTo(10);
        assertThat(insufficient).isEqualTo(20);

        BigDecimal aliceBalance = userRepository.findById(alice).orElseThrow().getBalance();
        BigDecimal bobBalance = userRepository.findById(bob).orElseThrow().getBalance();
        assertThat(aliceBalance).isEqualByComparingTo("0.0000");
        assertThat(bobBalance).isEqualByComparingTo("100.0000");

        List<TransactionRecord> records = transactionRepository.findAll();
        assertThat(records).hasSize(threads);   // 成功 10 条 + 失败 20 条，全部留痕
        long succeeded = records.stream().filter(r -> r.getStatus() == TransactionStatus.SUCCESS).count();
        assertThat(succeeded).isEqualTo(10);

        // 账实核对：成功流水金额之和 == 余额变动之和
        BigDecimal sumOfSuccess = records.stream()
                .filter(r -> r.getStatus() == TransactionStatus.SUCCESS)
                .map(TransactionRecord::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        assertThat(sumOfSuccess).isEqualByComparingTo("100.0000");
        assertThat(aliceBalance.add(bobBalance)).isEqualByComparingTo("100.0000");

        System.out.println("[CONC] 最终 Alice=" + aliceBalance + "，Bob=" + bobBalance
                + "，流水 " + records.size() + " 条（成功 " + succeeded + "），成功金额合计=" + sumOfSuccess);
    }

    @Test
    @DisplayName("同一个 txId 被 10 个线程并发投递：只入账一次")
    void concurrentSameTxIdIsAppliedOnce() throws Exception {
        String txId = "dup-" + UUID.randomUUID();
        int threads = 10;
        BigDecimal amount = new BigDecimal("10.0000");

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneGate = new CountDownLatch(threads);
        Map<TransactionOutcome, AtomicInteger> outcomes = new ConcurrentHashMap<>();
        AtomicInteger unexpected = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                    TransactionOutcome outcome = transactionService.processTransaction(
                            new Transaction(txId, alice, bob, amount));
                    outcomes.computeIfAbsent(outcome, k -> new AtomicInteger()).incrementAndGet();
                } catch (Exception e) {
                    unexpected.incrementAndGet();
                    System.out.println("[CONC-dup] 异常: " + e.getClass().getSimpleName() + " - " + e.getMessage());
                } finally {
                    doneGate.countDown();
                }
            });
        }

        startGate.countDown();
        assertThat(doneGate.await(60, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();

        int success = outcomes.getOrDefault(TransactionOutcome.SUCCESS, new AtomicInteger()).get();
        System.out.println("[CONC-dup] outcomes=" + summarize(outcomes)
                + "，意外异常=" + unexpected.get());

        int duplicates = outcomes.getOrDefault(TransactionOutcome.DUPLICATE, new AtomicInteger()).get();
        assertThat(unexpected.get()).isZero();
        assertThat(success).isEqualTo(1);
        assertThat(duplicates).isEqualTo(threads - 1);
        assertThat(outcomes).hasSize(2);   // 只有 SUCCESS 与 DUPLICATE，没有别的结果

        List<TransactionRecord> records = transactionRepository.findAll();
        assertThat(records).hasSize(1);
        assertThat(userRepository.findById(alice).orElseThrow().getBalance()).isEqualByComparingTo("90.0000");
        assertThat(userRepository.findById(bob).orElseThrow().getBalance()).isEqualByComparingTo("10.0000");
    }

    private String summarize(Map<TransactionOutcome, AtomicInteger> outcomes) {
        StringBuilder sb = new StringBuilder("{");
        outcomes.forEach((k, v) -> sb.append(k).append('=').append(v.get()).append(", "));
        return (sb.length() > 1 ? sb.substring(0, sb.length() - 2) : "") + "}";
    }
}

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
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * 交易链路的断言式测试：覆盖成功路径与全部拒绝路径。
 * 不依赖外部 Kafka / 激励服务，激励服务用 @MockBean 替换。
 */
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
class TransactionServiceTest {

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
        alice = userRepository.save(new UserRecord("Alice", new BigDecimal("100.0000"))).getId();
        bob = userRepository.save(new UserRecord("Bob", new BigDecimal("0.0000"))).getId();
    }

    private BigDecimal balanceOf(long userId) {
        return userRepository.findById(userId).orElseThrow().getBalance();
    }

    private Transaction tx(String txId, long from, long to, String amount) {
        return new Transaction(txId, from, to, new BigDecimal(amount));
    }

    @Test
    @DisplayName("转账成功：付款方扣本金，收款方得本金 + 激励，流水记录为 SUCCESS")
    void successCreditsPrincipalAndIncentive() {
        when(incentiveService.getIncentive(any())).thenReturn(Optional.of(new BigDecimal("2.0000")));

        TransactionOutcome outcome = transactionService.processTransaction(tx("tx-1", alice, bob, "10"));

        assertThat(outcome).isEqualTo(TransactionOutcome.SUCCESS);
        assertThat(balanceOf(alice)).isEqualByComparingTo("90");
        assertThat(balanceOf(bob)).isEqualByComparingTo("12");

        List<TransactionRecord> records = transactionRepository.findAll();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getStatus()).isEqualTo(TransactionStatus.SUCCESS);
        assertThat(records.get(0).getIncentive()).isEqualByComparingTo("2");
    }

    @Test
    @DisplayName("余额不足：交易被拒，双方余额不变，但失败流水必须落库")
    void insufficientFundsLeavesBalancesUntouched() {
        TransactionOutcome outcome = transactionService.processTransaction(tx("tx-2", alice, bob, "150"));

        assertThat(outcome).isEqualTo(TransactionOutcome.INSUFFICIENT_FUNDS);
        assertThat(balanceOf(alice)).isEqualByComparingTo("100");
        assertThat(balanceOf(bob)).isEqualByComparingTo("0");

        List<TransactionRecord> records = transactionRepository.findAll();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getStatus()).isEqualTo(TransactionStatus.FAILED);
        assertThat(records.get(0).getFailReason()).contains("余额不足");
    }

    @Test
    @DisplayName("发送方不存在：交易被拒并留痕")
    void unknownSenderIsRejected() {
        TransactionOutcome outcome = transactionService.processTransaction(tx("tx-3", 9999L, bob, "1"));

        assertThat(outcome).isEqualTo(TransactionOutcome.SENDER_NOT_FOUND);
        assertThat(balanceOf(bob)).isEqualByComparingTo("0");
        assertThat(transactionRepository.findAll()).hasSize(1);
    }

    @Test
    @DisplayName("收款方不存在：交易被拒并留痕")
    void unknownRecipientIsRejected() {
        TransactionOutcome outcome = transactionService.processTransaction(tx("tx-4", alice, 9999L, "1"));

        assertThat(outcome).isEqualTo(TransactionOutcome.RECIPIENT_NOT_FOUND);
        assertThat(balanceOf(alice)).isEqualByComparingTo("100");
    }

    @Test
    @DisplayName("自己转自己：交易被拒")
    void selfTransferIsRejected() {
        TransactionOutcome outcome = transactionService.processTransaction(tx("tx-5", alice, alice, "1"));

        assertThat(outcome).isEqualTo(TransactionOutcome.SENDER_AND_RECIPIENT_SAME);
        assertThat(balanceOf(alice)).isEqualByComparingTo("100");
    }

    @Test
    @DisplayName("同一 txId 重复投递：只入账一次")
    void duplicateTxIdIsAppliedOnce() {
        when(incentiveService.getIncentive(any())).thenReturn(Optional.empty());

        assertThat(transactionService.processTransaction(tx("tx-6", alice, bob, "10")))
                .isEqualTo(TransactionOutcome.SUCCESS);
        assertThat(transactionService.processTransaction(tx("tx-6", alice, bob, "10")))
                .isEqualTo(TransactionOutcome.DUPLICATE);

        assertThat(balanceOf(alice)).isEqualByComparingTo("90");
        assertThat(balanceOf(bob)).isEqualByComparingTo("10");
        assertThat(transactionRepository.findAll()).hasSize(1);
    }

    @Test
    @DisplayName("激励服务不可用：仅入账本金，主链路不受影响")
    void incentiveFailureDoesNotBlockTransfer() {
        when(incentiveService.getIncentive(any())).thenReturn(Optional.empty());

        TransactionOutcome outcome = transactionService.processTransaction(tx("tx-7", alice, bob, "10"));

        assertThat(outcome).isEqualTo(TransactionOutcome.SUCCESS);
        assertThat(balanceOf(bob)).isEqualByComparingTo("10");
        assertThat(transactionRepository.findAll().get(0).getIncentive()).isEqualByComparingTo("0");
    }

    @Test
    @DisplayName("float 时代的尾差问题：0.1 + 0.1 精确等于 0.2")
    void decimalAmountsHaveNoFloatingPointDrift() {
        when(incentiveService.getIncentive(any())).thenReturn(Optional.empty());

        transactionService.processTransaction(tx("tx-8", alice, bob, "0.1"));
        transactionService.processTransaction(tx("tx-9", alice, bob, "0.1"));

        assertThat(balanceOf(bob)).isEqualByComparingTo("0.2");
        assertThat(balanceOf(alice)).isEqualByComparingTo("99.8");
    }

    @Test
    @DisplayName("金额超过 4 位小数：拒绝而不是静默四舍五入")
    void excessiveScaleIsRejected() {
        TransactionOutcome outcome = transactionService.processTransaction(tx("tx-10", alice, bob, "0.12345"));

        assertThat(outcome).isEqualTo(TransactionOutcome.INVALID_AMOUNT);
        assertThat(balanceOf(alice)).isEqualByComparingTo("100");
        assertThat(balanceOf(bob)).isEqualByComparingTo("0");

        List<TransactionRecord> records = transactionRepository.findAll();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getFailReason()).contains("4 位小数");
    }
}

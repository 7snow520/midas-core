package com.jpmc.midascore;

import com.jpmc.midascore.component.IncentiveService;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.service.TransactionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * 资金一致性测试：扣款成功但入账落空（收款方在事务内消失）时必须整笔回滚，
 * 不能出现「钱从 A 划走、没有到 B」的状态。
 */
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
class TransactionRollbackTest {

    @Autowired
    private TransactionService transactionService;

    @SpyBean
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
        when(incentiveService.getIncentive(any())).thenReturn(Optional.empty());
    }

    @Test
    @DisplayName("入账命中 0 行：整笔回滚，扣款不生效，也不留 SUCCESS 流水")
    void creditMissingRowRollsBackDebit() {
        doReturn(0).when(userRepository).credit(anyLong(), any());

        Transaction transaction = new Transaction("tx-rollback", alice, bob, new BigDecimal("10"));

        assertThatThrownBy(() -> transactionService.processTransaction(transaction))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("收款方");

        assertThat(userRepository.findById(alice).orElseThrow().getBalance()).isEqualByComparingTo("100");
        assertThat(transactionRepository.findAll()).isEmpty();
    }
}

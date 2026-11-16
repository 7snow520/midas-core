package com.jpmc.midascore.service;

import com.jpmc.midascore.component.IncentiveService;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.TransactionStatus;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;
import java.util.UUID;

@Service
public class TransactionService {
    private static final Logger log = LoggerFactory.getLogger(TransactionService.class);

    private static final int MONEY_SCALE = 4;
    private static final BigDecimal ZERO = BigDecimal.ZERO.setScale(MONEY_SCALE);

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final IncentiveService incentiveService;
    private final TransactionTemplate transactionTemplate;

    public TransactionService(UserRepository userRepository,
                              TransactionRepository transactionRepository,
                              IncentiveService incentiveService,
                              PlatformTransactionManager transactionManager) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.incentiveService = incentiveService;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /**
     * 处理一笔交易。
     *
     * <p>外部 HTTP 调用（激励服务，超时 3s）被刻意放在事务之外：如果放在 debit 与 credit
     * 之间，发送方的行锁和数据库连接会被整个 RPC 时长占住，下游一抖动就把消费吞吐拖垮。
     * 代价是「重复消息 / 账户不存在 / 余额不足」这几类交易会白调一次激励服务——接口是只读
     * 计算、且这类交易占比很低，可以接受。
     */
    public TransactionOutcome processTransaction(Transaction transaction) {
        String txId = resolveTxId(transaction);
        BigDecimal rawAmount = transaction.getAmount();

        if (rawAmount == null || rawAmount.signum() <= 0) {
            return recordRejection(txId, transaction, ZERO, TransactionOutcome.INVALID_AMOUNT, "金额必须为正数");
        }
        // 金融场景不做静默舍入：超过 4 位小数直接拒绝，让上游暴露精度问题，而不是悄悄改金额
        if (rawAmount.stripTrailingZeros().scale() > MONEY_SCALE) {
            return recordRejection(txId, transaction, ZERO, TransactionOutcome.INVALID_AMOUNT,
                    "金额最多支持 " + MONEY_SCALE + " 位小数");
        }
        BigDecimal amount = rawAmount.setScale(MONEY_SCALE, RoundingMode.UNNECESSARY);

        if (transaction.getSenderId() == transaction.getRecipientId()) {
            return recordRejection(txId, transaction, amount, TransactionOutcome.SENDER_AND_RECIPIENT_SAME,
                    "发送方与接收方不能是同一账户");
        }

        // 快速路径：重复投递的消息先挡掉，不必为已知重复的交易白调一次激励服务
        if (transactionRepository.existsByTxId(txId)) {
            log.warn("重复交易被忽略: txId={}", txId);
            return TransactionOutcome.DUPLICATE;
        }

        // 事务外：调用外部激励服务
        BigDecimal incentive = incentiveService.getIncentive(transaction).orElse(ZERO);

        TransactionOutcome outcome;
        try {
            outcome = transactionTemplate.execute(status -> apply(transaction, txId, amount, incentive));
        } catch (DataIntegrityViolationException e) {
            // 并发窗口：两个线程同时通过了 existsByTxId 预检，唯一约束是最后一道防线。
            // 后到的那个事务整体回滚（扣款/入账一起撤销），这里转成 DUPLICATE 返回，
            // 而不是把异常抛给 Kafka 监听器触发无意义的重试和一片错误日志。
            log.warn("并发重复交易被唯一约束拦截，按重复处理: txId={}", txId);
            return TransactionOutcome.DUPLICATE;
        }
        if (outcome == null) {
            throw new IllegalStateException("交易处理回调返回 null: txId=" + txId);
        }
        return outcome;
    }

    /**
     * 事务内：查重、校验账户、原子扣款、入账、写流水。任一步抛出异常，整笔交易回滚。
     */
    private TransactionOutcome apply(Transaction transaction, String txId, BigDecimal amount, BigDecimal incentive) {
        if (transactionRepository.existsByTxId(txId)) {
            log.warn("重复交易被忽略(事务内兜底): txId={}", txId);
            return TransactionOutcome.DUPLICATE;
        }

        Optional<UserRecord> sender = userRepository.findById(transaction.getSenderId());
        if (sender.isEmpty()) {
            return reject(txId, transaction, amount, TransactionOutcome.SENDER_NOT_FOUND,
                    "发送方不存在: " + transaction.getSenderId());
        }
        Optional<UserRecord> recipient = userRepository.findById(transaction.getRecipientId());
        if (recipient.isEmpty()) {
            return reject(txId, transaction, amount, TransactionOutcome.RECIPIENT_NOT_FOUND,
                    "接收方不存在: " + transaction.getRecipientId());
        }

        // 原子扣款，0 行 = 余额不足（或并发下已被别的交易扣走）
        if (userRepository.debit(transaction.getSenderId(), amount) == 0) {
            log.warn("余额不足，交易拒绝: txId={}, sender={}, amount={}", txId, transaction.getSenderId(), amount);
            return reject(txId, transaction, amount, TransactionOutcome.INSUFFICIENT_FUNDS,
                    "发送方余额不足");
        }

        userRepository.credit(transaction.getRecipientId(), amount.add(incentive));

        transactionRepository.save(new TransactionRecord(txId, transaction.getSenderId(),
                transaction.getRecipientId(), amount, incentive, TransactionStatus.SUCCESS, null));

        log.info("交易成功: txId={}, {} -> {}, amount={}, incentive={}",
                txId, sender.get().getName(), recipient.get().getName(), amount, incentive);
        return TransactionOutcome.SUCCESS;
    }

    private TransactionOutcome reject(String txId, Transaction transaction, BigDecimal amount,
                                      TransactionOutcome outcome, String reason) {
        saveFailedRecord(txId, transaction, amount, reason);
        return outcome;
    }

    /**
     * 交易在进入事务之前就被拒（金额非法、自己转自己）：单独开一个短事务把失败流水写进去。
     */
    private TransactionOutcome recordRejection(String txId, Transaction transaction, BigDecimal amount,
                                               TransactionOutcome outcome, String reason) {
        transactionTemplate.executeWithoutResult(status ->
                saveFailedRecord(txId, transaction, amount, reason));
        return outcome;
    }

    private void saveFailedRecord(String txId, Transaction transaction, BigDecimal amount, String reason) {
        log.warn("交易拒绝: txId={}, sender={}, recipient={}, reason={}",
                txId, transaction.getSenderId(), transaction.getRecipientId(), reason);
        transactionRepository.save(new TransactionRecord(txId, transaction.getSenderId(),
                transaction.getRecipientId(), amount, ZERO, TransactionStatus.FAILED, reason));
    }

    private String resolveTxId(Transaction transaction) {
        String txId = transaction.getTxId();
        if (txId == null || txId.isBlank()) {
            txId = UUID.randomUUID().toString();
            log.warn("交易消息缺少 txId，已生成临时流水号={}（该消息不具备幂等保护）", txId);
        }
        transaction.setTxId(txId);
        return txId.length() > 64 ? txId.substring(0, 64) : txId;
    }
}

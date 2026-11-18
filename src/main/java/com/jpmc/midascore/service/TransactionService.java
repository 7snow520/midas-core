package com.jpmc.midascore.service;

import com.jpmc.midascore.component.IncentiveService;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.TransactionStatus;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Money;
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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

@Service
public class TransactionService {
    private static final Logger log = LoggerFactory.getLogger(TransactionService.class);

    /** 与数据库列 tx_id VARCHAR(64) 一致 */
    private static final int TX_ID_MAX_LENGTH = 64;

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
     *
     * <p>查重放在所有业务校验之前：同一条消息（无论是否合法）重投时都应该在第一步被挡掉，
     * 否则重复的「被拒交易」会再次尝试写 FAILED 流水、撞唯一约束抛异常，进而触发 Kafka 重试。
     */
    public TransactionOutcome processTransaction(Transaction transaction) {
        boolean txIdTooLong = transaction.getTxId() != null
                && transaction.getTxId().length() > TX_ID_MAX_LENGTH;
        String txId = resolveTxId(transaction);

        if (transactionRepository.existsByTxId(txId)) {
            log.warn("重复交易被忽略: txId={}", txId);
            return TransactionOutcome.DUPLICATE;
        }

        if (txIdTooLong) {
            return recordRejection(txId, transaction, Money.ZERO, TransactionOutcome.INVALID_TX_ID,
                    "txId 超过 " + TX_ID_MAX_LENGTH + " 字符");
        }

        BigDecimal rawAmount = transaction.getAmount();
        if (rawAmount == null || rawAmount.signum() <= 0) {
            return recordRejection(txId, transaction, Money.ZERO, TransactionOutcome.INVALID_AMOUNT,
                    "金额必须为正数");
        }
        // 金融场景不做静默舍入：超过 4 位小数直接拒绝，让上游暴露精度问题，而不是悄悄改金额
        if (rawAmount.stripTrailingZeros().scale() > Money.SCALE) {
            return recordRejection(txId, transaction, Money.ZERO, TransactionOutcome.INVALID_AMOUNT,
                    "金额最多支持 " + Money.SCALE + " 位小数");
        }
        BigDecimal amount = rawAmount.setScale(Money.SCALE, RoundingMode.UNNECESSARY);

        if (transaction.getSenderId() == transaction.getRecipientId()) {
            return recordRejection(txId, transaction, amount, TransactionOutcome.SENDER_AND_RECIPIENT_SAME,
                    "发送方与接收方不能是同一账户");
        }

        // 事务外：调用外部激励服务
        BigDecimal incentive = incentiveService.getIncentive(transaction).orElse(Money.ZERO);

        TransactionOutcome outcome;
        try {
            outcome = transactionTemplate.execute(status -> apply(transaction, txId, amount, incentive));
        } catch (DataIntegrityViolationException e) {
            if (!isDuplicateTxIdViolation(e)) {
                throw e;    // 其它约束错误（精度、非空等）必须暴露，不能伪装成「重复交易」
            }
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

        // 入账命中 0 行（收款方在事务内被删除）必须中断：否则扣了钱、没人收，事务照样提交
        if (userRepository.credit(transaction.getRecipientId(), amount.add(incentive)) == 0) {
            throw new IllegalStateException(
                    "收款方在事务内消失，整笔回滚: txId=" + txId + ", recipientId=" + transaction.getRecipientId());
        }

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
     * 交易在进入事务之前就被拒（金额非法、txId 非法、自己转自己）：单独开一个短事务把失败流水写进去。
     *
     * <p>并发窗口下两个线程可能同时写同一条 txId 的失败流水，唯一约束会拦下后到的那个；
     * 这里接住并忽略——一条重复的拒绝消息不需要被重试。
     */
    private TransactionOutcome recordRejection(String txId, Transaction transaction, BigDecimal amount,
                                              TransactionOutcome outcome, String reason) {
        try {
            transactionTemplate.executeWithoutResult(status ->
                    saveFailedRecord(txId, transaction, amount, reason));
        } catch (DataIntegrityViolationException e) {
            if (!isDuplicateTxIdViolation(e)) {
                throw e;
            }
            log.warn("重复的拒绝消息，忽略: txId={}, reason={}", txId, reason);
        }
        return outcome;
    }

    private void saveFailedRecord(String txId, Transaction transaction, BigDecimal amount, String reason) {
        log.warn("交易拒绝: txId={}, sender={}, recipient={}, reason={}",
                txId, transaction.getSenderId(), transaction.getRecipientId(), reason);
        transactionRepository.save(new TransactionRecord(txId, transaction.getSenderId(),
                transaction.getRecipientId(), amount, Money.ZERO, TransactionStatus.FAILED, reason));
    }

    /**
     * 取（并规范化）业务流水号。
     *
     * <p>超长 txId 不再截断：截断会让「前 64 位相同」的两笔不同交易撞成同一笔，
     * 后到的那笔会被误判为重复而静默丢弃。这里改用 SHA-256 指纹作为存储键，
     * 既满足列宽，又保持「同一条消息 → 同一个键」的确定性。
     */
    private String resolveTxId(Transaction transaction) {
        String txId = transaction.getTxId();
        if (txId == null || txId.isBlank()) {
            txId = UUID.randomUUID().toString();
            log.warn("交易消息缺少 txId，已生成临时流水号={}（该消息不具备幂等保护）", txId);
        } else if (txId.length() > TX_ID_MAX_LENGTH) {
            String fingerprint = sha256Hex(txId);
            log.warn("txId 超过 {} 字符，改用 SHA-256 指纹存储: 原长度={}, 指纹={}",
                    TX_ID_MAX_LENGTH, txId.length(), fingerprint);
            txId = fingerprint;
        }
        transaction.setTxId(txId);
        return txId;
    }

    /** 只认 tx_id 唯一约束；H2 会把约束名大写，所以做大小写无关判断。 */
    private boolean isDuplicateTxIdViolation(DataIntegrityViolationException e) {
        Throwable cause = e.getMostSpecificCause();
        String message = cause.getMessage();
        return message != null && message.toLowerCase().contains("uk_transaction_tx_id");
    }

    private String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 不可用", e);
        }
    }
}

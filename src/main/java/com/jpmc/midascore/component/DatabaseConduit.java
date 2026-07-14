package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DatabaseConduit {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConduit.class);

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final IncentiveService incentiveService;

    public DatabaseConduit(UserRepository userRepository, TransactionRepository transactionRepository, IncentiveService incentiveService) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.incentiveService = incentiveService;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }

    public boolean processTransaction(long senderId, long recipientId, float amount) {
        UserRecord sender = userRepository.findById(senderId);
        UserRecord recipient = userRepository.findById(recipientId);

        if (sender == null || recipient == null) {
            logger.info("无效交易：发送方或接收方不存在");
            return false;
        }

        if (sender.getBalance() < amount) {
            logger.info("无效交易：发送方余额不足");
            return false;
        }

        // 获取激励金额
        Transaction transaction = new Transaction(senderId, recipientId, amount);
        float incentive = incentiveService.getIncentive(transaction);

        // 发送方扣款（不扣激励）
        sender.setBalance(sender.getBalance() - amount);
        // 收款方加款（加激励）
        recipient.setBalance(recipient.getBalance() + amount + incentive);

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord record = new TransactionRecord(sender, recipient, amount, incentive);
        transactionRepository.save(record);

        logger.info("交易成功：{} -> {}，金额：{}，激励：{}", sender.getName(), recipient.getName(), amount, incentive);
        return true;
    }
}

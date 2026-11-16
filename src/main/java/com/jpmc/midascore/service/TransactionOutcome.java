package com.jpmc.midascore.service;

/**
 * 交易处理结果。除了 SUCCESS，其余都是「已处理但未成交」，不会触发 Kafka 重试。
 */
public enum TransactionOutcome {
    SUCCESS,
    DUPLICATE,
    INVALID_AMOUNT,
    SENDER_AND_RECIPIENT_SAME,
    SENDER_NOT_FOUND,
    RECIPIENT_NOT_FOUND,
    INSUFFICIENT_FUNDS
}

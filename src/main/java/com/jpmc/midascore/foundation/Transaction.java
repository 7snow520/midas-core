package com.jpmc.midascore.foundation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;

/**
 * 交易消息体（Kafka 入参）。
 *
 * <p>{@code txId} 用于幂等去重：上游应为每笔交易生成唯一的业务流水号；
 * 若消息未携带，则由服务端消费时生成（此时无法跨重投去重，仅用于历史消息兼容）。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction {
    private String txId;
    private long senderId;
    private long recipientId;
    private BigDecimal amount;

    public Transaction() {
    }

    public Transaction(long senderId, long recipientId, BigDecimal amount) {
        this(null, senderId, recipientId, amount);
    }

    public Transaction(String txId, long senderId, long recipientId, BigDecimal amount) {
        this.txId = txId;
        this.senderId = senderId;
        this.recipientId = recipientId;
        this.amount = amount;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public long getSenderId() {
        return senderId;
    }

    public void setSenderId(long senderId) {
        this.senderId = senderId;
    }

    public long getRecipientId() {
        return recipientId;
    }

    public void setRecipientId(long recipientId) {
        this.recipientId = recipientId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Transaction {txId=" + txId + ", senderId=" + senderId
                + ", recipientId=" + recipientId + ", amount=" + amount + "}";
    }
}

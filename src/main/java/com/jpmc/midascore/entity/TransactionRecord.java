package com.jpmc.midascore.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * 交易流水。成功与失败的交易都会落库，失败原因记在 {@code failReason}。
 *
 * <p>这里保存的是原始的 senderId / recipientId，而不是 {@code @ManyToOne} 关联：
 * 交易被拒时用户可能根本不存在，外键约束会导致这类流水无法持久化（审计缺口）。
 */
@Entity
@Table(name = "transaction_record",
        uniqueConstraints = @UniqueConstraint(name = "uk_transaction_tx_id", columnNames = "tx_id"))
public class TransactionRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @Column(name = "tx_id", nullable = false, length = 64)
    private String txId;

    @Column(name = "sender_id", nullable = false)
    private long senderId;

    @Column(name = "recipient_id", nullable = false)
    private long recipientId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal incentive;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private TransactionStatus status;

    @Column(name = "fail_reason", length = 255)
    private String failReason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected TransactionRecord() {
    }

    public TransactionRecord(String txId, long senderId, long recipientId, BigDecimal amount,
                             BigDecimal incentive, TransactionStatus status, String failReason) {
        this.txId = txId;
        this.senderId = senderId;
        this.recipientId = recipientId;
        this.amount = amount;
        this.incentive = incentive;
        this.status = status;
        this.failReason = failReason;
        this.createdAt = Instant.now();
    }

    public long getId() {
        return id;
    }

    public String getTxId() {
        return txId;
    }

    public long getSenderId() {
        return senderId;
    }

    public long getRecipientId() {
        return recipientId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public BigDecimal getIncentive() {
        return incentive;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public String getFailReason() {
        return failReason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    @Override
    public String toString() {
        return "TransactionRecord [txId=" + txId + ", senderId=" + senderId
                + ", recipientId=" + recipientId + ", amount=" + amount
                + ", incentive=" + incentive + ", status=" + status
                + ", failReason=" + failReason + "]";
    }
}

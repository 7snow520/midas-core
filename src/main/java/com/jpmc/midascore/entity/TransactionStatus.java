package com.jpmc.midascore.entity;

/**
 * 交易处理结果。失败交易同样落库，用于对账与审计。
 */
public enum TransactionStatus {
    SUCCESS,
    FAILED
}

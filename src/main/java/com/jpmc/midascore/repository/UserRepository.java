package com.jpmc.midascore.repository;

import com.jpmc.midascore.entity.UserRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;

public interface UserRepository extends JpaRepository<UserRecord, Long> {

    /**
     * 原子扣款：把「读余额 - 判断够不够 - 写回余额」合并成一条带条件的 UPDATE，
     * 由数据库保证并发下的原子性，避免两笔交易同时通过余额校验而造成透支。
     *
     * @return 受影响行数，0 表示余额不足或用户不存在
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update UserRecord u set u.balance = u.balance - :amount where u.id = :id and u.balance >= :amount")
    int debit(@Param("id") long id, @Param("amount") BigDecimal amount);

    /**
     * 原子入账。
     *
     * @return 受影响行数，0 表示收款方不存在
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update UserRecord u set u.balance = u.balance + :amount where u.id = :id")
    int credit(@Param("id") long id, @Param("amount") BigDecimal amount);
}

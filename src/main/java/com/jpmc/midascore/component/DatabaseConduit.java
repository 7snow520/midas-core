package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.stereotype.Component;

/**
 * 兼容保留的门面类：业务逻辑已迁移到
 * {@link com.jpmc.midascore.service.TransactionService}，下一个提交中删除。
 *
 * @deprecated 请直接使用 TransactionService 与 UserRepository。
 */
@Deprecated
@Component
public class DatabaseConduit {

    private final UserRepository userRepository;

    public DatabaseConduit(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }
}

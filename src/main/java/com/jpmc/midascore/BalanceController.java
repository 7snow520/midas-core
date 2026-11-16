package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import jakarta.validation.constraints.Positive;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Validated
public class BalanceController {

    private final UserRepository userRepository;

    public BalanceController(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    /**
     * 用户不存在时返回 404，而不是「余额 0」——否则调用方无法区分「查无此人」和「余额为零」。
     */
    @GetMapping("/balance")
    public ResponseEntity<Balance> getBalance(@RequestParam("userId") @Positive Long userId) {
        return userRepository.findById(userId)
                .map(user -> ResponseEntity.ok(new Balance(user.getId(), user.getBalance())))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}

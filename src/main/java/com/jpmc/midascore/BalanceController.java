package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import jakarta.validation.constraints.Positive;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
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
     *
     * <p>404 也带 ProblemDetail 体（detail 里写明是哪个 userId 查不到），
     * 与 400 的错误格式保持一致：调用方一套解析逻辑就能覆盖全部失败分支。
     */
    @GetMapping("/balance")
    public ResponseEntity<?> getBalance(@RequestParam("userId") @Positive Long userId) {
        return userRepository.findById(userId)
                .<ResponseEntity<?>>map(user -> ResponseEntity.ok(new Balance(user.getId(), user.getBalance())))
                .orElseGet(() -> {
                    ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                            HttpStatus.NOT_FOUND, "用户 " + userId + " 不存在");
                    problem.setTitle("用户不存在");
                    return ResponseEntity.of(problem).build();
                });
    }
}

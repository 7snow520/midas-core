package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BalanceController {

    private final UserRepository userRepository;

    public BalanceController(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    /**
     * 用户不存在时返回 404，而不是「余额 0」——否则调用方无法区分「查无此人」和「余额为零」。
     *
     * <p>id 不做「必须为正数」的额外校验：不存在的 id（含 0、负数）统一按「查无此人」返回 404，
     * 只有参数根本没法解析成数字时才返回 400。少一条特例分支，调用方也少一种要处理的错误。
     *
     * <p>404 也带 ProblemDetail 体（detail 里写明是哪个 userId 查不到），
     * 与 400 的错误格式保持一致：调用方一套解析逻辑就能覆盖全部失败分支。
     */
    @GetMapping("/balance")
    public ResponseEntity<?> getBalance(@RequestParam("userId") Long userId) {
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

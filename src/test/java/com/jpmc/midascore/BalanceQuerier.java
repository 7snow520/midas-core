package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Balance;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;

/**
 * 课程验证器（Task*Tests）用的余额查询客户端。
 *
 * <p>原版实现直接 {@code getForObject}，隐含假设「查不到用户也返回 200 + 余额 0」。
 * 0.2.0 起 {@code /balance} 对查无此人返回 404（否则无法区分「查不到」和「余额就是零」），
 * 这里做一层适配：404 视为「该用户不存在、余额按 0 打印」，让五关验证器的输出格式保持不变。
 * 需要区分这两种情况时直接看 HTTP 状态码即可，语义没有被改回去。
 */
@Component
public class BalanceQuerier {
    private final RestTemplate restTemplate;

    public BalanceQuerier(RestTemplateBuilder builder) {
        this.restTemplate = builder.build();
    }

    public Balance query(Long userId) {
        String url = "http://localhost:33400/balance?userId=" + userId;
        try {
            return restTemplate.getForObject(url, Balance.class);
        } catch (HttpClientErrorException.NotFound e) {
            return new Balance(userId, BigDecimal.ZERO);
        }
    }
}

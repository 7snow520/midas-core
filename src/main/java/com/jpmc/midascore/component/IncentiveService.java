package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Money;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;

/**
 * 激励服务客户端。
 *
 * <p>返回 {@link Optional}：empty 明确表示「没拿到可用的激励」（服务不可用、返回空、
 * 或金额不是合法的 4 位小数），调用方据此按 0 处理，但不会把「激励为 0」和「调用失败」混为一谈。
 *
 * <p>精度规则与交易金额保持一致：超 4 位小数不做四舍五入，按 0 处理并告警。
 * 主链路对金额零容忍、对激励容忍，会变成「同一套金额规则两种标准」，是审计上的隐患。
 */
@Service
public class IncentiveService {
    private static final Logger log = LoggerFactory.getLogger(IncentiveService.class);

    private final RestTemplate restTemplate;
    private final String incentiveUrl;

    public IncentiveService(RestTemplate incentiveRestTemplate,
                            @Value("${incentive.base-url}") String incentiveUrl) {
        this.restTemplate = incentiveRestTemplate;
        this.incentiveUrl = incentiveUrl;
    }

    public Optional<BigDecimal> getIncentive(Transaction transaction) {
        try {
            Incentive incentive = restTemplate.postForObject(incentiveUrl, transaction, Incentive.class);
            if (incentive == null || incentive.getAmount() == null) {
                log.warn("激励服务返回空结果，按 0 处理: txId={}", transaction.getTxId());
                return Optional.empty();
            }
            BigDecimal amount = incentive.getAmount();
            if (amount.signum() < 0) {
                log.warn("激励金额为负，按 0 处理: txId={}, amount={}", transaction.getTxId(), amount);
                return Optional.empty();
            }
            if (amount.stripTrailingZeros().scale() > Money.SCALE) {
                log.warn("激励金额精度超过 {} 位，按 0 处理（不做静默舍入）: txId={}, amount={}",
                        Money.SCALE, transaction.getTxId(), amount);
                return Optional.empty();
            }
            return Optional.of(amount.setScale(Money.SCALE, RoundingMode.UNNECESSARY));
        } catch (RestClientException e) {
            // 激励是附加收益，调用失败不应阻断主交易链路
            log.warn("激励服务调用失败，按 0 处理: txId={}, error={}", transaction.getTxId(), e.getMessage());
            return Optional.empty();
        }
    }
}

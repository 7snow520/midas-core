package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
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
 * <p>返回 {@link Optional}：empty 明确表示「没拿到激励」（服务不可用或返回空），
 * 调用方据此按 0 处理，但不会把「激励为 0」和「调用失败」混为一谈。
 */
@Service
public class IncentiveService {
    private static final Logger log = LoggerFactory.getLogger(IncentiveService.class);
    private static final int MONEY_SCALE = 4;

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
            return Optional.of(incentive.getAmount().setScale(MONEY_SCALE, RoundingMode.HALF_UP));
        } catch (RestClientException e) {
            // 激励是附加收益，调用失败不应阻断主交易链路
            log.warn("激励服务调用失败，按 0 处理: txId={}, error={}", transaction.getTxId(), e.getMessage());
            return Optional.empty();
        }
    }
}

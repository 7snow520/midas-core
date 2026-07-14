package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class IncentiveService {
    private static final Logger logger = LoggerFactory.getLogger(IncentiveService.class);
    private static final String INCENTIVE_API_URL = "http://localhost:8080/incentive";
    private final RestTemplate restTemplate;

    public IncentiveService() {
        this.restTemplate = new RestTemplate();
    }

    public float getIncentive(Transaction transaction) {
        try {
            Incentive incentive = restTemplate.postForObject(INCENTIVE_API_URL, transaction, Incentive.class);
            if (incentive != null) {
                logger.info("获得激励金额: {}", incentive.getAmount());
                return incentive.getAmount();
            }
        } catch (Exception e) {
            logger.error("获取激励失败: {}", e.getMessage());
        }
        return 0;
    }
}

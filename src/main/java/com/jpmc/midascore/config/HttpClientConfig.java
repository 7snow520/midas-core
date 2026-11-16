package com.jpmc.midascore.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

/**
 * 外部 HTTP 客户端配置：必须设超时，否则下游服务卡住会一路拖垮消费线程。
 */
@Configuration
public class HttpClientConfig {

    @Bean
    public RestTemplate incentiveRestTemplate(RestTemplateBuilder builder,
                                             @Value("${incentive.connect-timeout-ms:2000}") long connectTimeoutMs,
                                             @Value("${incentive.read-timeout-ms:3000}") long readTimeoutMs) {
        return builder
                .setConnectTimeout(Duration.ofMillis(connectTimeoutMs))
                .setReadTimeout(Duration.ofMillis(readTimeoutMs))
                .build();
    }
}

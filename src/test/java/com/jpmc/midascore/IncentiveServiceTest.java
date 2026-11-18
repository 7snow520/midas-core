package com.jpmc.midascore;

import com.jpmc.midascore.component.IncentiveService;
import com.jpmc.midascore.foundation.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/**
 * 激励客户端的金额规则测试：与交易金额同一套标准 —— 超 4 位小数不做静默舍入，按 0 处理。
 */
class IncentiveServiceTest {

    private static final String URL = "http://localhost:8080/incentive";

    private MockRestServiceServer server;
    private IncentiveService incentiveService;
    private Transaction transaction;

    @BeforeEach
    void setUp() {
        RestTemplate restTemplate = new RestTemplate();
        server = MockRestServiceServer.bindTo(restTemplate).build();
        incentiveService = new IncentiveService(restTemplate, URL);
        transaction = new Transaction("tx-incentive", 1L, 2L, new BigDecimal("10"));
    }

    private void respond(String body) {
        server.expect(requestTo(URL))
                .andRespond(withSuccess(body, MediaType.APPLICATION_JSON));
    }

    @Test
    @DisplayName("合法激励：补齐到 4 位小数")
    void validIncentiveIsScaledToFourDecimals() {
        respond("{\"amount\":2.5}");

        Optional<BigDecimal> incentive = incentiveService.getIncentive(transaction);

        assertThat(incentive).isPresent();
        assertThat(incentive.get()).isEqualByComparingTo("2.5");
        assertThat(incentive.get().scale()).isEqualTo(4);
        server.verify();
    }

    @Test
    @DisplayName("激励金额超过 4 位小数：按 0 处理，不做静默舍入")
    void excessiveScaleIncentiveIsTreatedAsZero() {
        respond("{\"amount\":0.12345}");

        assertThat(incentiveService.getIncentive(transaction)).isEmpty();
        server.verify();
    }

    @Test
    @DisplayName("激励金额为负：按 0 处理")
    void negativeIncentiveIsTreatedAsZero() {
        respond("{\"amount\":-3}");

        assertThat(incentiveService.getIncentive(transaction)).isEmpty();
        server.verify();
    }
}

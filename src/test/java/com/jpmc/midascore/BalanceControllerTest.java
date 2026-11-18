package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * /balance 的接口契约测试：200 有余额、404 查无此人、400 参数不合法且必须是 ProblemDetail 格式。
 * 只加载 Web 层，Repository 用 mock，不碰数据库。
 */
@WebMvcTest(BalanceController.class)
class BalanceControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private UserRepository userRepository;

    @Test
    @DisplayName("用户存在：返回 200 和余额")
    void returnsBalanceForExistingUser() throws Exception {
        UserRecord user = new UserRecord("Alice", new BigDecimal("100.0000"));
        when(userRepository.findById(1L)).thenReturn(Optional.of(user));

        mockMvc.perform(get("/balance").param("userId", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.userId").value(user.getId()))
                .andExpect(jsonPath("$.amount").value(100.0));
    }

    @Test
    @DisplayName("用户不存在：返回 404，而不是余额 0")
    void returnsNotFoundForUnknownUser() throws Exception {
        when(userRepository.findById(anyLong())).thenReturn(Optional.empty());

        mockMvc.perform(get("/balance").param("userId", "9999"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("userId 不是数字：返回 400 ProblemDetail")
    void returnsBadRequestForNonNumericUserId() throws Exception {
        mockMvc.perform(get("/balance").param("userId", "abc"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("参数格式错误"))
                .andExpect(jsonPath("$.detail").value("参数 userId 格式不正确"));
    }

    @Test
    @DisplayName("userId 非正数：返回 400 ProblemDetail")
    void returnsBadRequestForNonPositiveUserId() throws Exception {
        mockMvc.perform(get("/balance").param("userId", "0"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("参数校验失败"));
    }

    @Test
    @DisplayName("缺少 userId：返回 400 ProblemDetail")
    void returnsBadRequestForMissingUserId() throws Exception {
        mockMvc.perform(get("/balance"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("参数缺失"))
                .andExpect(jsonPath("$.detail").value("缺少必填参数 userId"));
    }
}

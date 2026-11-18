package com.jpmc.midascore;

import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * 读取 classpath 下的文本资源，供 Forage 的 Task*Tests 使用。
 *
 * <p>原来依赖 testcontainers 内部 shaded 的 commons-io（{@code org.testcontainers.shaded...IOUtils}），
 * 为了一个「读文件」的辅助功能背上一整个测试容器依赖、并且用到别人的内部包，属于不必要的耦合。
 * 这里改用 JDK 自带 API，行为保持一致（读不到时返回 null）。
 */
@Component
public class FileLoader {

    public String[] loadStrings(String path) {
        try (InputStream inputStream = this.getClass().getResourceAsStream(path)) {
            if (inputStream == null) {
                return null;
            }
            String fileText = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            return fileText.split(System.lineSeparator());
        } catch (Exception e) {
            return null;
        }
    }
}

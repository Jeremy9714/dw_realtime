package com.example.dw.realtime.console;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

/**
 * @Description:
 * @Author: Chenyang on 2024/12/05 19:47
 * @Version: 1.0
 */
@SpringBootApplication(scanBasePackages = "com.example.dw.realtime")
@ServletComponentScan("com.example.dw.realtime")
public class DWPublisherApplication {
    public static void main(String[] args) {
        SpringApplication.run(DWPublisherApplication.class, args);
    }
}

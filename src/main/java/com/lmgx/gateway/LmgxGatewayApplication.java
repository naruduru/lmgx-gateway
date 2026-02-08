package com.lmgx.gateway;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@MapperScan("com.lmgx.gateway.persist")
public class LmgxGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(LmgxGatewayApplication.class, args);
    }

}

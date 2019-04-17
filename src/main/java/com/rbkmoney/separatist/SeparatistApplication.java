package com.rbkmoney.separatist;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

@ServletComponentScan
@SpringBootApplication
public class SeparatistApplication extends SpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(SeparatistApplication.class, args);
    }

}

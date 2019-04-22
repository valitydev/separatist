package com.rbkmoney.separatist;

import com.rbkmoney.separatist.listener.StartupListener;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

import javax.annotation.PreDestroy;

@ServletComponentScan
@SpringBootApplication
@RequiredArgsConstructor
public class SeparatistApplication extends SpringApplication {

    private final StartupListener startupListener;

    public static void main(String[] args) {
        SpringApplication.run(SeparatistApplication.class, args);
    }

    @PreDestroy
    public void stop() {
        startupListener.stop();
    }

}

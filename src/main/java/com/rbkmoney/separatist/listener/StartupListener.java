package com.rbkmoney.separatist.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class StartupListener implements ApplicationListener<ContextRefreshedEvent> {

    private final Topology deduplicationTopology;
    private final Properties deduplicationStreamProperties;

    private KafkaStreams deduplicationKafkaStream;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        deduplicationKafkaStream = new KafkaStreams(deduplicationTopology, deduplicationStreamProperties);
        deduplicationKafkaStream.start();
        log.info("StartupListener start stream kafkaStreams: {}", deduplicationKafkaStream.allMetadata());
    }

    public void stop() {
        deduplicationKafkaStream.close(Duration.ofSeconds(60L));
    }

} 
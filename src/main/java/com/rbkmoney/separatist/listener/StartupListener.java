package com.rbkmoney.separatist.listener;

import com.rbkmoney.separatist.stream.DeduplicationStreamFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class StartupListener implements ApplicationListener<ContextRefreshedEvent> {

    private final DeduplicationStreamFactory deduplicationStreamFactory;
    private final Properties deduplicationStreamProperties;

    private KafkaStreams kafkaStreams;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        kafkaStreams = deduplicationStreamFactory.create(deduplicationStreamProperties);
        kafkaStreams.start();

        log.info("StartupListener start stream kafkaStreams: {}", kafkaStreams.allMetadata());
    }

    public void stop() {
        kafkaStreams.close(Duration.ofSeconds(60L));
    }

} 
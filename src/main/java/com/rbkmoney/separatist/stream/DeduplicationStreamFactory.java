package com.rbkmoney.separatist.stream;

import com.rbkmoney.separatist.config.KafkaConfig;
import com.rbkmoney.separatist.serde.SinkEventSerde;
import com.rbkmoney.separatist.transformer.DeduplicationTransformer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeduplicationStreamFactory {

    @Value("${kafka.task.deduplication.input.topic}")
    private String inputTopic;
    @Value("${kafka.task.deduplication.output.topic}")
    private String outputTopic;

    private final StoreBuilder storeBuilder;
    private final SinkEventSerde sinkEventSerde = new SinkEventSerde();

    public KafkaStreams create(final Properties deduplicationStreamProperties) {
        try {
            StreamsBuilder builder = new StreamsBuilder();
            builder.addStateStore(storeBuilder);
            builder.stream(inputTopic, Consumed.with(Serdes.String(), sinkEventSerde))
                    .peek((key, sinkEvent) -> log.info("Deduplication stream processing key {} sink event: {}", key, sinkEvent))
                    .transform(DeduplicationTransformer::new, KafkaConfig.DEDUPLICATION_STORE)
                    .peek((key, sinkEvent) -> log.info("Result of deduplication: key {} event {}", key, sinkEvent))
                    .to(outputTopic, Produced.with(Serdes.String(), sinkEventSerde));

            return new KafkaStreams(builder.build(), deduplicationStreamProperties);

        } catch (Exception e) {
            log.error("DeduplicationStreamFactory error when create stream e: {}", e);
            throw new RuntimeException(e);
        }
    }

}
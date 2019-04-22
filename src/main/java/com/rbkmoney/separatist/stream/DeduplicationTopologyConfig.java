package com.rbkmoney.separatist.stream;

import com.rbkmoney.separatist.serde.SinkEventSerde;
import com.rbkmoney.separatist.transformer.DeduplicationTransformer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.rbkmoney.separatist.config.DeduplicationClientConfig.DEDUPLICATION_STORE;

@Slf4j
@Configuration
public class DeduplicationTopologyConfig {

    @Bean
    @Qualifier("deduplicationTopology")
    public Topology deduplicationTopology(@Value("${kafka.task.deduplication.input.topic}") String inputTopic,
                                          @Value("${kafka.task.deduplication.output.topic}") String outputTopic,
                                          StoreBuilder deduplicationStoreBuilder,
                                          SinkEventSerde sinkEventSerde) {
        try {
            StreamsBuilder builder = new StreamsBuilder();
            builder.addStateStore(deduplicationStoreBuilder);
            builder.stream(inputTopic, Consumed.with(Serdes.String(), sinkEventSerde))
                    .peek((key, sinkEvent) -> log.info("Deduplication stream processing key {} sink event: {}", key, sinkEvent))
                    .transform(DeduplicationTransformer::new, DEDUPLICATION_STORE)
                    .peek((key, sinkEvent) -> log.info("Result of deduplication: key {} event {}", key, sinkEvent))
                    .to(outputTopic, Produced.with(Serdes.String(), sinkEventSerde));

            return builder.build();

        } catch (Exception e) {
            log.error("DeduplicationStreamFactory error when create stream e: {}", e);
            throw new RuntimeException(e);
        }
    }

    @Bean
    @Qualifier("deduplicationStoreBuilder")
    public StoreBuilder storeBuilder() {
        return new KeyValueStoreBuilder<>(Stores.persistentKeyValueStore(DEDUPLICATION_STORE),
                new Serdes.StringSerde(),
                new Serdes.LongSerde(),
                Time.SYSTEM);
    }

    @Bean
    public SinkEventSerde sinkEventSerde() {
        return new SinkEventSerde();
    }

}

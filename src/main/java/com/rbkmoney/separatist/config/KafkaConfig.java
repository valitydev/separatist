package com.rbkmoney.separatist.config;

import com.rbkmoney.separatist.serde.SinkEventSerde;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.util.Properties;

@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    private static final String APP_ID = "separatist";
    private static final String CLIENT_ID = "separatist-client-test";
    public static final String PKCS_12 = "PKCS12";
    public static final String DEDUPLICATION_STORE = "deduplication-store";

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;
    @Value("${kafka.concurrency}")
    private int concurrency;
    @Value("${kafka.ssl.server-password}")
    private String serverStorePassword;
    @Value("${kafka.ssl.server-keystore-location}")
    private String serverStoreCertPath;
    @Value("${kafka.ssl.keystore-password}")
    private String keyStorePassword;
    @Value("${kafka.ssl.key-password}")
    private String keyPassword;
    @Value("${kafka.ssl.keystore-location}")
    private String clientStoreCertPath;
    @Value("${kafka.ssl.enable}")
    private boolean kafkaSslEnable;
    @Value("${kafka.streams.replication-factor}")
    private int replicationFactor;

    @Bean
    public Properties deduplicationStreamProperties() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SinkEventSerde.class);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, replicationFactor - 1); //todo test on env
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, concurrency);
        if (kafkaSslEnable) {
            props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, new File(serverStoreCertPath).getAbsolutePath());
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, serverStorePassword);
            props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, PKCS_12);
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, PKCS_12);
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, new File(clientStoreCertPath).getAbsolutePath());
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        }
        initStreamsConsumerConfig(props);
        initStreamsProducerConfig(props);
        initStreamsTopicConfig(props);
        return props;
    }

    private void initStreamsTopicConfig(Properties props) {
//        props.put(StreamsConfig.topicPrefix(TopicConfig.RETENTION_MS_CONFIG), -1);
    }

    private void initStreamsProducerConfig(Properties props) {
        props.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");

    }

    private void initStreamsConsumerConfig(Properties props) {

    }

    @Bean
    public StoreBuilder storeBuilder() {
        return new KeyValueStoreBuilder<>(Stores.persistentKeyValueStore(DEDUPLICATION_STORE),
                new Serdes.StringSerde(),
                new Serdes.LongSerde(),
                Time.SYSTEM);
    }


}


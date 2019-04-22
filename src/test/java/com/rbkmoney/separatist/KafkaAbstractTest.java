package com.rbkmoney.separatist;

import com.rbkmoney.machinegun.eventsink.MachineEvent;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.separatist.serde.SinkEventDeserializer;
import com.rbkmoney.separatist.serde.SinkEventSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.KafkaContainer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ContextConfiguration(initializers = KafkaAbstractTest.Initializer.class)
public abstract class KafkaAbstractTest {

    private static final String CONFLUENT_PLATFORM_VERSION = "5.0.1";

    public static String inputTopic = "testInput";
    public static String outputTopic = "testOutput";

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(CONFLUENT_PLATFORM_VERSION).withEmbeddedZookeeper();

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues values = TestPropertyValues
                    .of("kafka.bootstrap.servers=" + kafka.getBootstrapServers(),
                        "kafka.task.deduplication.input.topic=" + inputTopic,
                        "kafka.task.deduplication.output.topic=" + outputTopic);
            values.applyTo(configurableApplicationContext);
        }
    }

    private static Producer<String, SinkEvent> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client_id");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SinkEventSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void writeToTopic(List<SinkEvent> sinkEvents) throws InterruptedException {
        Producer<String, SinkEvent> producer = createProducer();
        for (SinkEvent sinkEvent : sinkEvents) {
            ProducerRecord<String, SinkEvent> producerRecord = new ProducerRecord<>(inputTopic,
                    sinkEvent.getEvent().source_id + " " + sinkEvent.getEvent().event_id,
                    sinkEvent);
            try {
                producer.send(producerRecord).get();
            } catch (Exception e) {
                log.error("KafkaAbstractTest initialize e: ", e);
            }
        }
        waitForTopicSync();
        producer.close();
    }

    public static void waitForTopicSync() throws InterruptedException {
        Thread.sleep(1000L);
    }


    public static SinkEvent createMessage(Long eventId, String sourceId) {
        MachineEvent message = new MachineEvent();
        com.rbkmoney.machinegun.msgpack.Value data = new com.rbkmoney.machinegun.msgpack.Value();
        data.setBin(new byte[0]);
        message.setCreatedAt(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
        message.setEventId(eventId);
        message.setSourceNs("source_namespace");
        message.setSourceId(sourceId);
        message.setData(data);
        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setEvent(message);
        return sinkEvent;
    }

    public static Consumer<String, SinkEvent> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client_id");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SinkEventDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "TestListener2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

}
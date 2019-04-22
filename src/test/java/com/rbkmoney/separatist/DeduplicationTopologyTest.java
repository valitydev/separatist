package com.rbkmoney.separatist;

import com.rbkmoney.machinegun.eventsink.MachineEvent;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.separatist.config.DeduplicationClientConfig;
import com.rbkmoney.separatist.serde.SinkEventDeserializer;
import com.rbkmoney.separatist.serde.SinkEventSerializer;
import com.rbkmoney.separatist.stream.DeduplicationTopologyConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.rbkmoney.separatist.config.DeduplicationClientConfig.DEDUPLICATION_STORE;


@Slf4j
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {DeduplicationTopologyConfig.class, DeduplicationClientConfig.class})
@TestPropertySource(properties = {
        "kafka.task.deduplication.input.topic=inputTopic",
        "kafka.task.deduplication.output.topic=outputTopic",
        "kafka.bootstrap.servers=test:9091",
        "kafka.concurrency=5",
        "kafka.ssl.server-password=",
        "kafka.ssl.server-keystore-location=",
        "kafka.ssl.keystore-password=",
        "kafka.ssl.key-password=",
        "kafka.ssl.keystore-location=",
        "kafka.ssl.enable=false",
        "kafka.streams.replication-factor=1"
})
public class DeduplicationTopologyTest {

    @Autowired
    Topology deduplicationTopology;

    @Autowired
    Properties deduplicationStreamProperties;

    @Value("${kafka.task.deduplication.input.topic}")
    String inputTopic;

    @Value("${kafka.task.deduplication.output.topic}")
    String outputTopic;

    private static final int TOTAL = 3;

    private TopologyTestDriver topologyTestDriver;

    @Before
    public void init() {
        topologyTestDriver = new TopologyTestDriver(deduplicationTopology, deduplicationStreamProperties);
    }

    @After
    public void close() {
        topologyTestDriver.close();
    }

    @Test
    public void testSimpleDeduplication() {
        initInputTopic();

        assertOutputTopicDeduplicated();
    }

    private void assertOutputTopicDeduplicated() {
        List<ProducerRecord<String, SinkEvent>> list = new ArrayList<>();
        ProducerRecord<String, SinkEvent> stringSinkEventProducerRecord;

        while ((stringSinkEventProducerRecord = topologyTestDriver.readOutput(outputTopic, new StringDeserializer(), new SinkEventDeserializer())) != null) {
            list.add(stringSinkEventProducerRecord);
        }

        Assert.assertEquals(10, list.size());

        KeyValueStore<Object, Object> keyValueStore = topologyTestDriver.getKeyValueStore(DEDUPLICATION_STORE);
        Assert.assertEquals(2L, keyValueStore.get("0"));
        Assert.assertEquals(2L, keyValueStore.get("1"));
        Assert.assertEquals(2L, keyValueStore.get("2"));
        Assert.assertEquals(0L, keyValueStore.get("3"));
        Assert.assertNull(keyValueStore.get("666"));
    }

    private void initInputTopic() {
        ConsumerRecordFactory<String, SinkEvent> recordFactory = new ConsumerRecordFactory<>(inputTopic, new StringSerializer(), new SinkEventSerializer());
        List<ConsumerRecord<byte[], byte[]>> eventsToWrite = new ArrayList<>();
        for (int i = 0; i < TOTAL; i++) {
            for (int j = 0; j < TOTAL; j++) {
                for (int k = 0; k < TOTAL; k++) {
                    SinkEvent message = createMessage((long) j, i + "");
                    eventsToWrite.add(recordFactory.create(inputTopic, message.getEvent().getSourceId(), message));
                }
            }
        }
        SinkEvent message = createMessage((long) 0, "3");
        eventsToWrite.add(recordFactory.create(inputTopic, message.getEvent().getSourceId(), message));
        topologyTestDriver.pipeInput(eventsToWrite);
    }

    private SinkEvent createMessage(Long eventId, String sourceId) {
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

}

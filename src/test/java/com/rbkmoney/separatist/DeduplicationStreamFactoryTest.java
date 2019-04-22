package com.rbkmoney.separatist;

import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.separatist.stream.DeduplicationStreamFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@ContextConfiguration(classes = {SeparatistApplication.class})
public class DeduplicationStreamFactoryTest extends KafkaAbstractTest {

    @Autowired
    DeduplicationStreamFactory deduplicationStreamFactory;

    @Autowired
    Properties deduplicationStreamProperties;

    private static final int TOTAL = 3;

    @BeforeClass
    public static void init() throws InterruptedException {
        initTopic();
    }

    @Test
    public void test() throws InterruptedException {
        waitForTopicSync();

        assertOutputTopicDeduplicated();
    }

    private void assertOutputTopicDeduplicated() {
        Consumer<String, SinkEvent> consumer = createConsumer();
        consumer.subscribe(List.of(outputTopic));
        ConsumerRecords<String, SinkEvent> records = consumer.poll(Duration.ofSeconds(10));
        Assert.assertEquals(TOTAL, records.count());
    }

    private static void initTopic() throws InterruptedException {
        List<SinkEvent> eventsToWrite = new ArrayList<>();
        for (int i = 0; i < TOTAL; i++) {
            for (int j = 0; j < TOTAL; j++) {
                eventsToWrite.add(createMessage((long) j, j + ""));
            }
        }
        writeToTopic(eventsToWrite);
    }

}

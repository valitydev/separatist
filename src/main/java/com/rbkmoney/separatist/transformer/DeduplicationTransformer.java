package com.rbkmoney.separatist.transformer;

import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.separatist.config.KafkaConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class DeduplicationTransformer implements Transformer<String, SinkEvent, KeyValue<String, SinkEvent>> {

    private ProcessorContext context;

    private KeyValueStore<String, Long> lastEventStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;
        this.lastEventStore = (KeyValueStore<String, Long>) context.getStateStore(KafkaConfig.DEDUPLICATION_STORE);
    }

    @Override
    public KeyValue<String, SinkEvent> transform(String key, SinkEvent event) {
        Long lastEventId = lastEventStore.get(key);
        if (lastEventId != null && event.getEvent().getEventId() <= lastEventId) {
            return null;
        } else {
            lastEventStore.put(key, event.getEvent().getEventId());
            return KeyValue.pair(key, event);
        }
    }

    @Override
    public void close() {
        //  todo check this
        // Note: The store should NOT be closed manually here via `eventIdStore.close()`!
        // The Kafka Streams API will automatically close stores when necessary.
    }
}

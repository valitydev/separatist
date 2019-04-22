package com.rbkmoney.separatist.serde;

import com.rbkmoney.machinegun.eventsink.SinkEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class SinkEventSerde implements Serde<SinkEvent> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<SinkEvent> serializer() {
        return new SinkEventSerializer();
    }

    @Override
    public Deserializer<SinkEvent> deserializer() {
        return new SinkEventDeserializer();
    }
}

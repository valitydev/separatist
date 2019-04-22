package com.rbkmoney.separatist.serde;

import com.rbkmoney.deserializer.AbstractDeserializerAdapter;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SinkEventDeserializer extends AbstractDeserializerAdapter<SinkEvent> {

    @Override
    public SinkEvent deserialize(String topic, byte[] data) {
        log.debug("Message, topic: {}, byteLength: {}", topic, data.length);
        return deserialize(data, new SinkEvent());
    }

}
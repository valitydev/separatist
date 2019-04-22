package com.rbkmoney.separatist.serde;

import com.rbkmoney.machinegun.eventsink.SinkEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.Map;

@Slf4j
public class SinkEventDeserializer implements Deserializer<SinkEvent> {

    private ThreadLocal<TDeserializer> tDeserializerThreadLocal = ThreadLocal.withInitial(() -> new TDeserializer(new TBinaryProtocol.Factory()));

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public SinkEvent deserialize(String topic, byte[] data) {
        log.debug("Message, topic: {}, byteLength: {}", topic, data.length);
        SinkEvent machineEvent = new SinkEvent();
        try {
            tDeserializerThreadLocal.get().deserialize(machineEvent, data);
        } catch (Exception e) {
            log.error("Error when deserialize SinkEvent data: {} ", data, e);
        }
        return machineEvent;
    }

    @Override
    public void close() {

    }

}
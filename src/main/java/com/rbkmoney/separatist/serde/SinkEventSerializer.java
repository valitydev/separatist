package com.rbkmoney.separatist.serde;

import com.rbkmoney.machinegun.eventsink.SinkEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.Map;

@Slf4j
public class SinkEventSerializer implements Serializer<SinkEvent> {

    private ThreadLocal<TSerializer> tSerializerThreadLocal = ThreadLocal.withInitial(() -> new TSerializer(new TBinaryProtocol.Factory()));

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, SinkEvent data) {
        try {
            return tSerializerThreadLocal.get().serialize(data);
        } catch (Exception e) {
            log.error("Error when serialize SinkEvent data: {} ", data, e);
        }
        return null;
    }

    @Override
    public void close() {

    }

}


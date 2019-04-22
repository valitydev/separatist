package com.rbkmoney.separatist.serde;

import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.serializer.ThriftSerializer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SinkEventSerializer extends ThriftSerializer<SinkEvent> {}


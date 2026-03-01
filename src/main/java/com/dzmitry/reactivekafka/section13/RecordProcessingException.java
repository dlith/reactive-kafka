package com.dzmitry.reactivekafka.section13;

import reactor.kafka.receiver.ReceiverRecord;

public class RecordProcessingException extends RuntimeException {

    private final ReceiverRecord<?, ?> record;

    public RecordProcessingException(ReceiverRecord<?, ?> receiverRecord, Throwable e) {
        super(e);
        this.record = receiverRecord;
    }

    @SuppressWarnings("unchecked")
    public <K, V> ReceiverRecord<K, V> getRecord() {
        return (ReceiverRecord<K, V>) record;
    }
}

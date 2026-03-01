package com.dzmitry.reactivekafka.section13;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class OrderEventProcessor {

    private final ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer;

    public OrderEventProcessor(ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer) {
        this.deadLetterTopicProducer = deadLetterTopicProducer;
    }

    public Mono<Void> process(ReceiverRecord<String, String> record) {
        return Mono.just(record)
                .doOnNext(r -> {
                    if (r.key().endsWith("5")) {
                        throw new RuntimeException("processing exception");
                    }
                    log.info("key: {}, value: {}", r.key(), r.value());
                    r.receiverOffset().acknowledge();
                })
                .onErrorMap(ex -> new RecordProcessingException(record, ex))
                .transform(this.deadLetterTopicProducer.recordProcessingErrorHandler())
                .then();
    }
}

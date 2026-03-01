package com.dzmitry.reactivekafka.section13;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.retry.Retry;

import java.util.function.Function;

@Slf4j
@Data
@AllArgsConstructor
public class ReactiveDeadLetterTopicProducer<K, V> {

    private final KafkaSender<K, V> sender;
    private final Retry retrySpec;

    public Mono<SenderResult<K>> produce(ReceiverRecord<K, V> record) {
        SenderRecord<K, V, K> sr = toSenderRecord(record);
        return this.sender.send(Mono.just(sr)).next();
    }

    private SenderRecord<K, V, K> toSenderRecord(ReceiverRecord<K, V> record) {
        ProducerRecord<K, V> pr = new ProducerRecord<>(
                record.topic() + "-dlt",
                record.key(),
                record.value()
        );
        return SenderRecord.create(pr, pr.key());
    }

    public Function<Mono<ReceiverRecord<K, V>>, Mono<Void>> recordProcessingErrorHandler() {
        return mono -> mono.retryWhen(this.retrySpec)
                .onErrorMap(ex -> ex.getCause() instanceof RecordProcessingException, Throwable::getCause)
                .doOnError(ex -> log.error(ex.getMessage()))
                .onErrorResume(RecordProcessingException.class, ex -> this.produce(ex.getRecord())
                        .then(Mono.fromRunnable(() -> ex.getRecord().receiverOffset().acknowledge()))
                )
                .then();
    }
}

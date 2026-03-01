package com.dzmitry.reactivekafka.section12;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class KafkaConsumerV3 {

    public static void main(String[] args) {

        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "host.docker.internal:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        ReceiverOptions<Object, Object> options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events"));

        KafkaReceiver.create(options)
                .receive()
                .log()
                .concatMap(KafkaConsumerV3::process)
                .subscribe();
    }

    private static Mono<Void> process(ReceiverRecord<Object, Object> receiverRecord) {
        return Mono.just(receiverRecord)
                .doOnNext(r -> {
                    if (r.key().equals("5")) {
                        throw new RuntimeException("DB is down");
                    }
                    int index = ThreadLocalRandom.current().nextInt(1, 10);
                    log.info("key: {}, index: {} value: {}", r.key(), index, r.value().toString().toCharArray()[index]);
                    r.receiverOffset().acknowledge();
                })
                .retryWhen(retrySpec())
                .doOnError(ex -> log.error(ex.getMessage()))
                .onErrorResume(IndexOutOfBoundsException.class, ex -> Mono.fromRunnable(()-> receiverRecord.receiverOffset().acknowledge()))
                .then();
    }

    private static Retry retrySpec() {
        return Retry.fixedDelay(3, Duration.ofSeconds(1))
                .filter(IndexOutOfBoundsException.class::isInstance)
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }
}

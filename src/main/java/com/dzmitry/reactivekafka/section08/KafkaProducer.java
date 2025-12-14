package com.dzmitry.reactivekafka.section08;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;

@Slf4j
public class KafkaProducer {

    public static void main(String[] args) {

        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "host.docker.internal:8081",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        SenderOptions<String, String> senderOptions = SenderOptions.create(config);

        Flux<SenderRecord<String, String, String>> flux = Flux.interval(Duration.ofMillis(50))
                .take(10_000)
                .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
                .map(pr -> SenderRecord.create(pr, pr.key()));

        KafkaSender.create(senderOptions)
                .send(flux)
                .doOnNext(r -> log.info("correlation id: {}", r.correlationMetadata()))
                .subscribe();
    }
}

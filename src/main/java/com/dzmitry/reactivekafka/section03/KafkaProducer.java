package com.dzmitry.reactivekafka.section03;

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
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "host.docker.internal:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        SenderOptions<String, String> senderOptions = SenderOptions.<String, String>create(config)
                .maxInFlight(10_000);

        Flux<SenderRecord<String, String, String>> flux =
                Flux.range(1, 1_000_000)
                        .take(100)
                        .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
                        .map(pr -> SenderRecord.create(pr, pr.key()));

        long start = System.currentTimeMillis();

        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);
        sender
                .send(flux).doOnNext(r -> log.info("correlation id: {}", r.correlationMetadata()))
                .doOnComplete(() -> {
                    log.info("total time take: {} ms", (System.currentTimeMillis() - start));
                    sender.close();
                }).subscribe();
    }
}

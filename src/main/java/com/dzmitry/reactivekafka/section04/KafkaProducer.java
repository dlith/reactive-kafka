package com.dzmitry.reactivekafka.section04;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;

@Slf4j
public class KafkaProducer {

    public static void main(String[] args) {

        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "host.docker.internal:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        SenderOptions<String, String> senderOptions = SenderOptions.<String, String>create(config);

        Flux<SenderRecord<String, String, String>> flux =
                Flux.range(1, 10).map(KafkaProducer::createSenderRecord);

        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);
        sender
                .send(flux).doOnNext(r -> log.info("correlation id: {}", r.correlationMetadata()))
                .doOnComplete(sender::close)
                .subscribe();
    }


    private static SenderRecord<String, String, String> createSenderRecord(Integer i) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("client-id", "some-client-id".getBytes());
        headers.add("tracing-id", "123".getBytes());
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("order-events", null, i.toString(), "order-" + i, headers);
        return SenderRecord.create(producerRecord, producerRecord.key());
    }
}

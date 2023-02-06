package com.hieuphamduy.kafka.core;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

  public static void main(String[] args) {
    final var props = new Properties();
    props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "hieupd-producer");
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(64 * 1024 * 1024L));
    props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(2048 * 2));
    props.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    try (var producer = new KafkaProducer<String, String>(props)) {
      for (int i = 0; i < 100; i++) {
        final var message =
            new ProducerRecord<>(
                "new-kafka-topic", // topic name
                "key-" + i, // key
                "message: " + i // value
                );
        producer.send(message);
      }
    }
  }
}

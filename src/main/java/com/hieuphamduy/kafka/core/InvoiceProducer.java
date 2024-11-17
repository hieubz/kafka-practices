package com.hieuphamduy.kafka.core;

import com.hieuphamduy.kafka.config.JsonSerializer;
import com.hieuphamduy.kafka.model.Invoice;
import org.apache.kafka.clients.producer.KafkaProducer;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

public class InvoiceProducer {

  public static void main(String[] args) {

    final var random = new Random();
    final var props = new Properties();
    props.put(CLIENT_ID_CONFIG, "producer");
    props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
    props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    props.put(RETRIES_CONFIG, 10); // max 10 times
    props.put(RETRY_BACKOFF_MS_CONFIG, 500); // wait 0.5s between retries
    props.put(DELIVERY_TIMEOUT_MS_CONFIG, 5000); // 5 seconds timeout

    try (var producer = new KafkaProducer<String, Invoice>(props)) {
      IntStream.range(0, 1000)
          .parallel()
          .forEach(
              i -> {
                final var invoice =
                    Invoice.builder()
                        .invoiceNumber(String.format("%05d", i))
                        .storeId(i % 5 + "")
                        .created(System.currentTimeMillis())
                        .valid(random.nextBoolean())
                        .build();
                producer.send(new ProducerRecord<>("invoice-topic", invoice));
              });
    }
  }
}

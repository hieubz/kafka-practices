package com.hieuphamduy.kafka.core;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TransactionProducer {
  public static void main(String[] args) {
    final var props = new Properties();
    props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "java-producer");
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "demo-transaction-id");
    props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

    final var producer = new KafkaProducer<String, String>(props);
    producer.initTransactions();

    try {
      producer.beginTransaction();
      producer.send(new ProducerRecord<>("transaction-topic-1", "Message to topic 1"));
      producer.send(new ProducerRecord<>("transaction-topic-2", "Message to topic 2"));
      producer.commitTransaction();
    } catch (Exception ex) {
      producer.abortTransaction();
      producer.close();
      throw new RuntimeException(ex);
    }
    producer.close();
  }
}

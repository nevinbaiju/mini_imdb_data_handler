package com.alpha.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.util.Properties;

public class producer {

    public static void main(String[] args) {
        // Kafka producer configurations
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create a Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Kafka topic to produce messages to
        String topic = "movie_reviews";
        System.out.println("Starting to send messages.");

        try {
            while (true) {
                // Generate JSON message
                JSONObject movieData = new JSONObject();
                movieData.put("movie_name", "Avatar");
                movieData.put("review_score", 4.5);

                // Produce the JSON message
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, movieData.toString());
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Message delivery failed: " + exception.getMessage());
                    } else {
                        System.out.println("Message delivered to " + metadata.topic() +
                                " [" + metadata.partition() + "] at offset " + metadata.offset());
                    }
                });
                System.out.println("Sent and waiting.");
                Thread.sleep(1000); // Wait for 1 second before producing the next message
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("Oombalu thanne");
        } finally {
            producer.close();
        }
    }
}

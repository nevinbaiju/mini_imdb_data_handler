package com.alpha.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;

public class consumer {

    public static void main(String[] args) {
        // Kafka consumer configurations
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create a Kafka consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        // Kafka topic to consume messages from
        String topic = "movie_reviews";

        // Subscribe to the topic
        consumer.subscribe(Collections.singleton(topic));

        try {
            String buffer = "";
            int line_count = 0;
            while (true) {
                // Poll for new messages
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    // Print received message
                    if(line_count == 50000){
                        LocalDateTime currentDateTime = LocalDateTime.now();
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
                        String timeStamp = currentDateTime.format(formatter);
                        try (BufferedWriter writer = new BufferedWriter(new FileWriter(String.format("data/dumps/ratings_dump_%s.csv", timeStamp), true))) {
                            System.out.println("Dumped 50000 lines.");
                            writer.write(buffer);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        buffer = "";
                        line_count = 0;
                    }
                    line_count += 1;
                    String rating_string = record.value();
                    JSONObject jsonObject = new JSONObject(rating_string);
                    
                    buffer = buffer + jsonObject.getInt("movie_id") + ',';
                    buffer = buffer + jsonObject.getString("title") + ',';
                    buffer = buffer + jsonObject.getDouble("rating") + ',';
                    buffer = buffer + jsonObject.getDouble("old_rating") + '\n';
                    
                    // System.out.println("Line Count: " + line_count);
                    // System.out.println("Received message: " + rating_string + "\n");
                    // System.out.println("Received message: \n" + buffer);
                }
            }
        } finally {
            consumer.close();
        }
    }
}

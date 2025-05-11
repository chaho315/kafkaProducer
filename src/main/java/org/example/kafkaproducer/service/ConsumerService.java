package org.example.kafkaproducer.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {

    @KafkaListener(topics = "fastTopic", groupId = "spring")
    public void consumer(String message) {
        System.out.println("Subscribed : %s".formatted(message));
    }
}

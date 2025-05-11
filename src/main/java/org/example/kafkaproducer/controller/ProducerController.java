package org.example.kafkaproducer.controller;

import org.example.kafkaproducer.service.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    Producer producer;

    @Autowired
    ProducerController(Producer producer) {
        this.producer = producer;
    }

    @PostMapping("/message")
    public void publishMessage(@RequestParam String msg) {
        producer.pub(msg);
    }
}

package org.example.kafkaproducer.service;

import org.example.kafkaproducer.config.KafkaConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Producer {
    @Autowired
    KafkaConfig myConfig;

    String topicName = "fastTopic";
    private KafkaTemplate<String, Object> kafkaTemplate;

    /*@Autowired
    public Producer(KafkaTemplate kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }*/

    public void pub(String msg) {
        kafkaTemplate = myConfig.KafkaTemplateForGeneral();
        kafkaTemplate.send(topicName, msg);
    }

    public void sendJoinedMsg (String topicNm, Object msg) {
        kafkaTemplate = myConfig.KafkaTemplateForGeneral();
        kafkaTemplate.send(topicNm, msg);
    }

    public void sendMsgForWatchingAdLog (String topicNm, Object msg) {
        kafkaTemplate = myConfig.KafkaTemplateForWatchingAdLog();
        kafkaTemplate.send(topicNm, msg);
    }

    public void sendMsgForPurchaseLog (String topicNm, Object msg) {
        kafkaTemplate = myConfig.KafkaTemplateForPurchaseLog();
        kafkaTemplate.send(topicNm, msg);
    }
}

package com.rajanthari.javaconfigkafkaconsumer.config;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class TopicListener {

    @KafkaListener(topics = "mytopic", groupId = "myconfig-consumer")
    public void getTopicMessage(@Payload String message,
                                @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        System.out.println("Topic received: " + message + " Partition: " + partition);
    }
}

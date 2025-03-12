package com.rajanthari.springbootkafkaproducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/send")
public class MessageRestController {

    @Autowired
        KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping
    public String sendMessage(@RequestParam String message) {
        String result = "";
        CompletableFuture<SendResult<String, String>> mytopic = kafkaTemplate.send("mytopic", message);
        try {
            SendResult<String, String> stringStringSendResult = mytopic.get();
            result = " Partition Number : " + stringStringSendResult.getProducerRecord().partition();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return "Message sent: " + message + result;
    }
}

package com.johanna.springsimplekafka.controller;

import com.johanna.springsimplekafka.kafka.Producer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/v1/kafka")
public class MessageController {

    private final Producer producer;

    public MessageController(Producer producer) {
        this.producer = producer;
    }

    //localhost:8080/api/v1/kafka/publish?message=""
    @GetMapping("/publish")
    public ResponseEntity<String> publish(@RequestParam("message") String message){
        producer.sendMessage(message);
        return ResponseEntity.ok("Message published to topic 1");
    }
}

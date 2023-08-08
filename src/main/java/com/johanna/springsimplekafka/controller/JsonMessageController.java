package com.johanna.springsimplekafka.controller;

import com.johanna.springsimplekafka.kafka.JsonProducer;
import com.johanna.springsimplekafka.pojo.User;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/v1/kafka")
public class JsonMessageController {

    private final JsonProducer jsonProducer;

    public JsonMessageController(JsonProducer jsonProducer) {
        this.jsonProducer = jsonProducer;
    }

    @PostMapping("/publish")
    public ResponseEntity<String> publish(@RequestBody User user) {
        jsonProducer.sendMessage(user);
        return ResponseEntity.ok("Json message published to kafka");
    }

}

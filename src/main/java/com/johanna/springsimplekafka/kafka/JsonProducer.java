package com.johanna.springsimplekafka.kafka;

import com.johanna.springsimplekafka.pojo.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class JsonProducer {

    @Value("${spring.kafka.topic-json.name}")
    private String jsonTopicName;

    private final static Logger LOGGER = LoggerFactory.getLogger(JsonProducer.class);

    private final KafkaTemplate<String, User> kafkaTemplate;

    public JsonProducer(KafkaTemplate<String, User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(User data){

        LOGGER.info(String.format("Message published -> %s", data.toString()));

        Message<User> message = MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.TOPIC, jsonTopicName)
                .build();

        kafkaTemplate.send(message);

    }
}

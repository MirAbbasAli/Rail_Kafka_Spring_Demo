package com.rail.receiver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class ReceiverApplication {
    public static void main(String[] args) {
        SpringApplication.run(ReceiverApplication.class, args);
    }
    /**
     * *
     * @param message This annotation taken the name and id of the topic passed as arguments
     * and will be listening to this topic "Rail_CityA_To_CityB". Whenever a message is posted to this topic,
     * instantly that message will be consumed by this method and will be show in the console
     */
    @KafkaListener(id="RailA2B_1", topics="Rail_CityA_To_CityB")
    public void listenToTopic(String message) {
        System.out.println("Message Received is: "+message);
    }
}
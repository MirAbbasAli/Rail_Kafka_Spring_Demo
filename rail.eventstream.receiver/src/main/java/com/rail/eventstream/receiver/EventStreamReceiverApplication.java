package com.rail.eventstream.receiver;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
@EnableKafkaStreams
public class EventStreamReceiverApplication {
    public static void main(String[] args) {
        SpringApplication.run(EventStreamReceiverApplication.class, args);
    }

    @KafkaListener(id="RC2D_2", topics="Rail_C_To_D_Brake_Warning")
    public void listenToTopic2(ConsumerRecord<String, Long> message){
        System.out.println("Receiving "+message.key()+" as "+message.value()+" %.");
    }
}
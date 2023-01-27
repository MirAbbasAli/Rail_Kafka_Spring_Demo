package com.rail.consumerrecord;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class RailConsumerRecordApplication {
    public static void main(String[] args) {
        SpringApplication.run(RailConsumerRecordApplication.class, args);
    }

    @Bean
    public NewTopic topic(){
        return TopicBuilder.name("Rail_C_To_D")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public ApplicationRunner sendMessage(KafkaTemplate<String, String> template){
        return ars -> {
            for(int i=0;i<500;i++){
                template.send("Rail_C_To_D", "Speed", String.valueOf(Math.round(Math.random()*1000)));
                template.send("Rail_C_To_D", "BreakEfficiency", String.valueOf(Math.round(Math.random()*100)));
                System.out.println("Sender Module: ");
                System.out.println("Speed and breaking efficiency information sent successfully");
                Thread.sleep(2000,0);
            }
        };
    }

    @KafkaListener(id="RailC2D_1", topics="Rail_C_To_D")
    public void listenToTopic(ConsumerRecord<String, String> record){
        System.out.println("Receiver Module");
        System.out.println("key: "+record.key());
        System.out.println("value: "+record.value());
        System.out.println("partition: "+record.partition());
        System.out.println("topic: "+record.topic());
        System.out.println("timestamp: "+record.timestamp());
        System.out.println("offset: "+record.offset());
        System.out.println("--------------------------");
    }
}
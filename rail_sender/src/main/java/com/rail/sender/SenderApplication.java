package com.rail.sender;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class SenderApplication {
    public static void main(String[] args) {
        SpringApplication.run(SenderApplication.class,args);
    }

    /**
     * Create a Topic to send our messages
     * */
    @Bean
    public NewTopic topic() {
        return TopicBuilder.name("Rail_CityA_To_CityB")
                .partitions(1)
                .replicas(1)
                .build();
    }
    /**
     * Publish 500 Message with 500 random train speed from City A to City B in Kafka Topic Rail_CityA_to_CityB
     * To send message continuously, we have a for loop running for 500 times with 3 seconds of pause after sending message each time.
     * */
    @Bean
    public ApplicationRunner sendMessage(KafkaTemplate<String, String> template) {
        return args -> {
            for(int i=0; i<500; i++) {
                template.send("Rail_CityA_To_CityB", "Current Speed of Train is: "+ String.valueOf(Math.round(Math.random()*100))+" kmph.");
                System.out.println("Speed information sent successfully for "+(i+1)+" of 500 times");
                Thread.sleep(3000,0);
            }
        };
    }
}
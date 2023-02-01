package com.rail.eventstream.sender;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class EventStreamSenderApplication {
    public static void main(String[] args) {
        SpringApplication.run(EventStreamSenderApplication.class, args);
    }
    @Bean
    public NewTopic topic(){
            return TopicBuilder.name("Rail_C_To_D_Brake_Warning")
                    .partitions(1)
                    .replicas(1)
                    .build();
    }

    @Bean
    public NewTopic topic2(){
        return TopicBuilder.name("Rail_C_To_D")
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public ApplicationRunner sendMessage(KafkaTemplate<String, String> template){
        return args -> {
            for(int i=1;i<=500;i++){
                String speed=String.valueOf(Math.round(Math.random()*1000));
                String brakeData=String.valueOf(Math.round(Math.random()*100));
                template.send("Rail_C_To_D", "Speed", speed);
                template.send("Rail_C_To_D", "BrakeEfficiency", brakeData);
                System.out.println("Sending data= "+speed+" kmph and Brake Efficiency= "+brakeData+" %.");
                Thread.sleep(2000,0);
            }
        };
    }

}
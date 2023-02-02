package com.rail.eventstream.receiver.controller;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/breakdata")
public class BreakDataController {
    @Autowired
    private StreamsBuilderFactoryBean factoryBean;

    @GetMapping("/{input}")
    public Long getData(@PathVariable("input") String inputpercent){
        final KafkaStreams kafkastream=factoryBean.getKafkaStreams();
        final ReadOnlyKeyValueStore<String, Long> datareturn=
                kafkastream.store(StoreQueryParameters.fromNameAndType("breakdata", QueryableStoreTypes.keyValueStore()));
        return datareturn.get(inputpercent);
    }
}

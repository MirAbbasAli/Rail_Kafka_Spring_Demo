package com.rail.eventstream.receiver.utility;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StreamBuilder {
    @Autowired
    public void doProcess(StreamsBuilder streamsBuilder){
        KStream<String, String> trainData= streamsBuilder.stream("Rail_C_To_D",
                Consumed.with(Serdes.String(), Serdes.String()));
        KTable<String, Long> trainDatum=trainData
                .filter((key, value) -> String.valueOf(key).equalsIgnoreCase("BrakeEfficiency"))
                .filter((key, value) -> Long.parseLong(value) < 70)
                .groupBy((key, value)-> value, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("breakdata"));
        trainDatum.toStream().to("Rail_C_To_D_Brake_Warning", Produced.with(Serdes.String(), Serdes.Long()));
    }
    /*
    //http://localhost:8082/breakdata/16
    //http://localhost:8082/breakdata/BrakeEfficency
     */
}

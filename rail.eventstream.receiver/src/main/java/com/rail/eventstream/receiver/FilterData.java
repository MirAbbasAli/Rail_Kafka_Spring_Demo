package com.rail.eventstream.receiver;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Component;

@Component
public class FilterData {
    public void setStreamBuilder(StreamsBuilder streamsBuilder){
        KStream<String, String> trainData= streamsBuilder.stream("Rail_C_To_D",
                Consumed.with(Serdes.String(), Serdes.String()));
        trainData.filter((key, value)-> String.valueOf(key).equalsIgnoreCase("BrakeEfficiency"))
                .filter((key,value)-> Long.parseLong(value)<70)
                .to("Rail_C_To_D_Brake_Warning", Produced.with(Serdes.String(), Serdes.String()));
    }
}

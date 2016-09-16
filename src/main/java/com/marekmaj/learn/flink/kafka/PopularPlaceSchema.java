package com.marekmaj.learn.flink.kafka;

import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class PopularPlaceSchema implements SerializationSchema<PopularPlace> {

    @Override
    public byte[] serialize(PopularPlace element) {
        return element.toString().getBytes();
    }

}

package com.marekmaj.learn.flink.prediction;

import org.apache.flink.streaming.util.serialization.SerializationSchema;

class PredictionTimeTravelSchema implements SerializationSchema<PredictionTimeTravel> {

    @Override
    public byte[] serialize(PredictionTimeTravel element)  {
        return element.toString().getBytes();
    }
}

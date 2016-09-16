package com.marekmaj.learn.flink.kafka;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;


public class TaxiRideTimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<TaxiRide> {

    public TaxiRideTimestampExtractor(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(TaxiRide element) {
        return element.isStart ? element.startTime.getMillis() : element.endTime.getMillis();
    }
}

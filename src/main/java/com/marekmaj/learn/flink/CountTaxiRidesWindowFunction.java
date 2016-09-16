package com.marekmaj.learn.flink;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.stream.StreamSupport;


public class CountTaxiRidesWindowFunction implements WindowFunction<TaxiRide, Tuple5<Float, Float, Long, Boolean, Integer>, Tuple2<Boolean, Integer>, TimeWindow> {
    @Override
    public void apply(Tuple2<Boolean, Integer> key, TimeWindow window, Iterable<TaxiRide> input, Collector<Tuple5<Float, Float, Long, Boolean, Integer>> out) throws Exception {
        float lon = GeoUtils.getGridCellCenterLon(key.f1);
        float lat = GeoUtils.getGridCellCenterLat(key.f1);
        boolean isStart = key.f0;
        Long count = StreamSupport.stream(input.spliterator(), false).count();
        long timestamp = window.getEnd();
        out.collect(new Tuple5<>(lon, lat, timestamp, isStart, count.intValue()));
    }
}

package com.marekmaj.learn.flink;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.stream.StreamSupport;


public class TimeWindowAggregationJob {


    public static void main(String[] args) throws Exception {
        String path = ParameterTool.fromArgs(args).getRequired("data");
        int popularityThreshold = 30;

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // we will be aggregating every 15minutes so
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(path, 15, 60));

        // najpierw chcemy pogrupowac po area id oddzielnie starty i zakonczenia kursów
        KeyedStream<TaxiRide, Tuple2<Boolean, Integer>> ridesGrouped = rides.keyBy(taxiRide -> new Tuple2<>(taxiRide.isStart,
                taxiRide.isStart ?
                        GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat) :
                        GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat))
        );

        // teraz chcemy zagregowac w 15minutowe okna co 5minut i zliczyc kursy
        DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> result = ridesGrouped
                .timeWindow(Time.minutes(15), Time.minutes(5))
                .apply(new CountTaxiRidesWindowFunction());
        // TODO powyzszy sposob jest slaby bo bedziemy trzymac w pamieci wszystkie eventy! z okna a chcemy tak naprawde chcemy sume
        // TODO to niestety nie dziala bo w apply z fold i initial value typ Collectora jest taki jak initial value i to sie robi bez sensu
/*        DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> result = ridesGrouped
                .timeWindow(Time.minutes(15), Time.minutes(5))
                .apply(0, (FoldFunction<TaxiRide, Integer>) (acc, taxiRide) -> acc + 1, windowFunctionForIncrementalAggregation());*/

        result.filter(resultTuple -> resultTuple.f4 > popularityThreshold).print();

        env.execute("Flink Streaming API Window example");
    }

    private static class CountTaxiRidesWindowFunction implements WindowFunction<TaxiRide, Tuple5<Float, Float, Long, Boolean, Integer>, Tuple2<Boolean, Integer>, TimeWindow> {
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
}
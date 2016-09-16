package com.marekmaj.learn.flink.kafka;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TaxiRideSchema;
import com.marekmaj.learn.flink.CountTaxiRidesWindowFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;

public class DataFromKafkaJob {

    public static void main(String[] args) throws Exception {
        int popularityThreshold = 30;

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // read from kafka
        DataStream<TaxiRide> rides = env.addSource(new FlinkKafkaConsumer09<>(KafkaProps.inputTopic, new TaxiRideSchema(), KafkaProps.consumerProperties()))
                .assignTimestampsAndWatermarks(new TaxiRideTimestampExtractor(Time.seconds(DataToKafkaJob.MAX_DELAY_IN_SECONDS)));


        KeyedStream<TaxiRide, Tuple2<Boolean, Integer>> ridesGrouped = rides.keyBy(taxiRide -> new Tuple2<>(taxiRide.isStart,
                taxiRide.isStart ?
                        GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat) :
                        GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat))
        );
        DataStream<Tuple5<Float, Float, Long, Boolean, Integer>> filtered = ridesGrouped
                .timeWindow(Time.minutes(15), Time.minutes(5))
                .apply(new CountTaxiRidesWindowFunction());

        DataStream<PopularPlace> result = filtered
                .map(tuple -> PopularPlace.from(tuple))
                .filter(place -> place.count > popularityThreshold);

        result.addSink(new FlinkKafkaProducer09<>(KafkaProps.broker, KafkaProps.outputTopic, new PopularPlaceSchema()));

        env.execute("Flink from kafka consumer");
    }

}

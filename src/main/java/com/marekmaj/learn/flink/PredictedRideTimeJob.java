package com.marekmaj.learn.flink;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.TravelTimePredictionModel;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joda.time.Duration;

import java.util.concurrent.TimeUnit;

public class PredictedRideTimeJob {


    public static void main(String[] args) throws Exception {
        String path = ParameterTool.fromArgs(args).getRequired("data");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(6, Time.of(10, TimeUnit.SECONDS)));


        DataStream<TaxiRide> rides = env.addSource(new CheckpointedTaxiRideSource(path, 600));

        DataStream predictions = rides
                .map(ride -> new Tuple2<>(GeoUtils.mapToGridCell(ride.endLon, ride.endLat), ride))
                .keyBy(tuple -> tuple.f0)
                .flatMap(new FlatMapWithPrediction())
                .map(tuple -> new Tuple2<>(tuple.f0.rideId, tuple.f1));

        // TODO emit to kafka
        //filtered.addSink(new FlinkKafkaProducer09<TaxiRide>(KafkaProps.broker, "predictionTime", schema))
        predictions.print();

        env.execute("Flink travel time predition");
    }


    private static class FlatMapWithPrediction extends RichFlatMapFunction<Tuple2<Integer, TaxiRide>, Tuple2<TaxiRide, Integer>> {
        private ValueState<TravelTimePredictionModel> predictionModelByKey;

        @Override
        public void flatMap(Tuple2<Integer, TaxiRide> input, Collector<Tuple2<TaxiRide, Integer>> out) throws Exception {
            TaxiRide ride = input.f1;
            TravelTimePredictionModel model = predictionModelByKey.value();
            Integer direction = GeoUtils.getDirectionAngle(ride.startLon, ride.startLat, ride.endLon, ride.endLat);
            Double distance = GeoUtils.getEuclideanDistance(ride.startLon, ride.startLat, ride.endLon, ride.endLat);

            if (ride.isStart) { // predict
                int prediction = model.predictTravelTime(direction, distance);
                out.collect(new Tuple2<>(ride, prediction));
            } else { // learn
                Duration duration = new Duration(ride.startTime, ride.endTime);
                model.refineModel(direction, distance, duration.getStandardSeconds());
                predictionModelByKey.update(model);
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<TravelTimePredictionModel> descriptor =
                    new ValueStateDescriptor<>("predictionModelByKey", TravelTimePredictionModel.class, new TravelTimePredictionModel());
            predictionModelByKey = getRuntimeContext().getState(descriptor);
        }
    }
}

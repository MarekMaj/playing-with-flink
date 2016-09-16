package com.marekmaj.learn.flink.prediction;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;

import java.util.Objects;


public class PredictionTimeTravel {
    public TaxiRide ride;
    public int predictedTimeTravelInMinutes;

    public PredictionTimeTravel() {
    }

    public PredictionTimeTravel(TaxiRide ride, int predictedTimeTravelInMinutes) {
        this.ride = ride;
        this.predictedTimeTravelInMinutes = predictedTimeTravelInMinutes;
    }

    @Override
    public String toString() {
        return "Prediction{" +
                "ride={" + ride + "}" +
                ", predictedTimeTravelInMinutes=" + predictedTimeTravelInMinutes +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PredictionTimeTravel that = (PredictionTimeTravel) o;
        return predictedTimeTravelInMinutes == that.predictedTimeTravelInMinutes &&
                Objects.equals(ride, that.ride);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ride, predictedTimeTravelInMinutes);
    }
}
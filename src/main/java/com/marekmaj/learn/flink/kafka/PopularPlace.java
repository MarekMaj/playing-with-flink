package com.marekmaj.learn.flink.kafka;

import org.apache.flink.api.java.tuple.Tuple5;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;


public class PopularPlace {

    private static final ZoneId ZONE_ID = ZoneId.systemDefault();

    public Float longitude;
    public Float latitude;
    public Boolean isStart;
    public LocalDateTime processTime;
    public Integer count;

    public PopularPlace() {
    }

    public PopularPlace(Float longitude, Float latitude, Boolean isStart, LocalDateTime processTime, Integer count) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.isStart = isStart;
        this.processTime = processTime;
        this.count = count;
    }

    public static PopularPlace from(Tuple5<Float, Float, Long, Boolean, Integer> tuple) {
        Instant instant = Instant.ofEpochMilli(tuple.f2);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZONE_ID);
        return new PopularPlace(tuple.f0, tuple.f1, tuple.f3, dateTime, tuple.f4);
    }

    // TODO pewnie to mogloby byc lepsze...
    @Override
    public String toString() {
        return "PopularPlace{" +
                "longitude=" + longitude +
                ", latitude=" + latitude +
                ", isStart=" + isStart +
                ", processTime=" + ZonedDateTime.of(processTime, ZONE_ID).toInstant().getEpochSecond() +
                ", count=" + count +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PopularPlace that = (PopularPlace) o;
        return Objects.equals(longitude, that.longitude) &&
                Objects.equals(latitude, that.latitude) &&
                Objects.equals(isStart, that.isStart) &&
                Objects.equals(processTime, that.processTime) &&
                Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(longitude, latitude, isStart, processTime, count);
    }
}

package com.marekmaj.learn.flink.kafka;


import java.util.Properties;

public final class KafkaProps {

    public static final String broker = "kafka:9092";
    public static final String zookeeperHost = "zookeeper:2181";

    private KafkaProps() {
    }

    public static Properties consumerProperties() {
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", zookeeperHost);
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("group.id", "flink");
        props.setProperty("auto.offset.reset", "earliest");       // Always read topic from start
        return props;
    }
}

# Playing with flink 1.1.2

Flink training by dataartisans 

#### Download data 

```wget http://dataartisans.github.io/flink-training/trainingData/nycTaxiRides.gz```

```wget http://dataartisans.github.io/flink-training/trainingData/flinkMails.gz```

#### Run whole environment locally

```docker-compose up```

#### Code, build and deploy job

```./deploy.sh <job class name>```

#### Show logs

```docker exec -it taskmanager tail -f /usr/local/flink/log/flink--taskmanager-*.out```

- kafka.DataToKafkaJob: 

```docker exec -it kafka bash /opt/kafka_2.11-0.10.0.1/bin/kafka-console-consumer.sh --zookeeper zookeeper:2181 --topic taxiRides```

- kafka.DataFromKafkaJob:

```docker exec -it kafka bash /opt/kafka_2.11-0.10.0.1/bin/kafka-console-consumer.sh --zookeeper zookeeper:2181 --topic popularPlaces```

- prediction.PredictedRideTimeJob: 

```docker exec -it kafka bash /opt/kafka_2.11-0.10.0.1/bin/kafka-console-consumer.sh --zookeeper zookeeper:2181 --topic predictionTime```
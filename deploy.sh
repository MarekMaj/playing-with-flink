#!/usr/bin/env bash
path=/usr/local/flink

if [[ $# -eq 0 ]] ; then
    echo 'first argument should be main class simple name'
    exit 1
fi

mvn package -DskipTests

#copy file data to container
docker cp nycTaxiRides.gz taskmanager:$path

#copy jar
docker cp target/learn-flink-*.jar jobmanager:$path
docker exec -it jobmanager $path/bin/flink run -c com.marekmaj.learn.flink.$1 $path/learn-flink-1.0-SNAPSHOT.jar -data $path/nycTaxiRides.gz
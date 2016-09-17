#!/usr/bin/env bash
path=/usr/local/flink

if [[ $# -eq 0 ]] ; then
    echo 'first argument should be main class simple name'
    echo 'you can provide data file as second arg'
    exit 1
fi
data=${2:-nycTaxiRides.gz}

mvn package -DskipTests

#copy file data to container
docker cp $data taskmanager:$path
#batch job will be executed on jobmanager, stream job actually does not need this...
docker cp $data jobmanager:$path

#copy jar
docker cp target/learn-flink-*.jar jobmanager:$path
docker exec -it jobmanager $path/bin/flink run -c com.marekmaj.learn.flink.$1 $path/learn-flink-1.0-SNAPSHOT.jar -data $path/$data
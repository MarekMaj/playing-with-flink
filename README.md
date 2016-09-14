# Playing with flink 1.1.2

Flink training by dataartisans 

1. Download data 
`wget http://dataartisans.github.io/flink-training/trainingData/nycTaxiRides.gz`

2. Run flink locally
`docker-compose up`

3. Code, build and deploy job
`./deploy.sh StreamingJob`

4. Show logs
`docker exec -it taskmanager tail -f /usr/local/flink/log/flink--taskmanager-*.out`
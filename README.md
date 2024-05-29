# Flight-Data-Analysis
Creating a Real-Time Flight-info Data Pipeline with Kafka, Apache Spark, Elasticsearch and Kibana

## Pipeline
Our project pipeline is as follows:
![image](https://github.com/dp2003/Flight-Data-Analysis/assets/72189557/0f49f7f3-5668-4aba-b228-50f3a603477a)

## Prerequisites
The following software should be installed on your machine in order to reproduice our work:

- Spark (spark-3.3.1-bin-hadoop2.7)
- Kafka (kafka_2.13-2.7.0)
- ElasticSearch (elasticsearch-7.14.2)
- Kibana (kibana-7.14.2)
- Python 3.9.6

## Steps
###### Get Flight API:
We started by collecting in real-time Flight informations and then we sent them to Kafka for analytics.
###### Kafka Real-Time Producer:
The data is ingested from the flight streaming data API and sent to a kafka topic. You need to run Kafka Server with Zookeeper and create a dedicated topic for data transport.
###### PySpark Streaming:
 In Spark Streaming, Kafka consumer is created that periodically collect data in real time from the kafka topic and send them into an Elasticsearch index.
###### Index flight-info to Elasticsearch:
You need to enable and start Elasticsearch and run it to store the flight-info and their realtime information for further visualization purpose. You can navigate to http://localhost:9200 to check if it's up and running.
###### Kibana for visualization
Kibana is a visualization tool that can explore the data stored in elasticsearch. In our project, instead of directly output the result, we used this visualization tool to visualize the streaming data in a real-time manner.You can navigate to http://localhost:5601 to check if it's up and running.

## How to run

1. Start Elasticsearch

`sudo systemctl start elasticsearch ` & `sudo systemctl enable elasticsearch `

2. Start Kibana

`sudo systemctl start kibana ` & `sudo systemctl enable kibana  `

3. Start Zookeeper server by moving into the bin folder of Zookeeper installed directory by using:

`./bin/zookeeper-server-start.sh ./config/zookeeper.properties`

4. Start Kafka server by moving into the bin folder of Kafka installed directory by using:

`./bin/kafka-server-start.sh ./config/server.properties`

5. Run Kafka producer:

`python3 ./producer.py`

6. Run PySpark consumer with spark-submit:

`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.elasticsearch:elasticsearch-spark-30_2.12:7.14.2 /consumer.py`

## How to launch kibana dashboard

- Open http://localhost:5601/ in your browser.
- Go to Index Patterns.
- Make new index patterns.
- Go to Discover.
- Make Dashboard.

## Final result
![image](https://github.com/dp2003/Flight-Data-Analysis/assets/72189557/7f4eb1f0-fe94-49a5-a8b9-396d8a72a3d9)
![image](https://github.com/dp2003/Flight-Data-Analysis/assets/72189557/90e4b6c0-012d-4ad0-935b-7a2e695925ba)




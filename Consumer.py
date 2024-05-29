import os

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.elasticsearch:elasticsearch-spark-30_2.12:7.14.2 pyspark-shell"

import json

import math

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json, col
from pyspark import SparkContext

from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from subprocess import check_output

es = Elasticsearch(hosts=['localhost'], port=9200)

topic = "flight-realtime"
ETH0_IP = check_output(["hostname"]).decode(encoding="utf-8").strip()

SPARK_MASTER_URL = "local[*]"
SPARK_DRIVER_HOST = ETH0_IP

spark_conf = SparkConf()
spark_conf.setAll(
    [
        ("spark.master", SPARK_MASTER_URL),
        ("spark.driver.bindAddress", "0.0.0.0"),
        ("spark.driver.host", SPARK_DRIVER_HOST),
        ("spark.app.name", "Flight-infos"),
        ("spark.submit.deployMode", "client"),
        ("spark.ui.showConsoleProgress", "true"),
        ("spark.eventLog.enabled", "false"),
        ("spark.logConf", "false"),
    ]
)

mappings = {
 "mappings": {            
    "properties": {
        "location": {
                        "type": "geo_point"
                    }
    }
}
}

def getrows(df, rownums=None):
    return df.rdd.zipWithIndex().map(lambda x: x[0])

if __name__ == "__main__":
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "flight-realtime")
        .option("enable.auto.commit", "true")
        .load()
    )
    
    def func_call(df , batch_id):
    	df.selectExpr("CAST(value AS STRING) as json")
    	flight = getrows(df,rownums=[0]).collect()
    	for i in flight:
    		hex = i[1].decode('utf-8')

    		dictio = eval(hex)
    		es.indices.create(index='flight-realtime-project', body=mappings , ignore=400)
    		es.index(
                    index="flight-realtime-project",
                    doc_type="_doc",
                    body={
		            "flag": dictio.get("flag" , None),
		            "lat": dictio["lat"],
		            "lng": dictio["lng"],
		            "alt": dictio["alt"],
		            "dir": dictio["dir"],
		            "speed": dictio.get("speed" , None),
		            "flight_number": dictio.get("flight_number" , None),
		            "flight_icao": dictio.get('flight_icao' , None),
		            "dep_icao": dictio.get('dep_icao' , None),
		            "arr_icao": dictio.get('arr_icao' , None),
		            "airline_icao": dictio.get('airline_icao' , None),
		            "aircraft_icao": dictio.get('aircraft_icao' , None),
		            "status": dictio.get("status" , None),
		            "location": {
        "lat": dictio["lat"],
        "lon": dictio["lng"]
    }
                    
                    }
                )	
           
    query = df.writeStream \
    .format('console') \
    .foreachBatch(func_call) \
    .trigger(processingTime="15 seconds") \
    .start().awaitTermination()


  

from pyspark.sql import SparkSession


# Create a spark session using SparkSession builder
spark = SparkSession  \
	.builder  \
	.appName("Load Kafka ClickStream")  \
	.getOrCreate()

# Setting log level to ERROR
spark.sparkContext.setLogLevel('ERROR')	


# Subscribe to the topic
# Bootstrap-server - 18.211.252.152
# Port - 9092
# Topic - de-capstone3

lines = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","18.211.252.152:9092")  \
	.option("subscribe","de-capstone3")  \
    .option("startingOffsets","earliest") \
	.load()

# Reveal the schema of the dataFrame
lines.printSchema()

kafkaDF = lines.selectExpr("cast(key as string)","cast(value as string)")	


query = kafkaDF. \
	writeStream \
  	.outputMode("append") \
  	.format("json") \
  	.option("truncate","false") \
  	.option("path", "/user/ec2-user/path") \
  	.option("checkpointLocation","/user/ec2-user/checkpoint") \
  	.start()

query.awaitTermination()


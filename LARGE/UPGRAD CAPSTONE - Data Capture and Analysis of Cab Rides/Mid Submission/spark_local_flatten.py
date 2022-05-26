from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a spark session using SparkSession builder
spark = SparkSession.builder \
    .master("local") \
    .appName("Kafka To HDFS") \
    .getOrCreate()

# spark read in json format
# file location:  /user/ec2-user/path/part-00000-d3f68872-85e6-48a4-b4c2-665547114e86-c000.json
jsonDF = spark \
	.read \
	.json('/user/ec2-user/path/part-00000-d3f68872-85e6-48a4-b4c2-665547114e86-c000.json')

df = jsonDF.select( \
	get_json_object(jsonDF["value"], "$.customer_id").alias("customer_id"), \
	get_json_object(jsonDF["value"], "$.app_version").alias("app_version"), \
	get_json_object(jsonDF["value"], "$.OS_version").alias("OS_version"), \
	get_json_object(jsonDF["value"], "$.lat").alias("lat"), \
	get_json_object(jsonDF["value"], "$.lon").alias("lon"), \
	get_json_object(jsonDF["value"], "$.page_id").alias("page_id"), \
	get_json_object(jsonDF["value"], "$.button_id").alias("button_id"), \
	get_json_object(jsonDF["value"], "$.is_button_click").alias("is_button_click"), \
	get_json_object(jsonDF["value"], "$.is_page_view").alias("is_page_view"), \
	get_json_object(jsonDF["value"], "$.is_scroll_up").alias("is_scroll_up"), \
	get_json_object(jsonDF["value"], "$.is_scroll_down").alias("is_scroll_down"), \
	get_json_object(jsonDF["value"], "$.timestamp\n").alias("timestamp"), \
)

# Log some data
print(df.schema)
df.show(5)

# storing the CSV file
# location: 'user/ec2-user/ClickStreamData'
df \
	.coalesce(1) \
	.write \
	.format('com.databricks.spark.csv') \
	.mode('overwrite') \
	.save('user/ec2-user/ClickStreamData', header = 'true')
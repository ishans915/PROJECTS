from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
	.builder \
	.appName("Aggregate Date Wise Bookings Data") \
	.getOrCreate()

bookingsDF = spark \
	.read \
	.csv("/user/root/bookings_data/part-m-00000", inferSchema=True)

bookingsDFWithHeader = bookingsDF.withColumnRenamed("_c0","booking_id") \
	   .withColumnRenamed("_c1","customer_id") \
	   .withColumnRenamed("_c2","driver_id") \
	   .withColumnRenamed("_c3","customer_app_version")  \
	   .withColumnRenamed("_c4","customer_phone_os_version") \
	   .withColumnRenamed("_c5","pickup_lat") \
	   .withColumnRenamed("_c6","pickup_lon") \
	   .withColumnRenamed("_c7","drop_lat") \
	   .withColumnRenamed("_c8","drop_lon") \
	   .withColumnRenamed("_c9","pickup_timestamp")  \
	   .withColumnRenamed("_c10","drop_timestamp")  \
	   .withColumnRenamed("_c11","trip_fare") \
	   .withColumnRenamed("_c12","tip_amount")  \
	   .withColumnRenamed("_c13","currency_code") \
	   .withColumnRenamed("_c14","cab_color")  \
	   .withColumnRenamed("_c15","cab_registration_no") \
	   .withColumnRenamed("_c16","customer_rating_by_driver")  \
	   .withColumnRenamed("_c17","rating_by_customer")  \
	   .withColumnRenamed("_c18","passenger_count")

bookingsDFWithHeader =  bookingsDFWithHeader.withColumn("date", date_format('pickup_timestamp', "yyyy-MM-dd"))
bookingsDFWithHeader.show(10)

# Creating Date wise aggregated data flow
dateAggregatedDF = bookingsDFWithHeader \
	.select('date') \
	.groupBy('date') \
	.count()

# Log the new data flow and find count. Should be 289
dateAggregatedDF.show()
dateAggregatedDF.count()


# write the dataframe to HDFS
bookingsDFWithHeader \
	.coalesce(1) \
	.write \
	.format('com.databricks.spark.csv') \
	.mode('overwrite') \
	.save('user/ec2-user/bookings_data_with_header', header = 'true')



# write the aggregated dataframe to HDFS
dateAggregatedDF \
	.coalesce(1) \
	.write \
	.format('com.databricks.spark.csv') \
	.mode('overwrite') \
	.save('user/ec2-user/date_aggregated_bookings', header='true')
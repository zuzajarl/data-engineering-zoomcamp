# Question 1
Code: 
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

print(f"Spark version: {spark.version}")

df = spark.range(10)
df.show()

spark.stop()

Output:
[](images/image1.png)

Answer: 4.1.1

# Question 2

df = spark.read.parquet('yellow_tripdata_2025-11.parquet')
df = df.repartition(4)
df.write.parquet('tripdata_partitioned.parquet')

Result:
[](images/image2.png)

Answer: ~24.4MB

# Question 3

df.createOrReplaceTempView("trips")

df_15th = spark.sql("""
SELECT COUNT(*) AS trip_count
FROM trips
WHERE tpep_pickup_datetime >= '2025-11-15 00:00:00'
  AND tpep_pickup_datetime <  '2025-11-16 00:00:00'
""")

df_15th.show()

Answer: 
162604

# Question 4
from pyspark.sql import functions as F

df_longest = df.select(
    (
        (F.unix_timestamp("tpep_dropoff_datetime") - 
         F.unix_timestamp("tpep_pickup_datetime")) / 3600
    ).alias("trip_hours")
)
df_longest.agg(F.max("trip_hours")).show()

Answer: 90.64666666666666

# Question 5
[](image3.png)

(In my case it is 4042 because the 4040 was taken)

Answer: 4040

# Question 6
zone_df = spark.read.option("header", "true").csv("taxi_zone_lookup.csv")

zone_df.createOrReplaceTempView("zones")
df.createOrReplaceTempView("trips")

spark.sql("""
SELECT 
    z.Zone,
    COUNT(*) AS pickups
FROM trips t
JOIN zones z
ON t.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY pickups ASC
""").show()

Answer: Governor's Island/Ellis Island/Liberty Island and Arden Heights
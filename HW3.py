from pyspark.sql.functions import lit, col

# 1. Імпортувати дані жовтих та зелених поїздок:
# * Зчитати ys3://robot-dreams-source-data/home-work-1/nyc_taxi/ у Spark.

green_df = spark.read.parquet("s3://robot-dreams-source-data/Lecture_3/nyc_taxi/green/2024/*.parquet") \
    .withColumn("taxi_type", lit("green"))

yellow_df = spark.read.parquet("s3://robot-dreams-source-data/Lecture_3/nyc_taxi/yellow/2024/*.parquet") \
    .withColumn("taxi_type", lit("yellow"))

yellow_df.printSchema()
green_df.printSchema()
"""
root
 |-- VendorID: integer (nullable = true)
 |-- pickup_datetime: timestamp_ntz (nullable = true)
 |-- dropoff_datetime: timestamp_ntz (nullable = true)
 |-- passenger_count: long (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: long (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- payment_type: long (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- congestion_surcharge: double (nullable = true)
 |-- Airport_fee: double (nullable = true)
 |-- taxi_type: string (nullable = false)

root
 |-- VendorID: integer (nullable = true)
 |-- pickup_datetime: timestamp_ntz (nullable = true)
 |-- dropoff_datetime: timestamp_ntz (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- RatecodeID: long (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- passenger_count: long (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- ehail_fee: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- payment_type: long (nullable = true)
 |-- trip_type: long (nullable = true)
 |-- congestion_surcharge: double (nullable = true)
 |-- taxi_type: string (nullable = false)
"""

# 2. Уніфікувати схеми:
# * Переконатись, що всі колонки мають однакові назви та типи даних.
# * Додати колонку taxi_type з відповідними значеннями: yellow або green.

all_columns = list(set(green_df.columns).union(set(yellow_df.columns)))

green_aligned = green_df.select([
    col(c) if c in green_df.columns else lit(None).alias(c)
    for c in all_columns
])

yellow_aligned = yellow_df.select([
    col(c) if c in yellow_df.columns else lit(None).alias(c)
    for c in all_columns
])

# 3. Об’єднати дані в один датафрейм raw_trips_df.

raw_trips_df = green_aligned.unionByName(yellow_aligned)

raw_trips_df.printSchema()
raw_trips_df.show(5)
"""
 |-- ehail_fee: double (nullable = true)
 |-- pickup_datetime: timestamp_ntz (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- taxi_type: string (nullable = false)
 |-- passenger_count: long (nullable = true)
 |-- extra: double (nullable = true)
 |-- trip_type: long (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- VendorID: integer (nullable = true)
 |-- dropoff_datetime: timestamp_ntz (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- payment_type: long (nullable = true)
 |-- Airport_fee: double (nullable = true)
 |-- congestion_surcharge: double (nullable = true)
 |-- RatecodeID: long (nullable = true)
 |-- tolls_amount: double (nullable = true)

+---------+-------------------+------------------+---------+---------------+-----+---------+----------+-----------+------------+------------+-------+------------+-------------+--------+-------------------+---------------------+------------+-----------+--------------------+----------+------------+
|ehail_fee|    pickup_datetime|store_and_fwd_flag|taxi_type|passenger_count|extra|trip_type|tip_amount|fare_amount|total_amount|DOLocationID|mta_tax|PULocationID|trip_distance|VendorID|   dropoff_datetime|improvement_surcharge|payment_type|Airport_fee|congestion_surcharge|RatecodeID|tolls_amount|
"""

# 4. Виконати фільтрацію аномальних записів:
# * Вилучити поїздки з відстанню < 0.1 км, тарифом < $2, тривалістю < 1 хв.
from pyspark.sql.functions import unix_timestamp, col

filtered_df = raw_trips_df \
    .withColumn("duration_sec", unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) \
    .filter(
    (col("trip_distance") >= 0.1) &
    (col("fare_amount") >= 2) &
    (col("duration_sec") >= 60)
)
filtered_df.show(5)
"""
|ehail_fee|    pickup_datetime|store_and_fwd_flag|taxi_type|passenger_count|extra|trip_type|tip_amount|fare_amount|total_amount|DOLocationID|mta_tax|PULocationID|trip_distance|VendorID|   dropoff_datetime|improvement_surcharge|payment_type|Airport_fee|congestion_surcharge|RatecodeID|tolls_amount|duration_sec|
+---------+-------------------+------------------+---------+---------------+-----+---------+----------+-----------+------------+------------+-------+------------+-------------+--------+-------------------+---------------------+------------+-----------+--------------------+----------+------------+------------+
|     NULL|2024-04-01 00:56:16|                 N|    green|              1|  1.0|        1|      4.04|       17.7|       24.24|         225|    0.5|          65|         3.06|       2|2024-04-01 01:12:56|                  1.0|           1|       NULL|                 0.0|         1|         0.0|        1000|
|     NULL|2024-04-01 00:23:09|                 N|    green|              1|  1.0|        1|      3.48|       11.4|       17.38|         146|    0.5|         226|         1.95|       2|2024-04-01 00:33:03|                  1.0|           1|       NULL|                 0.0|         1|         0.0|         594|
|     NULL|2024-03-31 22:34:23|                 N|    green|              1|  1.0|        1|      3.06|       12.8|       18.36|         116|    0.5|          74|         1.93|       2|2024-03-31 22:45:33|                  1.0|           1|       NULL|                 0.0|         1|         0.0|         670|
|     NULL|2024-03-31 23:21:41|                 N|    green|              1|  1.0|        1|       0.8|       10.7|       16.75|         238|    0.5|         236|          1.5|       2|2024-03-31 23:29:40|                  1.0|           1|       NULL|                2.75|         1|         0.0|         479|
|     NULL|2024-04-01 00:41:53|                 N|    green|              1|  1.0|        1|       0.0|       12.1|        14.6|          92|    0.5|          92|         2.02|       2|2024-04-01 00:50:31|                  1.0|           2|       NULL|                 0.0|         1|         0.0|         518|
+---------+-------------------+------------------+---------+---------------+-----+---------+----------+-----------+------------+------------+-------+------------+-------------+--------+-------------------+---------------------+------------+-----------+--------------------+----------+------------+------------+
only showing top 5 rows
​
"""

# 5. Додати наступні колонки:
# * pickup_hour — годину початку поїздки.
# * pickup_day_of_week — день тижня.
# * duration_min — тривалість поїздки в хвилинах.


# 6. Виконати JOIN з taxi_zone_lookup.csv, додавши поля pickup_zone, dropoff_zone.
from pyspark.sql.functions import hour, date_format

enhanced_df = filtered_df \
    .withColumn("pickup_hour", hour("pickup_datetime")) \
    .withColumn("pickup_day_of_week", date_format("pickup_datetime", "E")) \
    .withColumn("duration_min", col("duration_sec") / 60)

enhanced_df.show(5)
"""
|ehail_fee|    pickup_datetime|store_and_fwd_flag|taxi_type|passenger_count|extra|trip_type|tip_amount|fare_amount|total_amount|DOLocationID|mta_tax|PULocationID|trip_distance|VendorID|   dropoff_datetime|improvement_surcharge|payment_type|Airport_fee|congestion_surcharge|RatecodeID|tolls_amount|duration_sec|pickup_hour|pickup_day_of_week|      duration_min|
+---------+-------------------+------------------+---------+---------------+-----+---------+----------+-----------+------------+------------+-------+------------+-------------+--------+-------------------+---------------------+------------+-----------+--------------------+----------+------------+------------+-----------+------------------+------------------+
|     NULL|2024-03-01 00:10:52|                 N|    green|              1|  1.0|        1|      3.06|       12.8|       18.36|         226|    0.5|         129|         1.72|       2|2024-03-01 00:26:12|                  1.0|           1|       NULL|                 0.0|         1|         0.0|         920|          0|               Fri|15.333333333333334|
|     NULL|2024-03-01 00:22:21|                 N|    green|              1|  1.0|        1|       0.0|       17.7|        20.2|         218|    0.5|         130|         3.25|       2|2024-03-01 00:35:15|                  1.0|           2|       NULL|                 0.0|         1|         0.0|         774|          0|               Fri|              12.9|
|     NULL|2024-03-01 00:45:27|                 N|    green|              2|  1.0|        1|       3.5|       23.3|       32.05|         107|    0.5|         255|         4.58|       2|2024-03-01 01:04:32|                  1.0|           1|       NULL|                2.75|         1|         0.0|        1145|          0|               Fri|19.083333333333332|
|     NULL|2024-03-01 00:16:45|                 N|    green|              1|  1.0|        1|       1.0|        8.6|        12.1|         135|    0.5|          95|         1.15|       2|2024-03-01 00:23:25|                  1.0|           1|       NULL|                 0.0|         1|         0.0|         400|          0|               Fri| 6.666666666666667|
|     NULL|2024-03-01 00:41:20|                 N|    green|              2|  1.0|        1|      9.42|       28.9|       40.82|           7|    0.5|          80|         6.78|       2|2024-03-01 00:57:00|                  1.0|           1|       NULL|                 0.0|         1|         0.0|         940|          0|               Fri|15.666666666666666|
+---------+-------------------+------------------+---------+---------------+-----+---------+----------+-----------+------------+------------+-------+------------+-------------+--------+-------------------+---------------------+------------+-----------+--------------------+----------+------------+------------+-----------+------------------+------------------+
only showing top 5 rows

​"""

# 6. Виконати JOIN з taxi_zone_lookup.csv, додавши поля pickup_zone, dropoff_zone.
zone_df = spark.read.option("header", True).csv("s3://robot-dreams-source-data/Lecture_3/nyc_taxi/taxi_zone_lookup.csv")

zone_df.show(5)

"""
Spark Job Progress
+----------+-------------+--------------------+------------+
|LocationID|      Borough|                Zone|service_zone|
+----------+-------------+--------------------+------------+
|         1|          EWR|      Newark Airport|         EWR|
|         2|       Queens|         Jamaica Bay|   Boro Zone|
|         3|        Bronx|Allerton/Pelham G...|   Boro Zone|
|         4|    Manhattan|       Alphabet City| Yellow Zone|
|         5|Staten Island|       Arden Heights|   Boro Zone|
+----------+-------------+--------------------+------------+
"""

zone_df_pickup = zone_df.withColumnRenamed("LocationID", "PULocationID").withColumnRenamed("Zone", "pickup_zone")
zone_df_dropoff = zone_df.withColumnRenamed("LocationID", "DOLocationID").withColumnRenamed("Zone", "dropoff_zone")

joined_df = enhanced_df \
    .join(zone_df_pickup.select("PULocationID", "pickup_zone"), on="PULocationID", how="left") \
    .join(zone_df_dropoff.select("DOLocationID", "dropoff_zone"), on="DOLocationID", how="left")

joined_df.show(5)

"""+------------+------------+---------+-------------------+------------------+---------+---------------+-----+---------+----------+-----------+------------+-------+-------------+--------+-------------------+---------------------+------------+-----------+--------------------+----------+------------+------------+-----------+------------------+------------------+--------------------+--------------------+
|DOLocationID|PULocationID|ehail_fee|    pickup_datetime|store_and_fwd_flag|taxi_type|passenger_count|extra|trip_type|tip_amount|fare_amount|total_amount|mta_tax|trip_distance|VendorID|   dropoff_datetime|improvement_surcharge|payment_type|Airport_fee|congestion_surcharge|RatecodeID|tolls_amount|duration_sec|pickup_hour|pickup_day_of_week|      duration_min|         pickup_zone|        dropoff_zone|
+------------+------------+---------+-------------------+------------------+---------+---------------+-----+---------+----------+-----------+------------+-------+-------------+--------+-------------------+---------------------+------------+-----------+--------------------+----------+------------+------------+-----------+------------------+------------------+--------------------+--------------------+
|          49|          65|     NULL|2024-05-01 00:07:08|                 N|    green|              1|  1.0|        1|       2.0|        9.3|        13.8|    0.5|         1.24|       2|2024-05-01 00:15:03|                  1.0|           1|       NULL|                 0.0|         1|         0.0|         475|          0|               Wed| 7.916666666666667|Downtown Brooklyn...|        Clinton Hill|
|         179|           7|     NULL|2024-05-01 00:30:48|                 N|    green|              1|  1.0|        1|      1.94|        7.2|       11.64|    0.5|         0.94|       2|2024-05-01 00:35:49|                  1.0|           1|       NULL|                 0.0|         1|         0.0|         301|          0|               Wed| 5.016666666666667|             Astoria|         Old Astoria|
|          42|          74|     NULL|2024-05-01 00:34:13|                 N|    green|              1|  1.0|        1|       0.0|        6.5|         9.0|    0.5|         0.84|       2|2024-05-01 00:38:07|                  1.0|           2|       NULL|                 0.0|         1|         0.0|         234|          0|               Wed|               3.9|   East Harlem North|Central Harlem North|
|         235|          75|     NULL|2024-05-01 00:58:01|                 N|    green|              1|  1.0|        1|       5.0|       25.4|        32.9|    0.5|         6.07|       2|2024-05-01 01:14:41|                  1.0|           1|       NULL|                 0.0|         1|         0.0|        1000|          0|               Wed|16.666666666666668|   East Harlem South|University Height...|
|          49|         256|     NULL|2024-05-01 00:11:45|                 N|    green|              2|  1.0|        1|      2.92|       12.1|       17.52|    0.5|         2.06|       2|2024-05-01 00:20:38|                  1.0|           1|       NULL|                 0.0|         1|         0.0|         533|          0|               Wed| 8.883333333333333|Williamsburg (Sou...|        Clinton Hill|
+------------+------------+---------+-------------------+------------------+---------+---------------+-----+---------+----------+-----------+------------+-------+-------------+--------+-------------------+---------------------+------------+-----------+--------------------+----------+------------+------------+-----------+------------------+------------------+--------------------+--------------------+
only showing top 5 rows
"""​


"""
Завдання 3: Створення та агрегація фінального датафрейму zone_summary

 1. Створити датафрейм zone_summary, що містить наступні колонки:

  * pickup_zone — зона посадки
  * total_trips — загальна кількість поїздок
  * avg_trip_distance — середня відстань поїздки
  * avg_total_amount — середній тариф поїздки
  * avg_tip_amount — середня сума чайових
  * yellow_share — частка жовтих поїздок у зоні
  * green_share — частка зелених поїздок у зоні
  * max_trip_distance — максимальна відстань поїздки
  * min_tip_amount — мінімальна сума чайових

 2. Зберегти агрегаційні результати:

  * Записати весь результат в один файл: Файл zone_summary.parquet для подальшого використання.
  * Розмістити цей файл у S3 в папку /yourbucket/results/zone_statistic/{date}/zone_statistic.parquet.
"""

from pyspark.sql.functions import count, avg, max, min, sum, when

base_agg = joined_df.groupBy("pickup_zone").agg(
    count("*").alias("total_trips"),
    avg("trip_distance").alias("avg_trip_distance"),
    avg("total_amount").alias("avg_total_amount"),
    avg("tip_amount").alias("avg_tip_amount"),
    max("trip_distance").alias("max_trip_distance"),
    min("tip_amount").alias("min_tip_amount")
)

base_agg.show(5)

"""+-----------------+-----------+------------------+------------------+-------------------+-----------------+--------------+
|      pickup_zone|total_trips| avg_trip_distance|  avg_total_amount|     avg_tip_amount|max_trip_distance|min_tip_amount|
+-----------------+-----------+------------------+------------------+-------------------+-----------------+--------------+
|   Pelham Parkway|       1677| 7.896171735241503| 35.48875372689324|0.21365533691115085|            39.24|           0.0|
|        Rego Park|       6604|10.882057843731067| 34.51277407631736| 1.9518140520896439|         25742.79|           0.0|
|Kew Gardens Hills|       2216|   7.5670261732852|36.031507220216604| 0.7670126353790614|            132.7|           0.0|
|  Jackson Heights|      24058| 20.60567794496635| 30.21681311829748| 1.7759227699725662|        151387.19|           0.0|
|   Yorkville West|     740548| 4.254516331149367|21.371129798473678|    2.5279329091429|        222478.29|           0.0|
+-----------------+-----------+------------------+------------------+-------------------+-----------------+--------------+
only showing top 5 rows
"""

# * yellow_share — частка жовтих поїздок у зоні
# * green_share — частка зелених поїздок у зоні

share_df = joined_df.groupBy("pickup_zone").agg(
    (sum(when(col("taxi_type") == "yellow", 1).otherwise(0)) / count("*")).alias("yellow_share"),
    (sum(when(col("taxi_type") == "green", 1).otherwise(0)) / count("*")).alias("green_share")
)

share_df.show(5)

"""
+-----------------+------------------+--------------------+
|      pickup_zone|      yellow_share|         green_share|
+-----------------+------------------+--------------------+
|   Pelham Parkway|0.9725700655933214|0.027429934406678593|
|        Rego Park|  0.61114476075106| 0.38885523924894005|
|Kew Gardens Hills|0.9305054151624549| 0.06949458483754513|
|  Jackson Heights|0.5818023110815529|  0.4181976889184471|
|   Yorkville West|0.9991614318045555|8.385681954444546E-4|
+-----------------+------------------+--------------------+
"""

zone_summary = base_agg.join(share_df, on="pickup_zone", how="inner")

zone_summary.show(5)

"""
+-----------------+-----------+-----------------+------------------+-------------------+-----------------+--------------+------------------+--------------------+
|      pickup_zone|total_trips|avg_trip_distance|  avg_total_amount|     avg_tip_amount|max_trip_distance|min_tip_amount|      yellow_share|         green_share|
+-----------------+-----------+-----------------+------------------+-------------------+-----------------+--------------+------------------+--------------------+
|   Pelham Parkway|       1677|7.896171735241506|35.488753726893265|0.21365533691115088|            39.24|           0.0|0.9725700655933214|0.027429934406678593|
|        Rego Park|       6604|10.88205784373107| 34.51277407631738|  1.951814052089643|         25742.79|           0.0|  0.61114476075106| 0.38885523924894005|
|Kew Gardens Hills|       2216|7.567026173285198| 36.03150722021661| 0.7670126353790612|            132.7|           0.0|0.9305054151624549| 0.06949458483754513|
|  Jackson Heights|      24058|20.60567794496634|30.216813118297456| 1.7759227699725666|        151387.19|           0.0|0.5818023110815529|  0.4181976889184471|
|   Yorkville West|     740548| 4.25451633114936|21.371129798473742| 2.5279329091430913|        222478.29|           0.0|0.9991614318045555|8.385681954444546E-4|
+-----------------+-----------+-----------------+------------------+-------------------+-----------------+--------------+------------------+--------------------+
only showing top 5 rows
"""

""" 2. Зберегти агрегаційні результати:

  * Записати весь результат в один файл: Файл zone_summary.parquet для подальшого використання.
  * Розмістити цей файл у S3 в папку /yourbucket/results/zone_statistic/{date}/zone_statistic.parquet.
"""
# з датою не спрацювало
output_path = f"s3://robot-dreams-source-data/results/zone_statistic/mmalyhina_zone_statistic.parquet"

zone_summary.coalesce(1).write.mode("overwrite").parquet(output_path)

df_mmalyhina = spark.read.parquet(
    "s3://robot-dreams-source-data/results/zone_statistic/mmalyhina_zone_statistic.parquet")

df_mmalyhina.show(5)

"""
+-----------------+-----------+------------------+------------------+------------------+-----------------+--------------+------------------+--------------------+
|      pickup_zone|total_trips| avg_trip_distance|  avg_total_amount|    avg_tip_amount|max_trip_distance|min_tip_amount|      yellow_share|         green_share|
+-----------------+-----------+------------------+------------------+------------------+-----------------+--------------+------------------+--------------------+
|   Pelham Parkway|       1677| 7.896171735241506|35.488753726893265|0.2136553369111509|            39.24|           0.0|0.9725700655933214|0.027429934406678593|
|        Rego Park|       6604| 10.88205784373107| 34.51277407631739|1.9518140520896428|         25742.79|           0.0|  0.61114476075106| 0.38885523924894005|
|Kew Gardens Hills|       2216|   7.5670261732852| 36.03150722021661|0.7670126353790614|            132.7|           0.0|0.9305054151624549| 0.06949458483754513|
|  Jackson Heights|      24058|20.605677944966338|30.216813118297463|1.7759227699725666|        151387.19|           0.0|0.5818023110815529|  0.4181976889184471|
|   Yorkville West|     740548| 4.254516331149361|21.371129798473454|2.5279329091430567|        222478.29|           0.0|0.9991614318045555|8.385681954444546E-4|
+-----------------+-----------+------------------+------------------+------------------+-----------------+--------------+------------------+--------------------+
"""

"""
Завдання 4: Агрегація по днях тижня та зонах

1. Підрахувати кількість поїздок за певний день тижня (наприклад, понеділок — Sunday, etc.).
2. Розрахувати частку поїздок з тарифами більше $30 за кожною зоною (high_fare_share).
3. Зберегти файл в у S3: /your-bucket/results/zone_days_statstic/{date}/zone_days_statstic.parquet.
"""

from pyspark.sql.functions import count

trip_counts_by_day = joined_df.groupBy("pickup_zone", "pickup_day_of_week") \
    .agg(count("*").alias("total_trips"))

trip_counts_by_day.show(5)

"""
+--------------------+------------------+-----------+
|         pickup_zone|pickup_day_of_week|total_trips|
+--------------------+------------------+-----------+
|       South Jamaica|               Fri|       1207|
|        Bedford Park|               Fri|        414|
|        Clinton East|               Sun|     147044|
|Central Harlem North|               Sun|       9546|
|Soundview/Castle ...|               Wed|        779|
+--------------------+------------------+-----------+
"""

from pyspark.sql.functions import sum, when

high_fare_stats = joined_df.groupBy("pickup_zone", "pickup_day_of_week").agg(
    (sum(when(col("total_amount") > 30, 1).otherwise(0)) / count("*")).alias("high_fare_share")
)

high_fare_stats.show(5)

"""
+--------------------+------------------+-------------------+
|         pickup_zone|pickup_day_of_week|    high_fare_share|
+--------------------+------------------+-------------------+
|       South Jamaica|               Fri| 0.7613918806959403|
|        Bedford Park|               Fri| 0.6086956521739131|
|        Clinton East|               Sun| 0.1901063627213623|
|    Roosevelt Island|               Tue| 0.5439330543933054|
|Central Harlem North|               Sun|0.25780431594385084|
+--------------------+------------------+-------------------+
"""

output_path_agg = f"s3://robot-dreams-source-data/results/zone_statistic/mmalyhina_zone_days_statstic.parquet"

zone_summary.coalesce(1).write.mode("overwrite").parquet(output_path_agg)

df_mmalyhina = spark.read.parquet(
    "s3://robot-dreams-source-data/results/zone_statistic/mmalyhina_zone_days_statstic.parquet")

df_mmalyhina.show(5)

"""
+--------------------+-----------+------------------+------------------+-------------------+-----------------+--------------+------------------+--------------------+
|         pickup_zone|total_trips| avg_trip_distance|  avg_total_amount|     avg_tip_amount|max_trip_distance|min_tip_amount|      yellow_share|         green_share|
+--------------------+-----------+------------------+------------------+-------------------+-----------------+--------------+------------------+--------------------+
|         Westerleigh|         51|12.006274509803923| 64.55176470588236|  4.930392156862745|             51.0|           0.0|               1.0|                 0.0|
|Charleston/Totten...|          8|27.827500000000004|            116.53|           13.56875|             57.2|           0.0|               1.0|                 0.0|
|      Pelham Parkway|       1677| 7.896171735241504|35.488753726893265|0.21365533691115088|            39.24|           0.0|0.9725700655933214|0.027429934406678593|
|           Rego Park|       6604|10.882057843731072|34.512774076317385|  1.951814052089643|         25742.79|           0.0|  0.61114476075106| 0.38885523924894005|
|   Kew Gardens Hills|       2216| 7.567026173285198| 36.03150722021661| 0.7670126353790615|            132.7|           0.0|0.9305054151624549| 0.06949458483754513|
+--------------------+-----------+------------------+------------------+-------------------+-----------------+--------------+------------------+--------------------+
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SubwayAnalyze").getOrCreate()

df = spark.read.csv(
    "/user/maria_dev/subway_preprocessed/*.csv",
    header=True,
    inferSchema=True
)

df.createOrReplaceTempView("subway")



# station peaple top 10 by year
year_station_top = spark.sql("""
SELECT year, station, total_passenger
FROM(SELECT year, station, SUM(total_passenger) AS total_passenger,
    ROW_NUMBER() OVER(
        PARTITION BY year
        ORDER BY SUM(total_passenger) DESC) AS rrank
    FROM subway
    GROUP BY year, station)t

WHERE rrank <= 10
ORDER BY year, total_passenger DESC """)

year_station_top.coalesce(1).write \
.option("header","true") \
.option("encoding","UTF-8") \
.mode("overwrite") \
.csv("/user/maria_dev/result_year_station_top10")


# the  most people station by subway line
year_line_top = spark.sql("""
SELECT year, line, station, total_passenger
FROM(SELECT year, line, station, SUM(total_passenger) AS total_passenger,
    ROW_NUMBER() OVER(
        PARTITION BY year, line
        ORDER BY SUM(total_passenger) DESC) AS rannk
    FROM subway
    GROUP BY year, line, station)t

WHERE rannk = 1
ORDER BY year, line """)

year_line_top.coalesce(1).write \
.option("header","true") \
.option("encoding","UTF-8") \
.mode("overwrite") \
.csv("/user/maria_dev/result_year_line_top")


# Changes in people by year and subway line
year_line_change = spark.sql("""
SELECT year, line, SUM(total_passenger) AS total_passenger
FROM subway
GROUP BY year, line
ORDER BY year, line""")

year_line_change.coalesce(1).write \
.option("header","true") \
.option("encoding","UTF-8") \
.mode("overwrite") \
.csv("/user/maria_dev/result_year_line_change")









year_total.coalesce(1).write \
.option("header","true") \
.option("encoding","UTF-8") \
.mode("overwrite") \
.csv("/user/maria_dev/result_year_total")

line_top.coalesce(1).write \
.option("header","true") \
.option("encoding","UTF-8") \
.mode("overwrite") \
.csv("/user/maria_dev/result_line_top")
print("Analysis completed.")





p


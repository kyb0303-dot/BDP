from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring

spark = SparkSession.builder.appName("SubwayPreprocess").getOrCreate()

df_old = spark.read.csv(
    [
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_2015.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_2016.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_2017.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_2017.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_2018.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_2019.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_2020.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_2021.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_2022.csv"

    ],
    header = True, inferSchema=True, encoding="cp949"
)

df_new = spark.read.csv(
    [
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_202504.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_202505.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_202506.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_202507.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_202508.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_202509.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_202510.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_202511.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_202512.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_202601.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_202602.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_202603.csv",
        "/user/maria_dev/subway_project/CARD_SUBWAY_MONTH_202604.csv"
    ],
    header=True, inferSchema=True, encoding="UTF-8"
)
df = df_old.unionByName(df_new)



df = df.toDF("use_date", "line", "station", "ride", "alight", "reg_date")
df = df.na.drop(subset=["use_date", "line", "station", "ride", "alight"])
df = df.withColumn("ride", col("ride").cast("int"))
df = df.withColumn("alight", col("alight").cast("int"))

df = df.withColumn("year", substring("use_date", 1, 4))
df = df.withColumn("month", substring("use_date", 5, 2))
df = df.withColumn("day", substring("use_date", 7, 2))
df = df.withColumn("total_passenger", col("ride") + col("alight"))
df = df.drop("reg_date")
df.write.option("header", "true") \
    .option("encoding", "UTF-8") \
    .mode("overwrite") \
    .csv("/user/maria_dev/subway_preprocessed")

print("Preprocessing completed.")

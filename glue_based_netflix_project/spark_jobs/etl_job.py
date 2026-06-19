import os
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ["PATH"]
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when , count

os.makedirs("output", exist_ok=True)

# -----------------------------
# 1. Initialize Spark Session
# -----------------------------

spark = SparkSession.builder \
    .appName("GlueStyleETL") \
    .master("local[*]") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .getOrCreate()

# -----------------------------
# 2. Extract (Read raw data)
# -----------------------------

df = spark.read.csv("data/raw/netflix.csv", header=True, inferSchema=True)

print("RAW DATA SCHEMA")
df.printSchema()

# -----------------------------
# 3. Transform (Cleaning)
# -----------------------------

# Handle nulls
df = df.fillna({
    "country": "Unknown",
    "rating": "Not Rated"
})

#Remove Duplicates

df=df.dropDuplicates()

#Standardize column values

df=df.withColumn("type", when(col("type")=="Movie", "MOVIE").otherwise("TV SHOW"))

before_count = df.count()
df = df.filter(col("release_year").rlike("^[0-9]{4}$"))
after_count = df.count()
print(f"Filtered {before_count - after_count} rows with malformed release_year values")

# -----------------------------
# 4. Analytics Transformations
# -----------------------------


#Movies vs TV shows

type_count=df.groupBy("type").count()


#Top countries producing content

country_count=df.groupBy("country").count().orderBy(col("count").desc())

#Content by release year

year_count = df.groupBy("release_year").count().orderBy("release_year")

# -----------------------------
# 5. Load (Write output)
# -----------------------------

type_count.toPandas().to_csv("output/type_count.csv", index=False)
country_count.toPandas().to_csv("output/country_count.csv", index=False)
year_count.toPandas().to_csv("output/year_count.csv", index=False)

print("ETL pipeline completed successfully")

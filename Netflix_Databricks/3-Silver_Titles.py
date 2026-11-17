# Databricks notebook source

from pyspark.sql.functions import col, trim

source = "abfss://bronze@netflixls.dfs.core.windows.net/netflix_titles"
target = "abfss://silver@netflixls.dfs.core.windows.net/netflix_titles"

# Read Bronze Delta data
df = spark.read.format("delta").load(source)

# Apply basic Silver cleaning
df_clean = (
    df.filter(col("show_id").isNotNull())
      .dropDuplicates(["show_id"])
      .withColumn("title", trim(col("title")))
      .withColumn("release_year", col("release_year").cast("int"))
)

# Write cleaned data to Silver
(
    df_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(target)
)

print("Netflix Titles successfully loaded into Silver")


# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window

source = "abfss://bronze@netflixls.dfs.core.windows.net/netflix_titles"
target = "abfss://silver@netflixls.dfs.core.windows.net/netflix_titles"

df = spark.read.format("delta").load(source)

# Keep your earlier basic cleaning
df_clean = (
    df.filter(F.col("show_id").isNotNull())
      .dropDuplicates(["show_id"])
      .withColumn("title", F.trim(F.col("title")))
      .withColumn("release_year", F.col("release_year").cast("int"))
)

df_clean.printSchema()


# COMMAND ----------



df_clean = (

    df_clean

    .withColumn(

        "duration_minutes",

        F.col("duration_minutes").cast("int")

    )

    .withColumn(

        "duration_seasons",

        F.col("duration_seasons").cast("int")

    )

    .fillna({

        "duration_minutes": 0,

        "duration_seasons": 1

    })

)
 
df_clean.printSchema()

 

# COMMAND ----------


df_clean = (
    df_clean
    .withColumn(
        "short_title",
        F.split(F.col("title"), ":").getItem(0)
    )
    .withColumn(
        "rating_prefix",
        F.split(F.col("rating"), "-").getItem(0)
    )
)


# COMMAND ----------


df_clean = df_clean.withColumn(
    "type_flag",
    F.when(F.col("type") == "Movie", 1)
     .when(F.col("type") == "TV Show", 2)
     .otherwise(0)
)


# COMMAND ----------


df_clean = df_clean.withColumn(
    "duration_ranking",
    F.dense_rank().over(
        Window.orderBy(
            F.col("duration_minutes").desc()
        )
    )
)


# COMMAND ----------


df_clean.createOrReplaceTempView("netflix_titles_temp")

df_summary = spark.sql("""
    SELECT type, COUNT(*) AS total_count
    FROM netflix_titles_temp
    GROUP BY type
""")

df_summary.show()


# COMMAND ----------


(
    df_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(target)
)

print("Netflix Titles transformations saved to Silver")


# COMMAND ----------


df_check = spark.read.format("delta").load(target)

print("Total records:", df_check.count())
df_check.printSchema()

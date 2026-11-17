# Databricks notebook source

from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_path = "abfss://silver@netflixls.dfs.core.windows.net/"


# COMMAND ----------


@dp.materialized_view(name="gold_netflix_directors")
def gold_netflix_directors():
    df = spark.read.format("delta").load(
        silver_path + "netflix_directors"
    )
    return df


# COMMAND ----------


lookup_rules = {
    "valid_show_id": "show_id IS NOT NULL"
}


# COMMAND ----------


# Gold Netflix Cast
@dp.materialized_view(name="gold_netflix_cast")
@dp.expect_all_or_drop(lookup_rules)
def gold_netflix_cast():
    return spark.read.format("delta").load(
        silver_path + "netflix_cast"
    )


# Gold Netflix Category
@dp.materialized_view(name="gold_netflix_category")
@dp.expect_all_or_drop(lookup_rules)
def gold_netflix_category():
    return spark.read.format("delta").load(
        silver_path + "netflix_category"
    )


# Gold Netflix Countries
@dp.materialized_view(name="gold_netflix_countries")
@dp.expect_all_or_drop(lookup_rules)
def gold_netflix_countries():
    return spark.read.format("delta").load(
        silver_path + "netflix_countries"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC Titles Processing

# COMMAND ----------


@dp.temporary_view(name="staging_netflix_titles")
def staging_netflix_titles():
    df = spark.read.format("delta").load(
        silver_path + "netflix_titles"
    )
    return df


# COMMAND ----------


@dp.temporary_view(name="transformed_netflix_titles")
def transformed_netflix_titles():
    df = spark.read.table("staging_netflix_titles")

    df = df.withColumn("new_flag", F.lit(1))

    return df


# COMMAND ----------


# Data quality rules for Netflix Titles
master_rules = {
    "valid_show_id": "show_id IS NOT NULL",
    "valid_new_flag": "new_flag IS NOT NULL"
}

@dp.materialized_view(name="gold_netflix_titles")
@dp.expect_all_or_drop(master_rules)
def gold_netflix_titles():
    df = spark.read.table("transformed_netflix_titles")
    return df

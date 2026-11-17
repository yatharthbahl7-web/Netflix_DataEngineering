# Databricks notebook source
# MAGIC %md
# MAGIC INCREMENTAL DATA LOADING USING AUTOLOADER

# COMMAND ----------

# DBTITLE 1,Create schema
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS netflix_adb_yb.net_schema;

# COMMAND ----------

checkpoint_location= "abfss://silver@netflixls.dfs.core.windows.net/checkpoint"

# COMMAND ----------


df=(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "csv")
 .option("cloudFiles.schemaLocation", checkpoint_location)
 .load("abfss://raw@netflixls.dfs.core.windows.net")
)


# COMMAND ----------

# DBTITLE 1,Display stream

query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "abfss://silver@netflixls.dfs.core.windows.net/netflix_titles_write_checkpoint"
    )
    .trigger(availableNow=True)
    .start("abfss://bronze@netflixls.dfs.core.windows.net/netflix_titles")
)

query.awaitTermination()

print("Data successfully ingested into Bronze!")

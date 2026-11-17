# Databricks notebook source

titles_path = "abfss://bronze@netflixls.dfs.core.windows.net/netflix_titles"

df_titles = spark.read.format("delta").load(titles_path)

print("Netflix Titles records:", df_titles.count())


# COMMAND ----------


bronze_path = "abfss://bronze@netflixls.dfs.core.windows.net/"

files = dbutils.fs.ls(bronze_path)

for file in files:
    print(file.name, "Folder:", file.isDir())


# COMMAND ----------


bronze_path = "abfss://bronze@netflixls.dfs.core.windows.net/"

for folder in dbutils.fs.ls(bronze_path):
    if folder.isDir() and "titles" not in folder.name.lower():
        print("FOLDER:", folder.name)

        for file in dbutils.fs.ls(folder.path):
            print("  FILE:", file.name)


# COMMAND ----------


# Create two parameters
dbutils.widgets.text("folder_name", "netflix_cast")
dbutils.widgets.text("file_name", "netflix_cast.csv")

# Retrieve their values
folder_name = dbutils.widgets.get("folder_name").strip()
file_name = dbutils.widgets.get("file_name").strip()

print("Folder:", folder_name)
print("File:", file_name)


# COMMAND ----------


# Construct the source path using parameters
source_path = f"abfss://bronze@netflixls.dfs.core.windows.net/{folder_name}/{file_name}"

# Read the CSV file
df_lookup = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(source_path)
)

print("Reading:", source_path)
print("Total records:", df_lookup.count())


# COMMAND ----------


# Construct the Silver destination path
target_path = f"abfss://silver@netflixls.dfs.core.windows.net/{folder_name}"

# Save the data as Delta
(
    df_lookup.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(target_path)
)

print("Successfully saved to:", target_path)


# COMMAND ----------


datasets = [
    "netflix_cast",
    "netflix_category",
    "netflix_countries",
    "netflix_directors",
    "netflix_titles"
]

for name in datasets:
    path = f"abfss://silver@netflixls.dfs.core.windows.net/{name}"
    df = spark.read.format("delta").load(path)
    print(name, ":", df.count(), "records")

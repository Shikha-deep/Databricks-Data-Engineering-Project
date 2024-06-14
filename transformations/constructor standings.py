# Databricks notebook source
# MAGIC %md
# MAGIC #### Produce constructor standings.

# COMMAND ----------

# MAGIC %run "../Includes/Configurations"

# COMMAND ----------


race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import col, when, count, sum, desc, rank

# COMMAND ----------

constructor_standing_df = race_results_df.groupBy("race_year","team") \
    .agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("win"))

# COMMAND ----------

constructor_standing_df.filter("race_year == 2020")

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

constructorrankspec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("win"))
final_df = constructor_standing_df.withColumn("rank",rank().over(constructorrankspec))

# COMMAND ----------

display(driver_standing_df.filter("race_year == 2020"))

# COMMAND ----------

final_df.write.parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------


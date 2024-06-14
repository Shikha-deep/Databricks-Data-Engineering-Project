# Databricks notebook source
# MAGIC %md
# MAGIC #### Produce driver standings.

# COMMAND ----------

# MAGIC %run "../Includes/Configurations"

# COMMAND ----------


race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import col, when, count, sum, desc, rank

# COMMAND ----------

driver_standing_df = race_results_df.groupBy("race_year","driver_name","driver_nationality","team") \
    .agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("win"))

# COMMAND ----------

driver_standing_df.filter("race_year == 2020")

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

driverrankspec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("win"))
final_df = driver_standing_df.withColumn("rank",rank().over(driverrankspec))

# COMMAND ----------

display(driver_standing_df.filter("race_year == 2020"))

# COMMAND ----------

final_df.write.parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------


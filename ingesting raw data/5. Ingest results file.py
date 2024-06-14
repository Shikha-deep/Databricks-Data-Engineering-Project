# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest results.json file.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1. Read the json file using spark dataframe reader API.

# COMMAND ----------

# MAGIC %run "../Includes/Configurations"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

results_schema = StructType([StructField("resultId", IntegerType(), False),
                            StructField("raceId",  IntegerType(), True), 
                            StructField("driverId", IntegerType(), True),
                            StructField("constructorId", IntegerType(), True),
                            StructField("number", IntegerType(), True),
                            StructField("grid", IntegerType(), True),
                            StructField("position", IntegerType(), True),
                            StructField("positionText", StringType(), True), 
                            StructField("positionOrder", IntegerType(), True),
                            StructField("points", FloatType(), True),
                            StructField("laps", IntegerType(), True),
                            StructField("time", IntegerType(), True),
                            StructField("milliseconds", IntegerType(), True),
                            StructField("fastestLap", IntegerType(), True), 
                            StructField("rank", IntegerType(), True),
                            StructField("fastestLapTime", StringType(), True),
                            StructField("fastestLapSpeed", FloatType(), True),
                            StructField("statusId", StringType(), True)]
    )

# COMMAND ----------

results_df = spark.read \
.option("header", True) \
.schema(results_schema) \
.json("/mnt/shikhsdlsgen2/raw/results.json")




# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2. Rename the required columns and add new columns.
# MAGIC     

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

results__renamed_df = results_df.withColumnRenamed("resultId","result_id") \
                                .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                                .withColumnRenamed("statusId","status_id") \
                                .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
                                .withColumnRenamed("raceId","race_id") \
                                .withColumnRenamed("driverId","driver_id") \
                                .withColumnRenamed("constructorId","constructor_id") \
                                .withColumnRenamed("positionText","position_text") \
                                .withColumnRenamed("positionOrder","position_order") \
                                .withColumnRenamed("fastestLap","fastest_lap") \
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("data_source", lit(v_data_source))

                            

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3. Drop the unwanted columns.

# COMMAND ----------

from pyspark.sql.functions import col

results_final_df = results__renamed_df.drop(col("status_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4. Write the output to processed container in parquet format.

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/shikhsdlsgen2/processed/results")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/shikhsdlsgen2/processed/results

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------


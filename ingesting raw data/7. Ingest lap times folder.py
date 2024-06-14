# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest lap time folder with multiple csv file.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1. Read the csv file using spark dataframe reader API.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

lap_time_schema = StructType([StructField("raceId",  IntegerType(), True), 
                            StructField("driverId", IntegerType(), True),
                            StructField("lap", IntegerType(), True),
                            StructField("position", IntegerType(), True),
                            StructField("time", IntegerType(), True),
                            StructField("milliseconds", IntegerType(), True),
                            ])

# COMMAND ----------

lap_time_df = spark.read \
.option("header", True) \
.schema(lap_time_schema) \
.csv("/mnt/shikhsdlsgen2/raw/lap_times")




# COMMAND ----------

display(lap_time_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2. Rename the required columns and add new columns.
# MAGIC     

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

lap_time_final_df = lap_time_df.withColumnRenamed("qualifyId","race_id") \
                               .withColumnRenamed("raceId","race_id") \
                                .withColumnRenamed("driverId","driver_id") \
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("data_source", lit(v_data_source))

                            

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4. Write the output to processed container in parquet format.

# COMMAND ----------

lap_time_final_df.write.mode("overwrite").parquet("/mnt/shikhsdlsgen2/processed/lap_time")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/shikhsdlsgen2/processed/lap_time

# COMMAND ----------


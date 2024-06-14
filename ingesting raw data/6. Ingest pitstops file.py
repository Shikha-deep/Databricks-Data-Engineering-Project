# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1. Read the json file using spark dataframe reader API.

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType([StructField("raceId", IntegerType(), False),
                            StructField("driverId",  IntegerType(), True), 
                            StructField("stop", StringType(), True),
                            StructField("lap", IntegerType(), True),
                            StructField("time", StringType(), True),
                            StructField("duration", StringType(), True),
                            StructField("milliseconds", IntegerType(), True) ])
                 

# COMMAND ----------

pit_stops_df = spark.read \
.option("header", True) \
.option("multiline", True) \
.schema(pit_stops_schema) \
.json("/mnt/shikhsdlsgen2/raw/pit_stops.json")


display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2. Rename the required columns and add new columns.
# MAGIC     

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

final_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
                       .withColumnRenamed("driverId", "driver_id") \
                       .withColumn("ingestion_date", current_timestamp()) \
                       .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4. Write the output to processed container in parquet format.

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/shikhsdlsgen2/processed/pitstops")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt//shikhsdlsgen2/processed/pitstops

# COMMAND ----------

display(spark.read.parquet("/mnt/shikhsdlsgen2/processed/pitstops"))

# COMMAND ----------


# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest qualifying folder with multiple json file.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1. Read the json file using spark dataframe reader API.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

qualifying_schema = StructType([StructField("qualifyId",  IntegerType(), False), 
                            StructField("raceId",  IntegerType(), False), 
                            StructField("driverId", IntegerType(), True),
                            StructField("constructorId", IntegerType(), True),
                            StructField("position", IntegerType(), True),
                            StructField("number", IntegerType(), True),
                            StructField("q1", StringType(), True),
                            StructField("q2", StringType(), True),
                            StructField("q3", StringType(), True)
                            ])

# COMMAND ----------

qualifying_df = spark.read \
.option("header", True) \
.schema(qualifying_schema) \
.option("multiline", True) \
.json("/mnt/shikhsdlsgen2/raw/qualifying")




# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2. Rename the required columns and add new columns.
# MAGIC     

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id") \
                                .withColumnRenamed("raceId","race_id") \
                                .withColumnRenamed("driverId","driver_id") \
                                .withColumnRenamed("constructorId","constructor_id") \
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("data_source", lit(v_data_source))

                            

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4. Write the output to processed container in parquet format.

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet("/mnt/shikhsdlsgen2/processed/qualifying")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/shikhsdlsgen2/processed/qualifying

# COMMAND ----------


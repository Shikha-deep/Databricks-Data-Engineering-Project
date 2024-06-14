# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1. Read the json file using spark dataframe reader API.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType([StructField("forename",StringType(), True),
                           StructField("surname", StringType(), True)])


# COMMAND ----------

driver_schema = StructType([StructField("driverId", IntegerType(), True),
                           StructField("driverRef", StringType(), True),
                           StructField("number", IntegerType(), True),
                           StructField("code", StringType(), True),
                           StructField("name", name_schema, True),
                           StructField("dob", DateType(), True),
                           StructField("nationality", StringType(), True),
                           StructField("url", StringType(), True)])


# COMMAND ----------

drivers_df = spark.read \
.option("header", True) \
.schema(driver_schema) \
.json("/mnt/shikhsdlsgen2/raw/drivers.json")

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2. Rename the required columns and add new columns.
# MAGIC     

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit


drivers_renamed_df = drivers_df.withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("driverRef","driver_ref") \
    .withColumn("ingestion_date",current_timestamp()) \
    .withColumn("name",concat(col("name.forename"),lit(" "), col("name.surname"))) \
    .withColumn("data_source", lit(v_data_source))
 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3. Drop the unwanted columns.

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4. Write the output to processed container in parquet format.

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/shikhsdlsgen2/processed/drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/shikhsdlsgen2/processed/drivers

# COMMAND ----------

display(spark.read.parquet("/mnt/shikhsdlsgen2/processed/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


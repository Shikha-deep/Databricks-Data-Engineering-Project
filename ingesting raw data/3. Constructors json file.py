# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest the Constructors.json file.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1. Read the json file using spark dataframe reader. This time we are defining schema with different method called DDL style. It can be used to previous file formats as well.

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json("/mnt/shikhsdlsgen2/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2. Drop unwanted column from the dataframe.
# MAGIC             
# MAGIC #####     * If we have multiple dataframes then we can drop like this 
# MAGIC
# MAGIC
# MAGIC          constructor_dropped_df = constructor_df.drop(constructor_df["url"])
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3. Renaming the column and adding ingestion_date column

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id") \
  .withColumnRenamed("constructorRef", "constructor_ref") \
  .withColumn("ingestion_date", current_timestamp()) \
  .withColumn("data_source", lit(v_data_source))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4. Write output to parquet file.

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/shikhsdlsgen2/processed/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/shikhsdlsgen2/processed/constructors

# COMMAND ----------


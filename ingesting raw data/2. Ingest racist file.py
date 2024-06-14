# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting races.csv file.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Step : Read the CSV file using spark dataframe reader API.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType([StructField("raceid",IntegerType(), False),
                           StructField("year", IntegerType(), True),
                           StructField("round", IntegerType(), True),
                           StructField("circuitid", IntegerType(), True),
                           StructField("name", StringType(), True),
                           StructField("date", DateType(), True),
                           StructField("time", StringType(), True),
                           StructField("url", StringType(), True)])

display(races_schema)

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv("/mnt/shikhsdlsgen2/raw/races.csv")

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Step : Add ingestion_time and races_timestamp to the dataframe.

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
.withColumn("races_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn("data_source", lit(v_data_source))
display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Step : Selecting only the columns required and renaming also.
# MAGIC

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceid').alias('race_id'),col('year').alias('race_year'), \
                                                   col('round'),col('circuitid').alias('circuit_id'), col('name'), \
                                                    col('ingestion_date'), col('races_timestamp')
                                                    )
display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Step : Write the cleaned/ transformed data to processed container in parquet format. Created partitions also.

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/shikhsdlsgen2/processed/races')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/shikhsdlsgen2/processed/races

# COMMAND ----------

display(spark.read.parquet('/mnt/shikhsdlsgen2/processed/races'))

# COMMAND ----------


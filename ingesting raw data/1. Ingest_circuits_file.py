# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Circuits.csv file
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../Includes/Configurations"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1. Read the CSV file using spark dataframe reader.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


# COMMAND ----------

circuit_schema = StructType([StructField("circuitId", IntegerType(), False),
                             StructField("circuitRef", StringType(), True),
                             StructField("name", StringType(), True),
                             StructField("location", StringType(), True),
                             StructField("country", StringType(), True),
                             StructField("lat", DoubleType(), True),
                             StructField("lng", DoubleType(), True),
                             StructField("alt", IntegerType(), True),
                             StructField("url", StringType(), True)])
                            

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Reading a data from the circuits file.
# MAGIC ##### 1. To make the first row as header we have put option method.
# MAGIC ##### 2. To get the schemaname of the data we used inferschema of option method. It will read the part of the data and idntify the datatype for each field. This is not the efficient way to see schema in production. It will be only usefull for small data or development purpose only.
# MAGIC ##### 3. That's why we will be creating a variable and define all the schema above and use that variabl so the code no need to read data every time.
# MAGIC
# MAGIC ##### circuits_df = spark.read.option("header", True).option("inferschema",True).csv("dbfs:/mnt/shikhsdlsgen2/raw/circuits.csv")
# MAGIC
# MAGIC

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuit_schema) \
.csv(f"{raw_folder_path}/circuits.csv")

circuits_df

# COMMAND ----------

/# # To see the type of the data.
# type(circuits_df)

# # To see top 20 records.
# circuits_df.show()

# # To see data in proper format.
# display(circuits_df)

# COMMAND ----------

# # Accessing file from ADLS without mounting.
# #
# circuits_df = spark.read.csv("abfss://raw@shikhsdlsgen2.dfs.core.windows.net/circuits.csv")
# circuits_df

# COMMAND ----------

# To see the schema of the data
circuits_df.printSchema()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Select only the required columns.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 1. The first way to select only required columns are:
# MAGIC #### circuits_selected_df = circuits_df.select("circuitid","circuitref","name")
# MAGIC #### display(circuits_selected_df)
# MAGIC
# MAGIC ##### 2. The sec way to select only required columns are:
# MAGIC #### circuits_selected_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name)
# MAGIC #### display(circuits_selected_df)
# MAGIC
# MAGIC
# MAGIC ##### 3. The third way to select only required columns are:
# MAGIC #### circuits_selected_df = circuits_df.select(circuits_df["circuitId"].alias("circID"),circuits_df["circuitRef"],circuits_df["name"])
# MAGIC #### display(circuits_selected_df)
# MAGIC
# MAGIC
# MAGIC ##### 4. The fourth way to select only required columns are:
# MAGIC
# MAGIC #### from pyspark.sql.functions import col
# MAGIC
# MAGIC #### circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name").alias("circuit_name"))
# MAGIC #### display(circuits_selected_df)
# MAGIC
# MAGIC
# MAGIC
# MAGIC ####  Note : The first one will only allow us to see the selected data but the other three let us apply any column based function for exp. usig alias.
# MAGIC
# MAGIC

# COMMAND ----------

# We are using the fourth way in our project.

from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))
display(circuits_selected_df)



# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename the columns as required.

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitid","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude") \
    .withColumn("data_source", lit(v_data_source))
display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------


circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to our datalake in parquet format.

# COMMAND ----------


circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/shikhsdlsgen2/processed/circuits

# COMMAND ----------

display(spark.read.parquet("/mnt/shikhsdlsgen2/processed/circuits"))

# COMMAND ----------


# Databricks notebook source
# MAGIC %md
# MAGIC #### Read all the data.

# COMMAND ----------

# MAGIC %run "../Includes/Configurations"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location","circuit_location") 


# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
    .withColumnRenamed("name","race_name") \
    .withColumnRenamed("races_timestamp","race_date") 


# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("name","driver_name") \
.withColumnRenamed("number","driver_number") \
.withColumnRenamed("nationality","driver_nationality") 


# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name","team") 


# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
    .withColumnRenamed("time","race_time") 


# COMMAND ----------

# MAGIC %md
# MAGIC #### Join races and circuits table.

# COMMAND ----------

races_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id==races_df.circuit_id,"inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date,circuits_df.circuit_location) 

# COMMAND ----------

race_results_df = results_df.join(races_circuit_df,races_circuit_df.race_id==results_df.race_id) \
 .join(drivers_df,drivers_df.driver_id==results_df.driver_id) \
.join(constructors_df,constructors_df.constructor_id==results_df.constructor_id) 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_year","race_name","race_date", "circuit_location","driver_name", "driver_number", "driver_nationality", "team","grid","fastest_lap","race_time","points","position") \
    .withColumn("created_date",current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------


# Databricks notebook source
# MAGIC %md
# MAGIC ## Service Principle

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.retailpr.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.retailpr.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.retailpr.dfs.core.windows.net", "cd2e4237-47fc-473e-8cd1-202b611213b3")
spark.conf.set("fs.azure.account.oauth2.client.secret.retailpr.dfs.core.windows.net", "1ZX8Q~rNO0nBJABewgN7Wo-sL4nduZ8lPSs7JduL")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.retailpr.dfs.core.windows.net", "https://login.microsoftonline.com/814d918e-1a90-433a-a7dc-eb9270035153/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read CSV File & Parquet File

# COMMAND ----------

df = spark.read.format("parquet").option("header",True).option("inferSchema",True).load('abfss://bronze@retailpr.dfs.core.windows.net/parquet/master_employees.parquet')

# COMMAND ----------

df.display()

# COMMAND ----------

df = spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@retailpr.dfs.core.windows.net/CSV/master_employees.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Schema Differences CSV & Parquet

# COMMAND ----------

# Read the CSV file and infer the schema
df = spark.read.csv("abfss://bronze@retailpr.dfs.core.windows.net/CSV/master_employees.csv", header=True, inferSchema=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = spark.read.parquet("abfss://bronze@retailpr.dfs.core.windows.net/parquet/primary_event_log.parquet", header=True, inferSchema=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparsion benfits

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step:1 Check size of original file

# COMMAND ----------

# Replace with your actual file path
file_info = dbutils.fs.ls("abfss://bronze@retailpr.dfs.core.windows.net/CSV/master_employees.csv")

# Get size in MB
csv_size_mb = file_info[0].size / (1024 * 1024)
print(f"CSV file size: {csv_size_mb:.2f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step:2 Convert CSV TO Compressed format(Eg:Parquet & Ezip)

# COMMAND ----------

df = spark.read.csv("abfss://bronze@retailpr.dfs.core.windows.net/CSV/master_employees.csv", header=True, inferSchema=True)

# Save as Parquet
df.write.mode("overwrite").parquet("abfss://silver@retailpr.dfs.core.windows.net/master_employees.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step:3: compressed file size

# COMMAND ----------

# List all part files in the folder (Parquet files are usually split into multiple parts)
compressed_info = dbutils.fs.ls("abfss://bronze@retailpr.dfs.core.windows.net/parquet/primary_event_log.parquet/")

# Sum all part file sizes (especially for Parquet, which is split into parts)
compressed_size = sum(f.size for f in compressed_info) / (1024 * 1024)  # Convert to MB

# Print the compressed file size in MB
print(f"Compressed file size: {compressed_size:.2f} MB")


# COMMAND ----------

# MAGIC %md
# MAGIC ## step:4: Calculate compressed file

# COMMAND ----------

csv_size_mb = 2.82           # Example: 2.82 MB CSV
compressed_size_mb = 1.0    # Example: 1.0 MB Parquet

compression_ratio = compressed_size_mb / csv_size_mb
space_saved_percent = 100 - (compression_ratio * 100)

print(f"Compression ratio: {compression_ratio:.2f}")
print(f"Space saved: {space_saved_percent:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV Loading performance matrix

# COMMAND ----------

import time

# COMMAND ----------

start_time = time.time()

# Load CSV file
df = spark.read.option("header", True).option("inferSchema", True).csv("abfss://bronze@retailpr.dfs.core.windows.net/parquet/primary_event_log.parquet")

end_time = time.time()

load_time = end_time - start_time
print(f"⏱ Load time: {load_time:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver layer Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading CSV

# COMMAND ----------

df = spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@retailpr.dfs.core.windows.net/CSV/master_employees.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trans:1 Null Values Checking

# COMMAND ----------

from pyspark.sql.functions import col,when,sum

# COMMAND ----------

null_counts = df.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns])

null_counts.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trans:2: Find out the Duplicates in Table

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

duplicate_rows = df.groupBy(df.columns).count().filter("count > 1")

duplicate_rows.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trans:3 : Data Type conversion

# COMMAND ----------

# Read the CSV file and infer the schema
df = spark.read.csv("abfss://bronze@retailpr.dfs.core.windows.net/CSV/master_employees.csv", header=True, inferSchema=True)

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trans: 3.1 Conversion from String Data Type to Integer

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_casted = df.withColumn("date_of_birth", col("date_of_birth").cast("int")) \
              .withColumn("manager_employee_id", col("manager_employee_id").cast("double"))

# COMMAND ----------

df_casted.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trans: 3.2 Again Conversion to previous Data types

# COMMAND ----------

df_casted = df.withColumn("date_of_birth", col("date_of_birth").cast("date")) \
              .withColumn("manager_employee_id", col("manager_employee_id").cast("string"))

# COMMAND ----------

df_casted.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trans:4: Rename the column

# COMMAND ----------

df = df.withColumnRenamed("employee_id", "emp_id") \
       .withColumnRenamed("email", "customer_email")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumnRenamed("emp", "empployee_id") \
       .withColumnRenamed("customer_email", "email")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trans:5: Findout Employee Age

# COMMAND ----------

from pyspark.sql.functions import current_date, datediff, floor

df = df.withColumn("age", floor(datediff(current_date(), "date_of_birth") / 365.25))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trans:6: Extract domain from email

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trans: 7: Lower case to Upper case

# COMMAND ----------

from pyspark.sql.functions import upper

df = df.withColumn("job_title", upper(df.job_title))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trans:8: Replace null values & Blank values

# COMMAND ----------

df = df.fillna({"email": "Unknown"})

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trans:9: Categorize age group

# COMMAND ----------

from pyspark.sql.functions import when

df = df.withColumn("age", when(df.age < 18, "Child")
                                  .when((df.age >= 18) & (df.age <= 35), "Youth")
                                  .when((df.age > 35) & (df.age <= 60), "Adult")
                                  .otherwise("Senior"))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trans:10: Combine column first name & lastname converts into full name

# COMMAND ----------

from pyspark.sql.functions import concat_ws

df = df.withColumn("full_address", concat_ws("", "first_name", "last_name"))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Trans:11 Group by Count of Customers

# COMMAND ----------

df.groupBy("emp_id").count().orderBy("count", ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trans:12: Add ingestion time stamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df = df.withColumn("date_of_birth", current_timestamp())

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trans:13 split phone number with custome format

# COMMAND ----------

df = df.withColumn("country_code", split(df["phone"], "-").getItem(0)) \
       .withColumn("mobile", split(df["phone"], "-").getItem(1))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trans:14 Replace the null values in column

# COMMAND ----------

df = df.fillna({"mobile": 0})

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## File uploaded fron bronze to silver with Transformations

# COMMAND ----------

df.write \
    .mode("overwrite") \
    .format("delta") \
    .save("abfss://silver@retailpr.dfs.core.windows.net/silver/")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read CSV File in Silver layer

# COMMAND ----------

# Save as Parquet
df.write.mode("overwrite").parquet("abfss://silver@retailpr.dfs.core.windows.net/silver/master_employees.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Trans4: Remove spaces

# COMMAND ----------

df = spark.read.csv("abfss://bronze@retailpr.dfs.core.windows.net/CSV/master_employees.csv", header=True, inferSchema=True)

# Save as Parquet
df.write.mode("overwrite").csv("abfss://silver@retailpr.dfs.core.windows.net/master_employees.csv")

# COMMAND ----------

df = spark.read.delta("abfss://silver@retailpr.dfs.core.windows.net/silver/part-00000-e988c834-61b3-460c-b5d2-5c063c786e76-c000.snappy.parquet", header=True, inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df = spark.read.format("delta").load("abfss://silver@retailpr.dfs.core.windows.net/master_employees.csv/")
df.show()

# COMMAND ----------


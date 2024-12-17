# Databricks notebook source
# DBTITLE 1,mount the data
# Replace <STORAGE_ACCOUNT>, <CONTAINER_NAME>, and <ACCESS_KEY>
STORAGE_ACCOUNT = "awsdtbricksgen2strg"
CONTAINER_NAME = "awsdtbricksgen2cnt"
ACCESS_KEY = "kp1iySxjaNrDGOTV2IJmKbrerUhlnYin8bDe7fewYFnGKbd+BiQjdtoPUFuwEMx+Nn6M3SHALucR+AStyil2fw=="

MOUNT_POINT = "/mnt/rawdata"

# Check if the mount point already exists
if any(mount.mountPoint == MOUNT_POINT for mount in dbutils.fs.mounts()):
    print(f"Mount point {MOUNT_POINT} already exists. Unmounting...")
    dbutils.fs.unmount(MOUNT_POINT)

# Mount the Azure Blob Storage
print(f"Mounting {CONTAINER_NAME} to {MOUNT_POINT}...")
dbutils.fs.mount(
    source=f"wasbs://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.blob.core.windows.net/",
    mount_point=MOUNT_POINT,
    extra_configs={f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net": ACCESS_KEY}
)

# Verify the mount
print("Verifying the mount...")
display(dbutils.fs.ls(MOUNT_POINT))


# COMMAND ----------

# DBTITLE 1,List the mounts
for mount in dbutils.fs.mounts():
    print(f"Mount Point: {mount.mountPoint}, Source: {mount.source}")

# COMMAND ----------

# DBTITLE 1,Read CSV file into the Spark Dataframe
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/rawdata/sales_data_large.csv")

# COMMAND ----------

# DBTITLE 1,Databricks Spark session to authenticate and access an Azure Data Lake Storage Gen2
spark.conf.set(
    "fs.azure.account.key.awsdtbricksgen2strg.dfs.core.windows.net",
    "kp1iySxjaNrDGOTV2IJmKbrerUhlnYin8bDe7fewYFnGKbd+BiQjdtoPUFuwEMx+Nn6M3SHALucR+AStyil2fw=="
)

# COMMAND ----------

# DBTITLE 1,Provide script lists the contents of a directory in Azure Data Lake Storage Gen2
dbutils.fs.ls("abfss://awsdtbricksgen2cnt@awsdtbricksgen2strg.dfs.core.windows.net/")

# COMMAND ----------

# DBTITLE 1,Create new database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS salesdb;

# COMMAND ----------

# DBTITLE 1,managed table in Databricks using SQL, and the table is sourced from a CSV file stored in Azure Data Lake Storage Gen2.
# MAGIC %sql
# MAGIC CREATE TABLE salesdb.my_table
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header "true",
# MAGIC   inferSchema "true"
# MAGIC )
# MAGIC LOCATION 'abfss://awsdtbricksgen2cnt@awsdtbricksgen2strg.dfs.core.windows.net/sales_data_large.csv';

# COMMAND ----------

# DBTITLE 1,Listing records
# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.salesdb.my_table
# MAGIC  LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC #DATA  TRANSFORMATION

# COMMAND ----------

# DBTITLE 1,removes null and duplicates
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cleaned_sales_data;
# MAGIC CREATE TABLE cleaned_sales_data AS
# MAGIC SELECT
# MAGIC   DISTINCT OrderID,
# MAGIC   Product,
# MAGIC   Price,
# MAGIC   Quantity,
# MAGIC   Region
# MAGIC FROM
# MAGIC   hive_metastore.salesdb.my_table
# MAGIC WHERE
# MAGIC   OrderID IS NOT NULL
# MAGIC   AND Price IS NOT NULL;
# MAGIC Select
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   cleaned_sales_data

# COMMAND ----------

# DBTITLE 1,Add a new column for total sales:
# MAGIC %sql
# MAGIC Drop table if exists transformed_sales_data;
# MAGIC CREATE TABLE transformed_sales_data AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   Price * Quantity AS Total_Sales
# MAGIC FROM
# MAGIC   cleaned_sales_data;
# MAGIC
# MAGIC Select count(*) from transformed_sales_data;

# COMMAND ----------

# DBTITLE 1,Summarize sales data by region.  sql Copy code
# MAGIC %sql
# MAGIC drop table if exists aggregated_sales_data;
# MAGIC CREATE TABLE aggregated_sales_data AS
# MAGIC SELECT
# MAGIC   Region,
# MAGIC   SUM(Total_Sales) AS Total_Sales_Amount,
# MAGIC   COUNT(DISTINCT OrderID) AS Total_Orders
# MAGIC FROM
# MAGIC   transformed_sales_data
# MAGIC GROUP BY
# MAGIC   Region;
# MAGIC select
# MAGIC   count(*)
# MAGIC from
# MAGIC   aggregated_sales_data;

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATING DELTA TABLE 

# COMMAND ----------

# DBTITLE 1,Store Final Table as Delta
# MAGIC %sql
# MAGIC drop table if exists final_sales_data; 
# MAGIC CREATE TABLE final_sales_data USING DELTA AS
# MAGIC SELECT * FROM aggregated_sales_data;
# MAGIC select count(*) from aggregated_sales_data

# COMMAND ----------

# DBTITLE 1,Partition the Data:
# MAGIC %sql
# MAGIC ALTER TABLE final_sales_data SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true);
# MAGIC OPTIMIZE final_sales_data ZORDER BY (Region);

# COMMAND ----------

# DBTITLE 1,Validate Optimized Table
# MAGIC %sql
# MAGIC SELECT * FROM final_sales_data WHERE Region = 'North America';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #  Create SQL Dashboard in Databricks SQL

# COMMAND ----------

# DBTITLE 1,Total region by sales
# MAGIC %sql
# MAGIC SELECT Region, SUM(Total_Sales_Amount) AS Total_Sales
# MAGIC FROM final_sales_data
# MAGIC GROUP BY Region;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Top 5 Regions by Sales
# MAGIC %sql
# MAGIC SELECT Region, Total_Sales_Amount
# MAGIC FROM final_sales_data
# MAGIC ORDER BY Total_Sales_Amount DESC
# MAGIC LIMIT 5;
# MAGIC

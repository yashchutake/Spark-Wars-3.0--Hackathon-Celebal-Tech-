# Databricks notebook source
# DBTITLE 1,DROP DATABASE IF EXISTS
spark.sql("DROP DATABASE IF EXISTS bronze CASCADE ")
spark.sql("DROP DATABASE IF EXISTS silver CASCADE ")
spark.sql("DROP DATABASE IF EXISTS gold CASCADE ")

# COMMAND ----------

# DBTITLE 1,Pre-Requisite : Create Schemas
spark.sql("CREATE DATABASE bronze")
spark.sql("CREATE DATABASE silver")
spark.sql("CREATE DATABASE gold")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Section A: Data Ingestion and Bronze Layer

# COMMAND ----------

# DBTITLE 1,Section A: Data Ingestion and Bronze Layer
# dbutils.fs.ls('dbfs:/FileStore/tables')
dbutils.fs.ls('dbfs:/FileStore/tables')

# COMMAND ----------

# DBTITLE 1,Create a table named 'transactions_bronze’
# transactions_df=spark.read.csv()
spark.sql("""USE schema bronze""")
spark.sql(""" CREATE TABLE IF NOT EXISTS transactions_bronze using CSV options (path="dbfs:/FileStore/tables/66a2634026e86_transactions_dataset.csv",header="True",mode="FAILFAST",inferSchema "True")""")

# COMMAND ----------

# DBTITLE 1,Create a table named 'customer_bronze
# transactions_df=spark.read.csv()
spark.sql("""USE schema bronze""")
spark.sql(""" CREATE TABLE IF NOT EXISTS customer_bronze using CSV options (path="dbfs:/FileStore/tables/66a262dd3659f_customer_dataset.csv",header="True",mode="FAILFAST",inferSchema "True")""")

# COMMAND ----------

# DBTITLE 1,Preview and Count Records:bronze tables.
# Preview first 5 rows
#Display the first 5 rows of both transactions_bronze and customer_bronze tables.
spark.sql("SELECT * FROM bronze.transactions_bronze LIMIT 5").show()
spark.sql("SELECT * FROM bronze.customer_bronze LIMIT 5").show()

# Count total records
transactions_count = spark.sql("SELECT COUNT(*) FROM bronze.transactions_bronze").show()
customers_count = spark.sql("SELECT COUNT(*) FROM bronze.customer_bronze").show()
 

# COMMAND ----------

# MAGIC %md
# MAGIC Questions:

# COMMAND ----------

# DBTITLE 1,Questions:   1.1 Extract records where balance is greater than 15000 and loan is 'yes' from customers_bronze.
# 1.1 Extract records where balance is greater than 15000 and loan is 'yes' from customers_bronze.
customers_filtered = spark.sql("""
SELECT *
FROM bronze.customer_bronze
WHERE balance > 15000 AND lower(trim(loan)) = 'yes'
""")
customers_filtered.show()


# COMMAND ----------

# DBTITLE 1,1.2 Extract transactions from transactions_bronze where transaction_type is 'purchase' and merchant_category is 'travel'.

# 1.2 Extract transactions from transactions_bronze where transaction_type is 'purchase' and merchant_category is 'travel'.
transactions_filtered = spark.sql("""
SELECT *
FROM bronze.transactions_bronze
WHERE transaction_type = 'purchase' AND merchant_category = 'travel'
""")
transactions_filtered.show()


# COMMAND ----------

# DBTITLE 1,1.3 Find the job type of the customer who have done max amount of transaction
# 1.3 Find the job type of the customer who have done max amount of transaction 
max_transaction = spark.sql("""
SELECT customer_id, SUM(transaction_amount) as total_amount
FROM bronze.transactions_bronze
GROUP BY customer_id
ORDER BY total_amount DESC
LIMIT 1
""").collect()[0]

max_transaction_customer_id = max_transaction['customer_id']

customer_job = spark.sql(f"""
SELECT job
FROM bronze.customer_bronze
WHERE customer_id = '{max_transaction_customer_id}'
""")
customer_job.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Section B: Create Silver Tables with Transformations

# COMMAND ----------

# DBTITLE 1,Section B: Create Silver Tables with Transformations
# Load bronze tables
transactions_bronze_df = spark.table("bronze.transactions_bronze")
customers_bronze_df = spark.table("bronze.customer_bronze")


# COMMAND ----------

# DBTITLE 1,Clean transactions_bronze table
# Clean transactions_bronze table
transactions_silver_df = transactions_bronze_df.dropna(subset=["customer_id"]).dropDuplicates()
transactions_silver_df.write.format("delta").mode("overwrite").save("/mnt/dataset/transactions_silver_delta")
spark.sql("""
    drop table if exists silver.transactions_silver
""")
spark.sql("""
    CREATE TABLE silver.transactions_silver
    USING DELTA
    LOCATION '/mnt/dataset/transactions_silver_delta'
""")

# Clean customer_bronze table
customers_silver_df = customers_bronze_df.dropDuplicates()
customers_silver_df.write.format("delta").mode("overwrite").save("/mnt/dataset/customers_silver_delta")
spark.sql("""
    drop table if exists silver.customers_silver
""")
spark.sql("""
    CREATE TABLE silver.customers_silver
    USING DELTA
    LOCATION '/mnt/dataset/customers_silver_delta'
""")


# COMMAND ----------

# DBTITLE 1,Optimization Techniques:
from pyspark.sql.functions import date_format

# Convert the timestamp string to timestamp type if needed
transactions_silver_df = transactions_silver_df.withColumn("timestamp_col", transactions_silver_df["transaction_date"].cast("timestamp"))  

# Extract the date from the timestamp
transactions_silver_df = transactions_silver_df.withColumn("date_col", date_format(transactions_silver_df["timestamp_col"], "yyyy-MM-dd"))

# Show the result
# transactions_silver_df.select("transaction_date","timestamp_col","date_col").show()
# transactions_silver_df["date_col"].distinct().count()

# COMMAND ----------

# DBTITLE 1,Optimization Techniques:
# Optimization Techniques:
# Apply partitioning on the transactions_silver table by transaction_date. 
transactions_silver_df.write.partitionBy("transaction_type").mode("overwrite").format("delta").save("/mnt/dataset/transactions_silver_partitioned_delta")
spark.sql("""
    CREATE TABLE silver.transactions_silver
    USING DELTA
    LOCATION '/mnt/dataset/transactions_silver_partitioned_delta'
""")


# COMMAND ----------

# DBTITLE 1,Apply bucketing on the bank_marketing_silver table by customer_id.
# Apply bucketing on the bank_marketing_silver table by customer_id.
customers_silver_df.write.bucketBy(8, "customer_id").mode("overwrite").format("parquet").saveAsTable("silver.customers_silver_bucketed")


# COMMAND ----------

# MAGIC %md
# MAGIC Questions:

# COMMAND ----------

# DBTITLE 1,2.1 Calculate the cumulative transaction amount for each merchant_category ordered by transaction_date available in transactions_silver.
# 2.1 Calculate the cumulative transaction amount for each merchant_category ordered by transaction_date available in transactions_silver. 
spark.sql("""
SELECT merchant_category, transaction_date, SUM(transaction_amount) OVER (PARTITION BY merchant_category ORDER BY transaction_date) AS cumulative_amount
FROM silver.transactions_silver
""").show()


# COMMAND ----------

# DBTITLE 1,2.2 Calculate the total number of transactions for each combination of transaction_status and merchant_category in transactions_silver.
# 2.2 Calculate the total number of transactions for each combination of transaction_status and merchant_category in transactions_silver. 
spark.sql("""
SELECT transaction_status, merchant_category, COUNT(*) AS total_transactions
FROM silver.transactions_silver
GROUP BY transaction_status, merchant_category
""").show()


# COMMAND ----------

# DBTITLE 1,2.3 Calculate Aggregates (Join based on Customer_ID)-
# 2.3 Calculate Aggregates (Join based on Customer_ID)
# 2.3.1 Select top 10 customers who have done maximum transactions per week
spark.sql("""
SELECT customer_id, weekofyear(transaction_date) AS week, COUNT(*) AS transaction_count
FROM silver.transactions_silver
GROUP BY customer_id, week
ORDER BY transaction_count DESC
LIMIT 10
""").show()


# COMMAND ----------

# DBTITLE 1,2.3.2 Calculate the percentage contribution of each segment to the total monthly transactions.
# 2.3.2 Calculate the percentage contribution of each segment to the total monthly transactions.

monthly_transactions = spark.sql("""
SELECT month(transaction_date) AS month, COUNT(*) AS total_transactions
FROM silver.transactions_silver
GROUP BY month
""")

total_transactions = monthly_transactions.groupBy().sum("total_transactions").collect()[0][0]

monthly_transactions.withColumn("percentage_contribution", (monthly_transactions["total_transactions"] / total_transactions) * 100).show()


# COMMAND ----------

# DBTITLE 1,2.3.2 Calculate the percentage contribution of each segment to the total monthly transactions.
# 2.3.2 Calculate the percentage contribution of each segment to the total monthly transactions. 
from pyspark.sql.functions import when,col,month
monthly_spend = spark.sql("""
SELECT customer_id, month(transaction_date) AS month, SUM(transaction_amount) AS monthly_amount
FROM silver.transactions_silver
GROUP BY customer_id, month
""")

monthly_spend.withColumn("spender_type", when(monthly_spend["monthly_amount"] > 5000, "High").otherwise("Low")).show()


# COMMAND ----------

# DBTITLE 1,2.3.3 Segment customers into high and low spenders based on monthly transactions.
# 2.3.3 Segment customers into high and low spenders based on monthly transactions.
monthly_spend = spark.sql("""
SELECT customer_id, month(transaction_date) AS month, SUM(transaction_amount) AS monthly_amount
FROM silver.transactions_silver
GROUP BY customer_id, month
""")

monthly_spend.withColumn("spender_type", when(monthly_spend["monthly_amount"] > 5000, "High").otherwise("Low")).show()


# COMMAND ----------

# DBTITLE 1,2.3.4 Rank job categories based on the total transaction_amount in the joined dataset.   Section C: Gold T
# 2.3.4 Rank job categories based on the total transaction_amount in the joined dataset.

spark.sql("""
SELECT c.job, SUM(t.transaction_amount) AS total_amount
FROM silver.transactions_silver t
JOIN silver.customers_silver c ON t.customer_id = c.customer_id
GROUP BY c.job
ORDER BY total_amount DESC
""").show()



# COMMAND ----------

# MAGIC %md
# MAGIC Section C: Gold Tables and Final Aggregations 
# MAGIC
# MAGIC

# COMMAND ----------

spark.sql("""
    select count(distinct transaction_type),count(distinct merchant_category ) from bronze.transactions_bronze
""").show()


# COMMAND ----------

# DBTITLE 1,Create Fact/Dim Tables on Gold layer:
# Create Fact/Dim Tables on Gold layer: 
# Task:  
# Create Fact and Dimensions tables via Silver tables on Gold Schema and apply partitioning on respective column. 

# Create Fact table
transactions_silver_df.write.partitionBy("transaction_type").mode("overwrite").format("delta").save("/mnt/dataset/transactions_fact_delta")
spark.sql("""
    CREATE TABLE gold.transactions_fact
    USING DELTA
    LOCATION '/mnt/dataset/transactions_fact_delta'
""")

# Create Dimension table
customers_silver_df.write.bucketBy(8, "customer_id").sortBy("customer_id").mode("overwrite").format("parquet").saveAsTable("gold.customers_dim")


# COMMAND ----------

# DBTITLE 1,Create view named Customer_transactions_vw based on Joining Fact/Dimension tables containing functional and attribute columns from both the tables.
# Create view named Customer_transactions_vw based on Joining Fact/Dimension tables containing functional and attribute columns from both the tables. 
customer_transactions_vw = spark.sql("""
SELECT 
    t.*, 
    c.job, 
    c.balance
FROM gold.transactions_fact t
JOIN gold.customers_dim c
ON t.customer_id = c.customer_id
""")
customer_transactions_vw.createOrReplaceTempView("Customer_transactions_vw")


# COMMAND ----------

# MAGIC %md
# MAGIC Question

# COMMAND ----------

# DBTITLE 1,3.1 Calculate the total balance and the number of customers in each job category (Based on View).
#3.1 Calculate the total balance and the number of customers in each job category (Based on View). 
spark.sql("""
SELECT 
    job, 
    SUM(balance) AS total_balance, 
    COUNT(customer_id) AS num_customers
FROM Customer_transactions_vw
GROUP BY job
""").show()


# COMMAND ----------

# DBTITLE 1,3.2 Create Visual Chart for Total transactions done based on Merchant Category, Job_type.
# 3.2 Create Visual Chart for Total transactions done based on Merchant Category, Job_type. 
visual_df = spark.sql("""
SELECT 
    job, 
    merchant_category, 
    COUNT(transaction_id) AS total_transactions
FROM Customer_transactions_vw
GROUP BY job, merchant_category
""")
# Use Databricks visualization tools to create the chart


# COMMAND ----------

# 3.3 Derive logic for -  
# 3.3.1 Considering only the customers who made purchases, what was the average purchase amount per customer. 
spark.sql("""
SELECT 
    AVG(transaction_amount) AS avg_purchase_amount
FROM gold.transactions_fact
WHERE transaction_type = 'purchase'
""").show()


# COMMAND ----------

# DBTITLE 1,3.3.2 Which customer had the highest total transaction amount (sum of purchases minus sum of refunds) on Weekly basis, and what was that amount?
# 3.3.2 Which customer had the highest total transaction amount (sum of purchases minus sum of refunds) on Weekly basis, and what was that amount? 
spark.sql("""
SELECT 
    customer_id, 
    weekofyear(transaction_date) AS week, 
    (SUM(CASE WHEN transaction_type = 'purchase' THEN transaction_amount ELSE 0 END) - 
    SUM(CASE WHEN transaction_type = 'refund' THEN transaction_amount ELSE 0 END)) AS net_amount
FROM gold.transactions_fact
GROUP BY customer_id, week
ORDER BY net_amount DESC
LIMIT 1
""").show()


# COMMAND ----------

# DBTITLE 1,3.4 Build logic for Reconcile the total number of records in the bronze, silver, and gold tables for customer and transactions datasets.
# 3.4 Build logic for Reconcile the total number of records in the bronze, silver, and gold tables for customer and transactions datasets.  
bronze_counts = spark.sql("""
SELECT 'bronze' AS layer, 'transactions' AS table, COUNT(*) AS count FROM bronze.transactions_bronze
UNION ALL
SELECT 'bronze', 'customers', COUNT(*) FROM bronze.customer_bronze
""")

silver_counts = spark.sql("""
SELECT 'silver' AS layer, 'transactions' AS table, COUNT(*) AS count FROM silver.transactions_silver
UNION ALL
SELECT 'silver', 'customers', COUNT(*) FROM silver.customers_silver
""")

gold_counts = spark.sql("""
SELECT 'gold' AS layer, 'transactions' AS table, COUNT(*) AS count FROM gold.transactions_fact
UNION ALL
SELECT 'gold', 'customers', COUNT(*) FROM gold.customers_dim
""")

bronze_counts.union(silver_counts).union(gold_counts).show()


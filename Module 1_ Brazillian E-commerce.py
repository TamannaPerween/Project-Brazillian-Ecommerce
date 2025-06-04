# Databricks notebook source
#mount adbf to adls
spark.conf.set(
    "***",
    "***"
)

# COMMAND ----------

# MAGIC %fs ls abfss://brazellian@tg00ginats24tamperdadls.dfs.core.windows.net/olist_customers_dataset.csv

# COMMAND ----------

base_path="abfss://brazellian@tg00ginats24tamperdadls.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data ingestion and Exploration

# COMMAND ----------

customer_path=base_path+"olist_customers_dataset.csv"
customer_df = spark.read.csv(customer_path,header=True,inferSchema=True)

customer_df.printSchema()
customer_df.show(5);

# COMMAND ----------



# Read all CSV files in the brazellian folder in ADLS
geolocation_path = base_path+"olist_geolocation.csv"
geolocation_df = spark.read.csv(geolocation_path, header=True, inferSchema=True)

geolocation_df.printSchema()
geolocation_df.show(5)

# COMMAND ----------

# Read all CSV files in the brazellian folder in ADLS
order_item_path = base_path+"olist_order_items_dataset.csv"
order_item_df = spark.read.csv(order_item_path, header=True, inferSchema=True)

order_item_df.printSchema()
order_item_df.show(5)

# COMMAND ----------

# Read all CSV files in the brazellian folder in ADLS
order_payment_path = base_path+"olist_order_payments_dataset.csv"
order_payment_df = spark.read.csv(order_payment_path, header=True, inferSchema=True)

order_payment_df.printSchema()
order_payment_df.show(5)

# COMMAND ----------

# Read all CSV files in the brazellian folder in ADLS
order_review_path = base_path+"olist_order_reviews_dataset.csv"
order_review_df = spark.read.csv(order_review_path, header=True, inferSchema=True)

order_review_df.printSchema()
order_review_df.show(5)

# COMMAND ----------

# Read all CSV files in the brazellian folder in ADLS
order_path = base_path+"olist_orders_dataset.csv"
order_df = spark.read.csv(order_path, header=True, inferSchema=True)

order_df.printSchema()
order_df.show(5)

# COMMAND ----------


# Read all CSV files in the brazellian folder in ADLS
product_path = base_path+"olist_products_dataset.csv"
product_df = spark.read.csv(product_path, header=True, inferSchema=True)

product_df.printSchema()
product_df.show(5)

# COMMAND ----------

# Read all CSV files in the brazellian folder in ADLS
seller_path = base_path+"olist_sellers_dataset.csv"
seller_df = spark.read.csv(seller_path, header=True, inferSchema=True)

seller_df.printSchema()
seller_df.show(5)

# COMMAND ----------

# Read all CSV files in the brazellian folder in ADLS
product_category_name_path = base_path+"product_category_name_translation.csv"
product_category_name_df = spark.read.csv(product_category_name_path, header=True, inferSchema=True)

product_category_name_df.printSchema()
product_category_name_df.show(5)

# COMMAND ----------

# DBTITLE 1,data leakage and drop
print(f'geolocation:{geolocation_df.count()} rows')

# COMMAND ----------

from pyspark.sql.functions import col
product_df.select([col(c).isNull().alias(c) for c in product_df.columns]).show()

# COMMAND ----------

from pyspark.sql.functions import col

customer_df.select([col(c).isNull().alias(c) for c in customer_df.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC to check how many null values are present in any column 

# COMMAND ----------

from pyspark.sql.functions import col, count, when

customer_df.select([count(when(col(c).isNull(),1)).alias(c) for c in customer_df.columns]).show()

# COMMAND ----------

# DBTITLE 1,check for duplicates values
customer_df.groupBy('customer_id').count().filter("count>1").show()

# COMMAND ----------

product_df.groupBy('product_id').count().filter('count>1').show()

# COMMAND ----------

display(customer_df)
customer_df.take(5)

# COMMAND ----------

#customer distribution by state
customer_df.groupBy('customer_state').count().orderBy('count',ascending=False).show()

# COMMAND ----------

#order status distribution
order_df.groupBy("order_status").count().orderBy('count',ascending=False).show()

# COMMAND ----------

#order_payment

order_payment_df.groupBy("payment_type").count().orderBy('count',ascending=False).show()

# COMMAND ----------

#top selling Product
order_item_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import sum
top_product= order_item_df.groupBy('product_id').agg(sum('price').alias('total_sales'))
top_product.orderBy('total_sales',ascending=False).show()

# COMMAND ----------

#Average delivery time analysis
display(order_df)

# COMMAND ----------

delivery_df=order_df.select('order_id','order_purchase_timestamp','order_delivered_customer_date')
display(delivery_df)

# COMMAND ----------

from pyspark.sql.functions import datediff, col

delivery_detail_df = delivery_df.withColumn(
    'delivery_time',
    datediff(
        col('order_delivered_customer_date'),
        col('order_purchase_timestamp')
    )
).orderBy('delivery_time', ascending=False)

display(delivery_detail_df)

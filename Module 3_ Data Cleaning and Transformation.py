# Databricks notebook source
#mount adbf to adls
spark.conf.set(
    "fs.azure.account.key.***.dfs.core.windows.net",
    "***"
)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/tamanna.perween@thorogood.com/Module 1: Brazillian E-commerce"

# COMMAND ----------

customer_df.cache()
order_df.cache()
order_item_df.cache()
order_payment_df.cache()
order_review_df.cache()
product_category_name_df.cache()
product_df.cache()
geolocation_df.cache()
seller_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ###join 

# COMMAND ----------

order_item_joined_df = order_df.join(order_item_df,'order_id','inner')

# COMMAND ----------

order_item_product_df= order_item_joined_df.join(product_df,'product_id','inner')

# COMMAND ----------

order_item_product_seller_df= order_item_product_df.join(broadcast(seller_df),'seller_id','inner')

# COMMAND ----------

full_order_df = order_item_product_seller_df.join(customer_df,'customer_id','inner')

# COMMAND ----------

full_order_df = full_order_df.join(geolocation_df,full_order_df.customer_zip_code_prefix == geolocation_df.geolocation_zip_code_prefix,'left')

# COMMAND ----------

full_order_df = full_order_df.join(order_review_df,'order_id','left')

# COMMAND ----------

full_order_df = full_order_df.join(order_payment_df,'order_id','left')

# COMMAND ----------

full_order_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Total Revenue

# COMMAND ----------

#total revenue per seller
from pyspark.sql.functions import *
seller_revenue_df = full_order_df.groupBy('seller_id').agg(sum('price').alias('total_revenue'))
seller_revenue_df.show(5)

# COMMAND ----------

#total order per customer
customer_order_count_df = full_order_df.groupBy('customer_id')\
.agg(count('order_id').alias('total_orders'))\
.orderBy(desc('total_orders'))

customer_order_count_df.show(5)

# COMMAND ----------

#Average Review Score per seller
seller_review_df = full_order_df.groupBy('seller_id')\
.agg(avg('review_score').alias('avg_review_score'))\
.orderBy(desc('avg_review_score'))

display(seller_review_df)

# COMMAND ----------

#top 10 most sold products
top_product_df = full_order_df.groupBy('product_id')\
.agg(count('order_id').alias('total_sold'))\
.orderBy(desc('total_sold'))\
.limit(10)

display(top_product_df)



# COMMAND ----------

# MAGIC %md
# MAGIC ##window function and ranking

# COMMAND ----------

#top 10 customer by spending
#dense rank for sellers based on revenue
from pyspark.sql.window import Window
window_spec = Window.partitionBy('seller_id').orderBy(desc('price'))


# COMMAND ----------

top_seller_product_df = full_order_df.withColumn('rank',rank().over(window_spec)).filter(col('rank')<=5)

top_seller_product_df=top_seller_product_df.select('seller_id',"price",'rank')
display(top_seller_product_df)

# COMMAND ----------

#Total Revenue & Average order value (AOV) per customer
customer_spending_df = full_order_df.groupBy('customer_id')\
.agg(
    count('order_id').alias('total_orders'),
    sum('price').alias('total_spent'),
    round(avg('price'),2).alias('AOV')
)\
.orderBy(desc('total_spent'))

customer_spending_df.show()

# COMMAND ----------

#seller Performance Metrics (Revenue , Average , Order Count)
seller_performance_df = full_order_df.groupBy('seller_id')\
.agg(
    count('order_id').alias('total_orders'),
    sum('price').alias('total_revenue'),
    round(avg('review_score'),2).alias('avg_revenue_score'),
    round(stddev('price'),2).alias('price_variability')
)\
.orderBy(desc('total_revenue'))

# COMMAND ----------

#Product Popularity Metrics

product_metrics_df = full_order_df.groupBy('product_id')\
.agg(
    count('order_id').alias('total_sales'),
    sum('price').alias('total_revenue'),
    round(avg('price'),2).alias('avg_price'),
    round(stddev('price'),2).alias('price_volatility'),
    collect_set('seller_id').alias('unique_sellers')
)\
.orderBy(desc('total_sales'))

product_metrics_df.show()

# COMMAND ----------

#Monthly Revenue and Order Count Trend
total_order
total_revenue
avg_order_value
min_order_value
max_order_value


# COMMAND ----------

#Customer Retention Analysis(First anfd Last Order)

customer_retention_df=full_order_df.groupBy('customer_id')\
.agg(
    first('order_purchase_timestamp').alias('first_order_date'),
    last('order_purchase_timestamp').alias('last_order_date'),
    count('order_id').alias('total_order'),
    round(avg('price'),2).alias("aov")

)\
.orderBy(desc('total_order'))

# COMMAND ----------

customer_retention_df.show()

# COMMAND ----------

 #Order Status Flag
 full_order_df.select('order_status').show()

# COMMAND ----------

full_order_df = full_order_df.withColumn('is_delivered',when(col('order_status')=='delivered',lit(1)).otherwise(lit(0)))\
.withColumn('is_canceled',when(col('order_status')=='canceled'),lit(1).otherwise(lit(0)))

full_order_df.select('order_status','is_delivered','is_canceled').show(10)

# COMMAND ----------

#Order Revenue Calculation
full_order_df = full_outer_df.withColumn('order_revenue',col('price')+ col('freight_value'))

# COMMAND ----------

full_order_df.select('price','freight_value','order_revenue').show()

# COMMAND ----------

#customer Segmentation based on spending
customer_spending_df = customer_spending_df.withColumn('customer_segment',when(col('AOV')>= 1200,"High-Value"))
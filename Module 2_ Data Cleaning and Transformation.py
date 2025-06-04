# Databricks notebook source
#mount adbf to adls
spark.conf.set(
    "fs.azure.account.key.***.dfs.core.windows.net",
    "***"
)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/tamanna.perween@thorogood.com/Module 1: Brazillian E-commerce"

# COMMAND ----------

#identify missing value
from pyspark.sql.functions import col,when,count
customer_df.select([count(when(col(c).isNull(),1)).alias(c) for c in customer_df.columns]).show()

# COMMAND ----------

from pyspark.sql.functions import *
def missing_values(df,df_name):
    print(f'Missing values in {df_name}:')
    df.select([count(when(col(c).isNull(),1)).alias(c) for c in df.columns]).show()

# COMMAND ----------

missing_values(customer_df,'customer')

# COMMAND ----------

missing_values(order_df,'order')
missing_values(order_item_df,'order_item')
missing_values(order_payment_df,'order_payment')
missing_values(order_review_df,'order_review')
missing_values(product_df,'product')
missing_values(product_category_name_df,'product_category_name')

# COMMAND ----------

# DBTITLE 1,Handle missing Value
#Handle missing Value
#1 Drop missing values(for non-critical columns)
#2 fill missing values(for numerical columns)
#3 Input missing value (for continuous data)


# COMMAND ----------

order_df_cleaned= order_df.na.drop(subset=['order_id','customer_id','order_status'])
display(order_df_cleaned)

# COMMAND ----------

# DBTITLE 1,use fillna()
order_df_cleaned=order_df.fillna({'order_delivered_customer_date':'2025-05-13'})
display(order_df_cleaned)

# COMMAND ----------

# MAGIC %md
# MAGIC ##mai imputer use nhi kr rhi hu

# COMMAND ----------

# MAGIC %md
# MAGIC ###standarizing the formats

# COMMAND ----------

def print_schema(df,df_name):
  print(f"schema of {df_name}:")
  df.printSchema()

# COMMAND ----------

print_schema(order_df,'order')

# COMMAND ----------

display(order_payment_df)

# COMMAND ----------

order_payment_df.groupBy('payment_type').count().orderBy("count",ascending=False).show()

# COMMAND ----------

# DBTITLE 1,clean order payment
payment_df_cleaned= order_payment_df.withColumn('payment_type',when(col('payment_type')=='boleto','Bank Transfer')
.when(col('payment_type')=='credit_card','Credit Card')
.when(col('payment_type')=='debit_card','Debit Card')
.otherwise('other'))
display(payment_df_cleaned)

# COMMAND ----------

# DBTITLE 1,convert datatype in customer table
customer_df_cleaned= customer_df.withColumn('customer_zip_code_prefix',col('customer_zip_code_prefix').cast('string'))

customer_df_cleaned.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #Remove duplicate Records

# COMMAND ----------

customer_df_cleaned = customer_df_cleaned.dropDuplicates(['customer_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ###join with order, order_item, order_payment,customer table

# COMMAND ----------

order_with_detail= order_df_cleaned.join(order_item_df,'order_id','left')\
.join(payment_df_cleaned,'order_id','left')\
.join(customer_df_cleaned,'customer_id','left')

display(order_with_detail)


# COMMAND ----------

order_with_total_values= order_with_detail.groupBy('order_id').agg(sum('payment_value').alias('total_order_value'))

order_with_total_values.show(5)

# COMMAND ----------

#advance transformation

quantities = order_item_df.approxQuantile('price',[0.01,0.99],0.0)
low_cutoff,high_cutoff=quantities[0],quantities[1]

# COMMAND ----------

order_item_df.select('price').summary().show()

# COMMAND ----------

low_cutoff,high_cutoff

# COMMAND ----------

# MAGIC %md
# MAGIC yha mai kuch cod rhi hu 55:00 second se

# COMMAND ----------

# MAGIC %fs ls 'abfss://brazellian@tg00ginats24tamperdadls.dfs.core.windows.net/'

# COMMAND ----------


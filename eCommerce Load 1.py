# Databricks notebook source
#initally loading all the 9 parquet files
#then converting to delta files to be stored in Azure datalake
#dbutils.fs.ls("/mnt/aamina_mount/raw/ecommerce_parquet")
#dbutils.fs.ls("/mnt/aamina_mount/raw")
#ecommerce_parquet/olist_customers_dataset.parquet

df_customers = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_customers_dataset.parquet")
display(df_customers.head(10))

df_geolocation = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_geolocation_dataset.parquet")
display(df_geolocation.head(10))

df_order_items = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_order_items_dataset.parquet")
display(df_order_items.head(10))

df_order_reviews = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_order_reviews_dataset.parquet")
display(df_order_reviews.head(10))

df_orders = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_orders_dataset.parquet")
display(df_orders.head(10))


df_order_payments = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_order_payments_dataset.parquet")
display(df_order_payments.head(10))

df_products = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_products_dataset.parquet")
display(df_products)
display(df_products.count())

df_sellers = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_sellers_dataset.parquet")
display(df_sellers.head(10))

df_prod_cat_name = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/product_category_name_translation.parquet")
display(df_prod_cat_name.head(10))



# COMMAND ----------

#first run join to replace all the spanish names with english names for df_products

#want to switch prod_cat name from spanish ot english using the translation table
#joining prod_cat_name with products table
df_products_translation = spark.sql("select pr.product_id, prodcat.product_category_name_english, pr.product_name_lenght, pr.product_description_lenght, pr.product_photos_qty, pr.product_weight_g, pr.product_length_cm, pr.product_height_cm, pr.product_width_cm from {prod} pr left join {prod_cat} prodcat on pr.product_category_name = prodcat.product_category_name", prod = df_products, prod_cat = df_prod_cat_name)
display(df_products_translation)
display(df_products_translation.count())
display(spark.sql("select * from {prodtrans} where product_category_name_english = 'perfumery'", prodtrans = df_products_translation))

#df_products_translation = spark.sql("select pr.product_id, prod.product_category_name_english, pr.product_name_lenght,pr.product_description_lenght, pr.product_photos_qty, pr.product_weight_g, pr.product_length_cm, pr.product_height_cm, pr.product_width_cm from {prod} pr join {prod_cat} prod on pr.product_category_name = prod.product_category_name_english", prod = df_products, prod_cat = df_prod_cat_name)

#$test this line over here to make sure perfume category is being displayed
#display(spark.sql("select * from {prod}", prod = df_products_translation))
#display(df_products_translation)

display(
    spark.sql(
        "select * from {prod} pr where pr.product_category_name_english = 'perfumery'",
        prod=df_products_translation
    )
)

# COMMAND ----------

display(df_customers)
display(df_customers.count())

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

#dim payments for my data model
df_order_payments.createOrReplaceTempView("payments")

display(spark.sql(
"""
select * 
from payments
where order_id = "e481f51cbdc54678b7cc49136f2d6af7"
"""
))

display(spark.sql(
"""
select * 
from payments
where order_id = "69923a4e07ce446644394df37a710286"
"""
))

dim_payments = spark.sql(
    """
    select distinct p.payment_type 
    from payments p
    """
)
dim_payments = dim_payments.withColumn("type_id", monotonically_increasing_id()+1)
display(dim_payments)


# COMMAND ----------

# creating customer dim table
#command shift f for format
dim_customer = spark.sql(
    "select distinct c.customer_id, c.customer_unique_id, c.customer_zip_code_prefix, c.customer_city, c.customer_state from {cust} as c", cust = df_customers
    )
display(dim_customer.head(10))
display(dim_customer.count())
dim_customer1 = spark.sql(
    "select distinct c.customer_unique_id, c.customer_zip_code_prefix from {cust} as c", cust = df_customers
    )
display(dim_customer1.count())

# COMMAND ----------

display(df_geolocation.head(10))
print(df_geolocation.columns)
display(df_geolocation.count())

# COMMAND ----------

#geolocation dim table
#Note: this did not make it into the final data model --> dim_customer1 included the prefix codes, city, and states
#reason being is becuase of cross join issues (when joining to fact table) and denormalizing data (many repeated valued)
df_geolocation.createOrReplaceTempView("geolocation")
dim_geolocation_customer = spark.sql(
    """
    select distinct gc.geolocation_zip_code_prefix, gc.geolocation_city, gc.geolocation_state 
    from geolocation gc
    """
)
display(dim_geolocation_customer)
# display(dim_geolocation_customer)
display(dim_geolocation_customer.count())
# come back --> if I use distinct, there will be unique zip_code_prefixes, however, all the latitudes and longitudes will be removed --> each user lives in a different location (diff lat and long) in the same city and state --> these unique lat and long will be removed if I use distinct

# COMMAND ----------

df_orders.createOrReplaceTempView("orders")
display(
    spark.sql(
        """
        select * from orders
        where order_id = "e481f51cbdc54678b7cc49136f2d6af7"
        """
    )
)

display(
    spark.sql(
        """
        select * from orders
        where customer_id = "31f31efcb333fcbad2b1371c8cf0fa84"
        """
    )
)


# COMMAND ----------

#data exploration to understand how tables are connected
#following an order_id and checking if unique or not
df_customers.createOrReplaceTempView("customers")
display(
    spark.sql(
        """
        select * from customers
        where customer_id = "9ef432eb6251297304e76186b10a928d"
        """
    )
)

display(
    spark.sql(
        """
        select * from customers
        where customer_unique_id = "7c396fd4830fd04220f754e42b4e5bff"
        """
    )
)

display(
    spark.sql(
        """
        select * from customers
        where customer_unique_id = "7c396fd4830fd04220f754e42b4e5bff"
        """
    )
)

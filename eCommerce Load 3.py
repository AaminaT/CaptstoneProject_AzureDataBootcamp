# Databricks notebook source
#initally loading all the 9 parquet files
#then converting to delta files to be stored in Azure datalake
#dbutils.fs.ls("/mnt/aamina_mount/raw/ecommerce_parquet")
#dbutils.fs.ls("/mnt/aamina_mount/raw")
#ecommerce_parquet/olist_customers_dataset.parquet

df_customers = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_customers_dataset.parquet")
df_geolocation = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_geolocation_dataset.parquet")
df_order_items = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_order_items_dataset.parquet")
df_order_reviews = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_order_reviews_dataset.parquet")
df_orders = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_orders_dataset.parquet")
df_order_payments = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_order_payments_dataset.parquet")
df_products = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_products_dataset.parquet")
df_sellers = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/olist_sellers_dataset.parquet")
df_prod_cat_name = spark.read.format("parquet").load("/mnt/aamina_mount/raw/ecommerce_parquet/product_category_name_translation.parquet")

df_order_reviews.createOrReplaceTempView("reviews")

display(spark.sql(
    """
    select * from reviews
    """
))
display(df_order_reviews.count())

df_distinct_reviews = spark.sql(
    """
    select *, count(*) c
    from reviews
    group by review_id, order_id, review_score, review_creation_date, review_answer_timestamp
    having c > 1
    """
)
display(df_distinct_reviews)
display(df_distinct_reviews.count())
#78cd965d0bc0388d390404eee6490c5b
df_distinct_reviews.createOrReplaceTempView("revs")
# display(spark.sql(
#     """
#     select order_id from revs
#     where order_id = "78cd965d0bc0388d390404eee6490c5b"
#     """
# ))
# display(spark.sql(
#     """
#     select review_id, count(*) c
#     from revs
#     group by review_id
#     having c > 1
#     """
# ))
# display(spark.sql(
#     """
#     SELECT order_id, COUNT(DISTINCT review_id) as c
#     FROM revs
#     GROUP BY order_id
#     HAVING c > 1
#     """
# ))
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

df_order_payments.createOrReplaceTempView("payments")

dim_payments = spark.sql(
    """
    select distinct p.payment_type 
    from payments p
    """
)
dim_payments = dim_payments.withColumn("type_id", monotonically_increasing_id()+1)
dim_customer = spark.sql(
    "select distinct c.customer_id, c.customer_unique_id, c.customer_zip_code_prefix, c.customer_city, c.customer_state from {cust} as c", cust = df_customers
    )
dim_customer1 = spark.sql(
    "select distinct c.customer_unique_id, c.customer_zip_code_prefix from {cust} as c", cust = df_customers
    )
df_geolocation.createOrReplaceTempView("geolocation")
dim_geolocation_customer = spark.sql(
    """
    select distinct gc.geolocation_zip_code_prefix, gc.geolocation_city, gc.geolocation_state 
    from geolocation gc
    """
)

# creating dim calendar table
# need from 2016 to 2019
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, expr
# sequence, to_date

beginDate = '2016-01-01'
endDate = '2023-12-31'

df_test_cal = spark.sql(f"SELECT explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate").createOrReplaceTempView('dates')

#df_test_cal.createOrReplaceTempView("date")

spark.sql("SELECT * FROM dates").show()

# date_id, day_name, day_of_month, month (int), month_name, quarter (int), more quarter details, year int


# COMMAND ----------

# MAGIC %scala
# MAGIC // writing the code in scala based on data found from medium.com
# MAGIC case class dim_date_schema(
# MAGIC                        date_key: Int,
# MAGIC                        date: String,
# MAGIC                        day: Int,
# MAGIC                        day_suffix: String,
# MAGIC                        week_day: Int,
# MAGIC                        week_day_name: String,
# MAGIC                        week_day_name_short: String,
# MAGIC                        week_day_name_first_letter: String,
# MAGIC                        day_of_year: Int,
# MAGIC                        week_of_month: Int,
# MAGIC                        week_of_year: Int,
# MAGIC                        month: Int,
# MAGIC                        month_name: String,
# MAGIC                        month_name_short: String,
# MAGIC                        month_name_first_letter: String,
# MAGIC                        quarter: Int,
# MAGIC                        quarter_name: String,
# MAGIC                        year: Int,
# MAGIC                        yyyymm: String,
# MAGIC                        month_year: String,
# MAGIC                        is_weekend: Int,
# MAGIC                        is_holiday: Int,
# MAGIC                        first_date_of_year: String,
# MAGIC                        last_date_of_year: String,
# MAGIC                        first_date_of_quarter: String,
# MAGIC                        last_date_of_quarter: String,
# MAGIC                        first_date_of_month: String,
# MAGIC                        last_date_of_month: String,
# MAGIC                        first_date_of_week: String,
# MAGIC                        last_date_of_week: String,
# MAGIC                        last_12_month_flag: Int,
# MAGIC                        last_6_month_flag: Int,
# MAGIC                        last_month_flag: Int
# MAGIC                      )
# MAGIC import java.time.LocalDate
# MAGIC import java.time.format.DateTimeFormatter
# MAGIC import scala.collection.mutable.ListBuffer
# MAGIC
# MAGIC object date_time_utils {
# MAGIC   val default_format = "yyyy-MM-dd"
# MAGIC
# MAGIC   def check(start_date: String, end_date: String): Boolean = {
# MAGIC     val start = LocalDate.parse(start_date, DateTimeFormatter.ofPattern(default_format))
# MAGIC     val end = LocalDate.parse(end_date, DateTimeFormatter.ofPattern(default_format))
# MAGIC     end.isAfter(start)
# MAGIC   }
# MAGIC
# MAGIC
# MAGIC   def convert_string_to_date(current_date: String, input_format: String, output_format: String) = {
# MAGIC     val input_formatter = DateTimeFormatter.ofPattern(input_format)
# MAGIC     val output_formatter = DateTimeFormatter.ofPattern(output_format)
# MAGIC     output_formatter.format(input_formatter.parse(current_date))
# MAGIC   }
# MAGIC
# MAGIC   def get_day_suffix(current_date: String) = {
# MAGIC     val day = convert_string_to_date(current_date, default_format, "d").toInt
# MAGIC     day match {
# MAGIC       case 1 => "st"
# MAGIC       case 21 => "st"
# MAGIC       case 31 => "st"
# MAGIC       case 2 => "nd"
# MAGIC       case 22 => "nd"
# MAGIC       case 3 => "rd"
# MAGIC       case 23 => "rd"
# MAGIC       case _ => "th"
# MAGIC     }
# MAGIC   }
# MAGIC
# MAGIC   def get_quater_name(current_date: String) = {
# MAGIC     val quater = convert_string_to_date(current_date, default_format, "Q").toInt
# MAGIC     quater match {
# MAGIC       case 1 => "Q1"
# MAGIC       case 2 => "Q2"
# MAGIC       case 3 => "Q3"
# MAGIC       case 4 => "Q4"
# MAGIC     }
# MAGIC   }
# MAGIC
# MAGIC   def is_weekend(current_date: String) = {
# MAGIC     val week_day_name = convert_string_to_date(current_date, default_format, "EEEE")
# MAGIC     week_day_name match {
# MAGIC       case "Saturday" => 1
# MAGIC       case "Sunday" => 1
# MAGIC       case _ => 0
# MAGIC     }
# MAGIC   }
# MAGIC
# MAGIC   def get_date_of_year(current_date: String, position: String) = {
# MAGIC     val year = convert_string_to_date(current_date, default_format, "u")
# MAGIC     position match {
# MAGIC       case "first" => year + "-01-01"
# MAGIC       case "last" => year + "-12-31"
# MAGIC     }
# MAGIC   }
# MAGIC
# MAGIC   def get_first_date_of_quarter(current_date: String) = {
# MAGIC     val quater = convert_string_to_date(current_date, default_format, "QQ")
# MAGIC     val year = convert_string_to_date(current_date, default_format, "u")
# MAGIC     quater match {
# MAGIC       case "01" => year + "-01-01"
# MAGIC       case "02" => year + "-04-01"
# MAGIC       case "03" => year + "-07-01"
# MAGIC       case "04" => year + "-10-01"
# MAGIC     }
# MAGIC   }
# MAGIC
# MAGIC   def get_last_date_of_quarter(current_date: String) = {
# MAGIC     val quater = convert_string_to_date(current_date, default_format, "QQ")
# MAGIC     val year = convert_string_to_date(current_date, default_format, "u")
# MAGIC     quater match {
# MAGIC       case "01" => year + "-03-31"
# MAGIC       case "02" => year + "-06-30"
# MAGIC       case "03" => year + "-09-30"
# MAGIC       case "04" => year + "-12-31"
# MAGIC     }
# MAGIC   }
# MAGIC
# MAGIC   def get_first_date_of_month(current_date: String) = {
# MAGIC     val month = convert_string_to_date(current_date, default_format, "MM")
# MAGIC     val year = convert_string_to_date(current_date, default_format, "u")
# MAGIC     year + "-" + month + "-01"
# MAGIC   }
# MAGIC
# MAGIC   def get_last_date_of_month(current_date: String) = {
# MAGIC     val converted_date = LocalDate.parse(current_date, DateTimeFormatter.ofPattern(default_format))
# MAGIC     val last_day_of_month = converted_date.withDayOfMonth(converted_date.getMonth.length(converted_date.isLeapYear))
# MAGIC     last_day_of_month.toString
# MAGIC   }
# MAGIC
# MAGIC   def get_first_date_of_week(current_date: String) = {
# MAGIC     val converted_date = LocalDate.parse(current_date, DateTimeFormatter.ofPattern(default_format))
# MAGIC     val day_backward = convert_string_to_date(current_date, default_format, "e").toInt - 1
# MAGIC     converted_date.minusDays(day_backward).toString
# MAGIC   }
# MAGIC
# MAGIC   def get_last_date_of_week(current_date: String) = {
# MAGIC     val converted_date = LocalDate.parse(current_date, DateTimeFormatter.ofPattern(default_format))
# MAGIC     val day_forward = 7 - convert_string_to_date(current_date, default_format, "e").toInt
# MAGIC     converted_date.plusDays(day_forward).toString
# MAGIC   }
# MAGIC   
# MAGIC   def get_last_12_month_list() = {
# MAGIC     var last_12_month_list = ListBuffer[String]()
# MAGIC     var i = 0
# MAGIC     for( i <- 1 to 12){
# MAGIC       last_12_month_list += DateTimeFormatter.ofPattern("yyyyMM").format(LocalDate.now.minusMonths(i))
# MAGIC     }
# MAGIC     last_12_month_list
# MAGIC   }
# MAGIC   
# MAGIC   
# MAGIC   def get_last_12_month_flag(yyyyMM: String) = {
# MAGIC     if (get_last_12_month_list().contains(yyyyMM)) 1 else 0
# MAGIC   }
# MAGIC   
# MAGIC   def get_last_6_month_flag(yyyyMM: String) = {    
# MAGIC     if (get_last_12_month_list().slice(0,6).contains(yyyyMM)) 1 else 0
# MAGIC   }
# MAGIC   
# MAGIC   def get_last_month_flag(yyyyMM: String) = {
# MAGIC     if (get_last_12_month_list()(0).equals(yyyyMM)) 1 else 0
# MAGIC   }
# MAGIC   
# MAGIC   def get_calendar_end_date(plus_month:Int) = {
# MAGIC     get_last_date_of_month(DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDate.now.plusMonths(plus_month)))
# MAGIC   }
# MAGIC }
# MAGIC // dim date generator
# MAGIC def dim_date_generator(current_date: String): dim_date_schema ={
# MAGIC   val default_format = "yyyy-MM-dd"
# MAGIC   val date_key = date_time_utils.convert_string_to_date(current_date, default_format, "yyyyMMdd").toInt
# MAGIC   val date = current_date
# MAGIC   val day = date_time_utils.convert_string_to_date(current_date, default_format, "d").toInt
# MAGIC   val day_suffix = date_time_utils.get_day_suffix(current_date)
# MAGIC   val week_day = date_time_utils.convert_string_to_date(current_date, default_format, "e").toInt
# MAGIC   val week_day_name = date_time_utils.convert_string_to_date(current_date, default_format, "EEEE")
# MAGIC   val week_day_name_short = date_time_utils.convert_string_to_date(current_date, default_format, "E").toUpperCase
# MAGIC   val week_day_name_first_letter = date_time_utils.convert_string_to_date(current_date, default_format, "E").substring(0, 1)
# MAGIC   val day_of_year = date_time_utils.convert_string_to_date(current_date, default_format, "D").toInt
# MAGIC   val week_of_month = date_time_utils.convert_string_to_date(current_date, default_format, "W").toInt
# MAGIC   val week_of_year = date_time_utils.convert_string_to_date(current_date, default_format, "w").toInt
# MAGIC   val month = date_time_utils.convert_string_to_date(current_date, default_format, "M").toInt
# MAGIC   val month_name = date_time_utils.convert_string_to_date(current_date, default_format, "MMMM")
# MAGIC   val month_name_short = date_time_utils.convert_string_to_date(current_date, default_format, "MMM").toUpperCase
# MAGIC   val month_name_first_letter = date_time_utils.convert_string_to_date(current_date, default_format, "MMM").substring(0, 1)
# MAGIC   val quarter = date_time_utils.convert_string_to_date(current_date, default_format, "Q").toInt
# MAGIC   val quarter_name = date_time_utils.get_quater_name(current_date)
# MAGIC   val year = date_time_utils.convert_string_to_date(current_date, default_format, "u").toInt
# MAGIC   val yyyyMM = date_time_utils.convert_string_to_date(current_date, default_format, "yyyyMM")
# MAGIC   val month_year = date_time_utils.convert_string_to_date(current_date, default_format, "yyyy MMM").toUpperCase
# MAGIC   val is_weekend = date_time_utils.is_weekend(current_date)
# MAGIC   val is_holiday = 0
# MAGIC   val first_date_of_year = date_time_utils.get_date_of_year(current_date, "first")
# MAGIC   val last_date_of_year = date_time_utils.get_date_of_year(current_date, "last")
# MAGIC   val first_date_of_quarter = date_time_utils.get_first_date_of_quarter(current_date)
# MAGIC   val last_date_of_quarter = date_time_utils.get_last_date_of_quarter(current_date)
# MAGIC   val first_date_of_month = date_time_utils.get_first_date_of_month(current_date)
# MAGIC   val last_date_of_month = date_time_utils.get_last_date_of_month(current_date)
# MAGIC   val first_date_of_week = date_time_utils.get_first_date_of_week(current_date)
# MAGIC   val last_date_of_week = date_time_utils.get_last_date_of_week(current_date)
# MAGIC   val last_12_month_flag = date_time_utils.get_last_12_month_flag(yyyyMM)
# MAGIC   val last_6_month_flag = date_time_utils.get_last_6_month_flag(yyyyMM)
# MAGIC   val last_month_flag = date_time_utils.get_last_month_flag(yyyyMM)
# MAGIC   dim_date_schema(date_key, date, day, day_suffix, week_day, week_day_name, week_day_name_short, week_day_name_first_letter,
# MAGIC     day_of_year, week_of_month, week_of_year, month, month_name, month_name_short, month_name_first_letter, quarter,
# MAGIC     quarter_name, year, yyyyMM, month_year, is_weekend, is_holiday, first_date_of_year, last_date_of_year, first_date_of_quarter,
# MAGIC     last_date_of_quarter, first_date_of_month, last_date_of_month, first_date_of_week, last_date_of_week, last_12_month_flag, last_6_month_flag, last_month_flag)
# MAGIC }
# MAGIC // starting with dimension start date “2016–01–01” till current year + 5 years
# MAGIC
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC
# MAGIC def create_final_df(): DataFrame = {
# MAGIC
# MAGIC   //Start Date
# MAGIC   var start_date = "2016-01-01"
# MAGIC
# MAGIC   //End Date
# MAGIC   val end_date = date_time_utils.get_calendar_end_date(60)
# MAGIC
# MAGIC   //Mutable list to store dim date
# MAGIC   var dim_date_mutable_list = new ListBuffer[dim_date_schema]()
# MAGIC
# MAGIC   while (date_time_utils.check(start_date, end_date)) {
# MAGIC     val dim_date_schema_object = dim_date_generator(start_date)
# MAGIC     dim_date_mutable_list += dim_date_schema_object
# MAGIC     start_date = LocalDate.parse(start_date, DateTimeFormatter.ofPattern("yyyy-MM-dd")).plusDays(1).toString
# MAGIC   }
# MAGIC
# MAGIC   val dim_date_list = dim_date_mutable_list.toList
# MAGIC   val dim_date_df = spark.createDataset(dim_date_list)
# MAGIC   dim_date_df.select(col("date_key")
# MAGIC     , col("date").cast("date")
# MAGIC     , col("day")
# MAGIC     , col("day_suffix")
# MAGIC     , col("week_day")
# MAGIC     , col("week_day_name")
# MAGIC     , col("week_day_name_short")
# MAGIC     , col("week_day_name_first_letter")
# MAGIC     , col("day_of_year")
# MAGIC     , col("week_of_month")
# MAGIC     , col("week_of_year")
# MAGIC     , col("month")
# MAGIC     , col("month_name")
# MAGIC     , col("month_name_short")
# MAGIC     , col("month_name_first_letter")
# MAGIC     , col("quarter")
# MAGIC     , col("quarter_name")
# MAGIC     , col("year")
# MAGIC     , col("yyyymm")
# MAGIC     , col("month_year")
# MAGIC     , col("is_weekend")
# MAGIC     , col("is_holiday")
# MAGIC     , col("first_date_of_year").cast("date")
# MAGIC     , col("last_date_of_year").cast("date")
# MAGIC     , col("first_date_of_quarter").cast("date")
# MAGIC     , col("last_date_of_quarter").cast("date")
# MAGIC     , col("first_date_of_month").cast("date")
# MAGIC     , col("last_date_of_month").cast("date")
# MAGIC     , col("first_date_of_week").cast("date")
# MAGIC     , col("last_date_of_week").cast("date")
# MAGIC     , col("last_12_month_flag")
# MAGIC     , col("last_6_month_flag")
# MAGIC     , col("last_month_flag")
# MAGIC     , current_timestamp().as("load_date"))
# MAGIC }
# MAGIC
# MAGIC val finalDF = create_final_df()
# MAGIC finalDF.createOrReplaceTempView("calendar")

# COMMAND ----------

# spark.sql("SELECT * FROM my_table").show()
dim_calendar = spark.sql("SELECT * FROM calendar").toPandas()
dim_calendar.tail(10)

# COMMAND ----------

# fact table
# current dim tables
# dim customer and dim location customer
from pyspark.sql import SparkSession
#view_name = "customer"
#spark.sql(f"DROP VIEW IF EXISTS {view_name}")
df_order_payments.createOrReplaceTempView("order_payments")
dim_payments.createOrReplaceTempView("payment_type")
df_order_payments.createOrReplaceTempView("reviews")
#display(dim_payments)
# display(df_orders)
#display(dim_customer)
#display(df_orders.count())
#display(dim_customer.count())
# display(dim_calendar)
#display(dim_geolocation_customer)
df_orders.createOrReplaceTempView("orders")
dim_customer.createOrReplaceTempView("customers")
dim_geolocation_customer.createOrReplaceTempView("geolocation")
#dim_calendar.createOrReplaceTempView("calendar")
dim_calendar_spark = spark.createDataFrame(dim_calendar)
dim_calendar_spark.createOrReplaceTempView("calendar")
# display(dim_calendar_spark)

#the payment table will be the fact table which we will be adding our dimension tables into
#will require a temp df to match ordeds by customer_id, but customer_id will not be included
df_temp = spark.sql(
    """
    select o.order_id, o.order_status, o.order_purchase_timestamp, o.order_approved_at, o.order_delivered_carrier_date, o.order_delivered_customer_date, o.order_estimated_delivery_date, c.customer_id, c.customer_unique_id, c.customer_zip_code_prefix, c.customer_city, c.customer_state
    from orders o
    join customers c on o.customer_id = c.customer_id
    """
)
df_temp.createOrReplaceTempView("order_customer")
display(df_temp)
display(df_temp.count())

df_fact = spark.sql(
    """
    select oc.*, p.payment_sequential, pt.type_id, p.payment_installments, p.payment_value
    -- r.review_id, r.review_score, r.review_creation_date, r.review_answer_timestamp
    from order_payments p
    join payment_type pt on p.payment_type = pt.payment_type
    join order_customer oc on p.order_id = oc.order_id  
    """
)

display(df_fact)
display(df_fact.count())
# df_fact = spark.sql(
# """
#     Select o.*, c.customer_unique_id, c.customer_zip_code_prefix, g.geolocation_lat, g.geolocation_lng, g.geolocation_city, g.geolocation_state
#     from orders o
#     join customer c on c.customer_id = o.customer_id
#     join geolocation g on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
    

# """
# )
# display(df_fact.count())
# display(df_fact)
dim_customer1.createOrReplaceTempView("customer1")
display(spark.sql(
    """
    select customer_unique_id, count(*) c
    from customer1
    group by customer_unique_id
    having c>1
    """
))

display(
    spark.sql(
        """
        select *
        from customer1
        where customer_unique_id = "c45ece361aab055ea6c55b61eb2d99c0"
        """
    )
)

dim_calendar1 = spark.sql(
    """
        select * 
        from calendar
        where  date_key >= 20160101  and  date_key <= 20191231              
    """
     )
# display(dim_calendar1)
# display(dim_calendar1.count())

# display(df_fact)
# display(dim_customer1)
# display(dim_payments)
# display(dim_calendar1)

# COMMAND ----------

#writing delta tables
#df_payments = spark.read.format("delta").load("/mnt/aamina_mount/datamart/dimpayments")
dim_payments.write.format("delta").mode("overwrite").save("/mnt/aamina_mount/datamart/dimpayments1")
dim_customer1.write.format("delta").mode("overwrite").save("/mnt/aamina_mount/datamart/dimcustomer1")
dim_calendar1.write.format("delta").mode("overwrite").save("/mnt/aamina_mount/datamart/dimcalendar2")
df_fact.write.format("delta").mode("overwrite").save("/mnt/aamina_mount/datamart/factolist1")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create database olistbi1
# MAGIC USE DATABASE olistbi1;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW vw_dim_payments
# MAGIC AS
# MAGIC SELECT * FROM delta.`/mnt/aamina_mount/datamart/dimpayments1`;
# MAGIC
# MAGIC Create or replace view vw_dim_customer1
# MAGIC AS
# MAGIC Select * from delta.`/mnt/aamina_mount/datamart/dimcustomer1`;
# MAGIC
# MAGIC create or replace view vw_dim_calendar2
# MAGIC As
# MAGIC select * from delta. `/mnt/aamina_mount/datamart/dimcalendar2`;
# MAGIC
# MAGIC create or replace view vw_fact_olist
# MAGIC as
# MAGIC select * from delta. `/mnt/aamina_mount/datamart/factolist`;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_dim_payments  limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_dim_customer1 limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_dim_calendar2 limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_fact_olist limit 5;

# COMMAND ----------


# df_new_reviews = spark.sql(
#     """
#     select r.*, oc.*
#     from reviews r
#     join order_customer oc on oc.order_id = r.order_id
#     """
# )
# display(df_new_reviews)

#save as delta format
#moving all the dimension and fact tables into the datamart folder
#work on creating fact table - all numerical values
#col names
#each order assigned to customer_id
# df_customers:  customer_id, customer_unique_id
# work on creating dimension table
#and other reporting tables (for fun)


#session 8 1/11
#streaming data is an abundant area --> costly because 24/7 processing
# will be focusing on batch processing at the moment
#will be focusing on incremental data
#historical load -how many months or weeks
#streaming real-time reporting needed
#real time --> prime-time --> 7 PM , 4-7 PM, business needs
#pull data from the source and run from datamart
#IoTs, --> sending constant alerts, more connected devices are creating data that needs to be processed
#incremental or delta )means you are bringing the new data or latest data to your system


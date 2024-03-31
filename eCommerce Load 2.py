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

# testing reviews table
display(df_order_reviews)
display(df_order_reviews.count())
review_test = spark.sql(
    "select distinct rev.review_id from {reviews} as rev", reviews=df_order_reviews
)
display(review_test.count())
# I can see that there are multiple review_id

# find all the nonunique review_id and pair with order_id and display
# needs to be a nested query
# df_order_reviews as a temporary view
#create or replace view df_order_reviews
df_order_reviews.createOrReplaceTempView("order_reviews")

#need to create temporary view in order to accessorder review table in the code below 
display(
    spark.sql(
    """
    select review_id
    from (
        select review_id, count(*) as count from order_reviews 
        group by review_id
        )
        where count>1
    """

)
)
# very insteresting....
display(
    spark.sql(
        """
        select * 
        from order_reviews
        where (review_id) in (select review_id 
        from(
            select review_id, count(*) as count from order_reviews
            group by review_id
        )
        where count>1
        )
        """
    )
)

identical_review_id = spark.sql(
    """
    select review_id, order_id from 
    (select review_id, order_id, count(*) as count from order_reviews group by review_id, order_id) as counts
    where counts.count>1 
    """
)
display(identical_review_id)

# COMMAND ----------

#checking for duplicate review_ids
display(
    spark.sql(
        """
        select review_id, count(*) as count
        from order_reviews
        group by review_id
        having count >1
        """
    )
)
#return an entire record for each duplicate
# select the entire table and join that to our duplicate rows
#need to save the below code in its own table to further data exploration
df_reveiw_id_duplicates = spark.sql(
        """
        select r_orig.* from
        order_reviews as r_orig
        join(
            select review_id, count(*) as count
            from order_reviews
            group by review_id
            having count >1) as dupl
        on r_orig.review_id = dupl.review_id
        order by review_id
        """
    )
display(df_reveiw_id_duplicates)


#need to map with df_customers to find the issue of the review_id

#ok so there are duplicate review_id that have the same score --> but they map to different order_id --> which is interstesting because everything is the same except for the order_id
#should look into the order_id values in the other tables

# display(
#     spark.sql(
#         """
#         select *
#         from order_reviews
#         where (review_id) in 
#         (select review_id, count(*) as count
#         from order_reviews
#         group by review_id
#         having count >1)
#         """
#     )
# )

# COMMAND ----------

# display(
#     spark.sql(
#         """
#         select rev.order_id, rev.review_id, count(*) as count
#         from order_reviews as rev
#         group by rev.order_id, rev.review_id
#         having count >1
#         """
#     )
# )

# COMMAND ----------

#need to map with df_customers to find the issue of the review_id
#need to use the df_orders not customers
df_customers.createOrReplaceTempView("customers")
df_orders.createOrReplaceTempView("orders")
df_reveiw_id_duplicates.createOrReplaceTempView("duplicates")
display(
    spark.sql(
        """
        select *
         from orders
        """
    )
)

# display(
#     spark.sql(
#         """
#         select rev.review_id, rev.order_id, cust.customer_id, cust.customer_unique_id
#         from duplicates rev
#         left join customers cust
#         on cust.customer_id = rev.order_id
#         """
#     )
# )

# display(
#     spark.sql(
#         """
#         select rev.review_id, order_id, cust.customer_unique_id
#         from duplicates rev 
#         left JOIN customers cust
#         on rev.order_id = cust.customer_id 
#         """
#     )
# )

#let's take a step back and inspect the order_reviews and customers table to makse sure the values exist

df_review_customer_order_match = spark.sql(
        """
        select rev.review_id, ord.customer_id, rev.order_id
        from duplicates rev
        join orders ord
        on rev.order_id = ord.order_id
        order by rev.review_id
        """
    )
display(df_review_customer_order_match)
df_review_customer_order_match.createOrReplaceTempView("rev_cust_ord_match")
#based on the table I was outputted, the review_id maps to different customer_id and order_id
#now that I have obtained these values I can go and check the customer_id inside the customer table
#checking if customer_id maps to the same or different customers

display(
    spark.sql(
        """
        select rev.review_id, rev.order_id,cust.customer_id, cust.customer_unique_id
        from rev_cust_ord_match rev
        join customers cust
        on rev.customer_id = cust.customer_id
        order by review_id
        """
    )
)

display(
spark.sql(
    """
    select cus.customer_unique_id, count(*) as c
    from customers cus
    group by cus.customer_unique_id
    having c >1
    """
)
)

display(
spark.sql(
    """
    select cus.customer_id, count(*) as c
    from customers cus
    group by cus.customer_id
    having c >1
    """
)
)

# COMMAND ----------

# display(
#     spark.sql(
#         """
#         select * 
#         from orders
#         limit 10
#         """
#     )
# )

# print(df_orders.columns)
# ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']

display(
    spark.sql(
        """
        select min(date(order_purchase_timestamp)), max(order_purchase_timestamp), min(order_delivered_carrier_date), max(order_delivered_carrier_date), min(order_estimated_delivery_date), max(order_estimated_delivery_date)
        from orders

        """
    )
)

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %scala
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

# COMMAND ----------

# MAGIC %scala
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

# COMMAND ----------

# MAGIC %scala
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

# COMMAND ----------

# MAGIC %scala
# MAGIC val finalDF = create_final_df()
# MAGIC finalDF.createOrReplaceTempView("calendar")

# COMMAND ----------

# spark.sql("SELECT * FROM my_table").show()
dim_calendar = spark.sql("SELECT * FROM calendar").toPandas()
dim_calendar.tail(10)

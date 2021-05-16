import subprocess
import os
import traceback
import sys
from datetime import date, datetime, timedelta
import time
import datetime

from pyspark.sql.functions import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf = SparkConf().setAppName('retail_usecase_processing')
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

sqlContext.setConf("spark.sql.shuffle.partitions", "10")
sqlContext.setConf("hive.exec.dynamic.partition", "true")
sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")


DB_NAME = "retail"
#DB_NAME = sys.argv[0]
date_difference_in_days = 1
#date_difference_in_days = sys.argv[1]
PARTITION_DATE = (datetime.datetime.now() -
                  timedelta(days=date_difference_in_days)).strftime('%Y-%m-%d')
#PARTITION_MONTH = (datetime.datetime.now() - timedelta(months=1)).strftime('%Y-%m')
PARTITION_MONTH = (date.today().replace(day=1) -
                   timedelta(days=1)).strftime('%Y-%m')


# Loading categories table
categories = sqlContext.sql(
    """select * from {0}.categories""".format(DB_NAME))
categories.registerTempTable('categories')

# Loading departments table
departments = sqlContext.sql(
    """select * from {0}.departments""".format(DB_NAME))
departments.registerTempTable('departments')

# Loading custoemrs table
customers = sqlContext.sql("""select * from {0}.customers""".format(DB_NAME))
customers.registerTempTable('customers')

# Loading orders table for t-1
orders = sqlContext.sql(
    """select * from {0}.orders where order_status not in ('CANCELED', 'ON_HOLD') and dt = date_sub(current_date(),{1})""".format(DB_NAME, date_difference_in_days))
orders.registerTempTable('orders')

# Loading order_items table
order_items = sqlContext.sql(
    """select * from {0}.order_items""".format(DB_NAME))
order_items.registerTempTable('order_items')

# Loading products table
products = sqlContext.sql("""select * from {0}.products""".format(DB_NAME))
products.registerTempTable('products')


# Joining categories, products, order_items and orders tables
cat_prod_ord_items = sqlContext.sql(
    """select c.*, o.order_customer_id as customer_id, p.product_id, oi.* from categories c join products p on c.category_id = p.product_category_id join order_items oi on p.product_id = oi.order_item_product_id join orders o on oi.order_item_order_id = o.order_id """)
cat_prod_ord_items.registerTempTable('cat_prod_ord_items')

sqlContext.cacheTable('cat_prod_ord_items')


#### Category wise sales per day ####
category_wise_sales1 = sqlContext.sql(
    """select category_id, category_name, category_department_id as department_id, round(sum(order_item_subtotal),2) as total_sales from cat_prod_ord_items group by category_id, category_name, category_department_id""")
category_wise_sales1.registerTempTable('category_wise_sales1')

category_wise_sales = sqlContext.sql("""select c.category_id, c.category_name, d.department_name, c.total_sales from category_wise_sales1 c join departments d on c.department_id = d.department_id""").withColumn(
    "txn_date", from_unixtime(unix_timestamp() - (86400 * lit(date_difference_in_days)), 'yyyy-MM-dd'))
category_wise_sales.registerTempTable('category_wise_sales')

# Creating Hive table to store output for category_wise_sales
sqlContext.sql(
    """create table IF NOT EXISTS {0}.category_wise_sales(category_id int, category_name string, department_name string, total_sales double) partitioned by (txn_date string)""".format(DB_NAME))

# Dropping existing partition in case of re-execution
sqlContext.sql("""alter table {0}.category_wise_sales drop IF EXISTS partition(txn_date = '{1}') """.format(
    DB_NAME, PARTITION_DATE))

# Inserting output into table partition
sqlContext.sql(
    """insert into table {0}.category_wise_sales partition(txn_date) select * from category_wise_sales""".format(DB_NAME))


#### Department wise sales per day ####
department_wise_sales_1 = sqlContext.sql(
    """select category_department_id, round(sum(order_item_subtotal),2) as total_sales from cat_prod_ord_items group by category_department_id""")
department_wise_sales_1.registerTempTable('department_wise_sales_1')

department_wise_sales = sqlContext.sql("""select dw.category_department_id as department_id, d.department_name, dw.total_sales from department_wise_sales_1 dw join departments d on dw.category_department_id = d.department_id""").withColumn(
    "txn_date", from_unixtime(unix_timestamp() - (86400 * lit(date_difference_in_days)), 'yyyy-MM-dd'))
department_wise_sales.registerTempTable('department_wise_sales')

# Creating Hive table to store output for department_wise_sales
sqlContext.sql(
    """create table IF NOT EXISTS {0}.department_wise_sales(department_id int, department_name string, total_sales double) partitioned by (txn_date string)""".format(DB_NAME))

# Dropping existing partition in case of re-execution
sqlContext.sql("""alter table {0}.department_wise_sales drop IF EXISTS partition(txn_date = '{1}') """.format(
    DB_NAME, PARTITION_DATE))

# Inserting output into table partition
sqlContext.sql(
    """insert into table {0}.department_wise_sales partition(txn_date) select * from department_wise_sales""".format(DB_NAME))


#### Quantity-wise TOP-5 products bought daily ####
top5_prod_qty1 = sqlContext.sql(
    """select product_id, sum(order_item_quantity) as total_qty from cat_prod_ord_items group by product_id order by total_qty desc limit 5""")
top5_prod_qty1.registerTempTable('top5_prod_qty1')

top5_prod_qty = sqlContext.sql("""select t.product_id, p.product_name, t.total_qty from top5_prod_qty1 t join products p on t.product_id = p.product_id """).withColumn(
    "txn_date", from_unixtime(unix_timestamp() - (86400 * lit(date_difference_in_days)), 'yyyy-MM-dd'))
top5_prod_qty.registerTempTable('top5_prod_qty')

# Creating Hive table to store output for top5_prod_qty
sqlContext.sql(
    """create table IF NOT EXISTS {0}.top5_prod_qty(product_id int, product_name string, total_qty double) partitioned by (txn_date string)""".format(DB_NAME))

# Dropping existing partition in case of re-execution
sqlContext.sql("""alter table {0}.top5_prod_qty drop IF EXISTS partition(txn_date = '{1}') """.format(
    DB_NAME, PARTITION_DATE))

# Inserting output into table partition
sqlContext.sql(
    """insert into table {0}.top5_prod_qty partition(txn_date) select * from top5_prod_qty""".format(DB_NAME))


#### Amount-wise TOP-5 products bought daily ####
top5_prod_amt1 = sqlContext.sql(
    """select product_id, round(sum(order_item_subtotal),2) as total_amount from cat_prod_ord_items group by product_id order by total_amount desc limit 5""")
top5_prod_amt1.registerTempTable('top5_prod_amt1')

top5_prod_amt = sqlContext.sql("""select t.product_id, p.product_name, t.total_amount from top5_prod_amt1 t join products p on t.product_id = p.product_id """).withColumn(
    "txn_date", from_unixtime(unix_timestamp() - (86400 * lit(date_difference_in_days)), 'yyyy-MM-dd'))
top5_prod_amt.registerTempTable('top5_prod_amt')

# Creating Hive table to store output for top5_prod_amt
sqlContext.sql(
    """create table IF NOT EXISTS {0}.top5_prod_amt(product_id int, product_name string, total_amount double) partitioned by (txn_date string)""".format(DB_NAME))

# Dropping existing partition in case of re-execution
sqlContext.sql("""alter table {0}.top5_prod_amt drop IF EXISTS partition(txn_date = '{1}') """.format(
    DB_NAME, PARTITION_DATE))

# Inserting output into table partition
sqlContext.sql(
    """insert into table {0}.top5_prod_amt partition(txn_date) select * from top5_prod_amt""".format(DB_NAME))


#### Daily Order status ####
order_status = sqlContext.sql("""select order_status, count(order_id) as order_count from {0}.orders where dt = date_sub(current_date(),{1}) group by order_status""".format(
    DB_NAME, date_difference_in_days)).withColumn("txn_date", from_unixtime(unix_timestamp() - (86400 * lit(date_difference_in_days)), 'yyyy-MM-dd'))
order_status.registerTempTable('order_status')

# Creating Hive table to store output for order_status
sqlContext.sql(
    """create table IF NOT EXISTS {0}.order_status(order_status string, order_count bigint) partitioned by (txn_date string)""".format(DB_NAME))

# Dropping existing partition in case of re-execution
sqlContext.sql("""alter table {0}.order_status drop IF EXISTS partition(txn_date = '{1}') """.format(
    DB_NAME, PARTITION_DATE))

# Inserting output into table partition
sqlContext.sql(
    """insert into table {0}.order_status partition(txn_date) select * from order_status""".format(DB_NAME))


#### TOP-5 spending customers - daily ####
top5_custs_daily1 = sqlContext.sql(
    """select customer_id, round(sum(order_item_subtotal),2) as total_amount from cat_prod_ord_items group by customer_id order by total_amount desc limit 5""")
top5_custs_daily1.registerTempTable('top5_custs_daily1')

top5_custs_daily = sqlContext.sql("""select t.customer_id, c.customer_fname, c.customer_lname, c.customer_city, c.customer_state, t.total_amount from top5_custs_daily1 t join customers c on t.customer_id = c.customer_id """).withColumn(
    "txn_date", from_unixtime(unix_timestamp() - (86400 * lit(date_difference_in_days)), 'yyyy-MM-dd'))
top5_custs_daily.registerTempTable('top5_custs_daily')

# Creating Hive table to store output for top5_custs_daily
sqlContext.sql(
    """create table IF NOT EXISTS {0}.top5_custs_daily(customer_id int, customer_fname string, customer_lname string, customer_city string, customer_state string, total_amount double) partitioned by (txn_date string)""".format(DB_NAME))

# Dropping existing partition in case of re-execution
sqlContext.sql("""alter table {0}.top5_custs_daily drop IF EXISTS partition(txn_date = '{1}') """.format(
    DB_NAME, PARTITION_DATE))

# Inserting output into table partition
sqlContext.sql(
    """insert into table {0}.top5_custs_daily partition(txn_date) select * from top5_custs_daily""".format(DB_NAME))


# TOP-5 spending customers - monthly

dt_str = datetime.datetime.now().strftime('%d')

if dt_str == '01':
    orders_monthly = sqlContext.sql(
        """select * from {0}.orders where order_status not in ('CANCELED', 'ON_HOLD') and substr(dt,0,7) = substr(add_months(current_date(),-1),0,7)""".format(DB_NAME))
    orders_monthly.registerTempTable('orders_monthly')
    cat_prod_ord_items_mon = sqlContext.sql(
        """select c.*, o.order_customer_id as customer_id, p.product_id, oi.* from categories c join products p on c.category_id = p.product_category_id join order_items oi on p.product_id = oi.order_item_product_id join orders_monthly o on oi.order_item_order_id = o.order_id """)
    cat_prod_ord_items_mon.registerTempTable('cat_prod_ord_items_mon')
    top5_custs_monthly1 = sqlContext.sql(
        """select customer_id, round(sum(order_item_subtotal),2) as total_amount from cat_prod_ord_items_mon group by customer_id order by total_amount desc limit 5""")
    top5_custs_monthly1.registerTempTable('top5_custs_monthly1')
    top5_custs_monthly = sqlContext.sql("""select t.customer_id, c.customer_fname, c.customer_lname, c.customer_city, c.customer_state, t.total_amount from top5_custs_monthly1 t join customers c on t.customer_id = c.customer_id """).withColumn(
        "txn_month", from_unixtime(unix_timestamp() - (86400 * lit(date_difference_in_days)), 'yyyy-MM'))
    print "Monthly report generated"
    top5_custs_monthly.registerTempTable('top5_custs_monthly')
    # Creating Hive table to store output for top5_custs_monthly
    sqlContext.sql(
        """create table IF NOT EXISTS {0}.top5_custs_monthly(customer_id int, customer_fname string, customer_lname string, customer_city string, customer_state string, total_amount double) partitioned by (txn_month string)""".format(DB_NAME))
    # Dropping existing partition in case of re-execution
    sqlContext.sql("""alter table {0}.top5_custs_monthly drop IF EXISTS partition(txn_month = '{1}') """.format(
        DB_NAME, PARTITION_MONTH))
    # Inserting output into table partition
    sqlContext.sql(
        """insert into table {0}.top5_custs_monthly partition(txn_month) select * from top5_custs_monthly""".format(DB_NAME))


print "Processing job completed successfully"

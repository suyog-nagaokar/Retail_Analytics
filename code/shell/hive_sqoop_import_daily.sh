

echo "**************************************************"
echo "create retail db if not exists"
hive -e "CREATE DATABASE IF NOT EXISTS retail;"
echo "**************************************************"

echo "**************************************************"
echo "importing orders table"
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db --username root --password cloudera --query 'select *, DATE_SUB(CURDATE(), INTERVAL 1 DAY) as dt from orders where order_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY) and $CONDITIONS' --split-by order_id --target-dir hdfs://quickstart.cloudera:8020/user/cloudera/retail_usecase/sqoop_import/orders_temp -m 2 --hive-import --hive-table retail.orders_temp --hive-overwrite --delete-target-dir
echo "**************************************************"

echo "**************************************************"
echo "Moving orders to partitions table"
hive -f hive_orders_partitioning.hive
echo "**************************************************"

echo "**************************************************"
echo "importing categories table"
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db --username root --password cloudera --table categories --target-dir hdfs://quickstart.cloudera:8020/user/cloudera/retail_usecase/sqoop_import/categories -m 2 --hive-import --hive-table retail.categories --hive-overwrite --delete-target-dir
echo "**************************************************"

echo "**************************************************"
echo "importing departments table"
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db --username root --password cloudera --table departments --target-dir hdfs://quickstart.cloudera:8020/user/cloudera/retail_usecase/sqoop_import/departments -m 2 --hive-import --hive-table retail.departments --hive-overwrite --delete-target-dir
echo "**************************************************"

echo "**************************************************"
echo "importing products table"
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db --username root --password cloudera --table products --target-dir hdfs://quickstart.cloudera:8020/user/cloudera/retail_usecase/sqoop_import/products -m 2 --hive-import --hive-table retail.products --hive-overwrite --delete-target-dir
echo "**************************************************"

echo "**************************************************"
echo "importing customers table"
sqoop import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db --username root --password cloudera --table customers --target-dir hdfs://quickstart.cloudera:8020/user/cloudera/retail_usecase/sqoop_import/customers -m 2 --hive-import --hive-table retail.customers --hive-overwrite --delete-target-dir
echo "**************************************************"

echo "**************************************************"
echo "executing job: importing order_items table incrementally"
sqoop job --exec import_order_items
echo "**************************************************"



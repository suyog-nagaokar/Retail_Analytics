hive -e "use retail;"

if [ $? -eq 0 ];
then
    echo "Import from mysql to hive with scoop daily job."
    sh hive_scoop_import_daily.sh
    sh spark_retail_processing.sh
else
    echo "Running for the first time."
    sh mysql_date_update.sh
    sh hive_scoop_import_first_time.sh
    echo 'Copy hive-site.xml to spark'
    cp /etc/hive/conf/hive-site.xml /usr/lib/spark/conf
    sh spark_retail_processing.sh
    hive -e "truncate table retail.order_items;"
    sqoop job --delete import_order_items
    sqoop job --create import_order_items -- import --connect jdbc:mysql://quickstart.cloudera:3306/retail_db --username root --password cloudera --table order_items --target-dir hdfs://quickstart.cloudera:8020/user/cloudera/retail_usecase/sqoop_import/order_items --incremental append --check-column order_item_id --last-value 0 --hive-import --hive-table retail.order_items
fi


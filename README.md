# Retail analytics

This project aims at generating insights around the daily & monthly transactions which will help the business to execute campaigns for Cross-sell and Up-sell. The reporting team's insights can also be used to generate dashboards and present the outcome in a visually appealing format.

The retail analytics project consists of retail data points including customers, categories/departments, products, and transactional data.

Data is imported from the RDBMS into the Hive data warehouse using Apache Sqoop.
Business logic is applied and this data is processed using Apache Spark to generate multiple insights. The final output is stored back to Hive, consumed by the campaign/marketing/reporting teams. The entire process can be automated using Oozie/Airflow.

## Setup

You can use CDH Quickstart VM v5.x which has all the services pre-installed.
In CDH VM, click on places --> Home Folder. Then copy + paste the scripts from your local PC to Home Folder (/home/cloudera/) in VM.

## Run

Once the CDH VM is started,
Follow the below steps:

1. Update our existing MYSQL database for current dates. NOTE: - the dates are as of March 2021. These will be updated frequently in the repo.
2. Import data into hive with Sqoop.
3. Generating insights using transformations and aggregations for analytics team with spark and store the output back to hive.
4. Optional: - Import all Oozie workflows(JSON) in HUE and run the main workflow.

### Update existing MYSQL database for current dates

Connect to MySQL DB with below command. Password is 'cloudera'

```
mysql -u root -p
```

Run `show databases` in order to see if the "retail" db is available.

Run the script `/home/cloudera/mysql_date_update_sql.sh`.
The last updated date will be displayed in the end.

### Import data into hive with scoop

Create database in hive, import tables and setup partitioning via the shell script

```
sh hive_sqoop_import.sh
```

### Generating insights using transformations and aggregations for analytics team with spark and store the output back to hive.

Create database in hive, import tables and setup partitioning via the shell script

```

sh spark_retail_processing.sh
```

Finally analysts can use the results to display information.

The output can be queried in the Hive shell. It can also be viewed in HUE query editor which also provides a basic dashboard interface to view the output.


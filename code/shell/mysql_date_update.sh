echo "Start update sql"
mysql -h localhost -u root -pcloudera < mysql_date_update.sql
echo "End update sql"

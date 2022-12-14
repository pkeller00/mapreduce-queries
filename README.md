# mapreduce-queries
A Java program which implements four different queries based on the MapReduce framework. These were perfomed on a the TCP-DS benchmark dataset in the 1GB and 40GB (approximately 10 million records) scale, however the code is still compatible with larger scalling factors. We chose to focus on the store and store_sales table, whose schema can be seen in the schema folder. 

To test our code we perofmred Exploratory Data Analysis in order to find the distrubtions of the data and thus see how well our code scales as we increase the number of records we need to access. Our program was compared against a standard Hive SQL program for any given query and time and  resoucres used were compared. For every query our pgraom perofrmed better than the Hive equivalent. 

## Example
We assume the path to hadoop folder is HADOOP_HOME and store_sales.dat is stored at STORE_SALES_PATH and store.dat stored at STORE_PATH on HDFS file system.


First you will hvae to compile the Java code using the following lines, as shown for query 1a:
1) cd mapreduce-queries/1a
2) {HADOOP_HOME}/bin/hadoop com.sun.tools.javac.Main -d ./ *.java
3) jar cf wc.jar Main*.class *.class

After compiling the code you will need to execute the query using its appropriate run command:
1) {HADOOP_HOME}/bin/hadoop jar wc.jar Main 10 2450816 2451181 {STORE_SALES_PATH}/store_sales.dat output
2) {HADOOP_HOME}/bin/hdfs dfs -cat output/part-r-00000

## Queries
We use three varaibles for the queries. These are:
K: return top K records
start_date: start date of the given period
end_date: end date of the given period

### 1a
Print top K stores by net paid in a given period.
#### Query
SELECT ss_store_sk AS store_sk, COALESCE(SUM(ss_net_paid), 0) AS net_paid FROM store_sales WHERE 
ss_sold_date_sk >= 2451146 AND ss_sold_date <= 2451894 GROUP BY ss_store_sk ORDER BY net_paid DESC LIMIT 10;
#### Command
{HADOOP_HOME}/bin/hadoop jar wc.jar Main K start_date end_date {STORE_SALES_PATH}/store_sales.dat output_directory

### 1b
Print top K selling items by number of sales in a given period.
#### Query
SELECT ss_item_sk AS item_sk, COALESCE(SUM(ss_quantity), 0) AS quantity FROM store_sales WHERE ss_sold_date_sk >= 2451146 AND ss_sold_date <= 2451894 GROUP BY ss_item_sk ORDER BY quantity DESC LIMIT 10;
#### Command
{HADOOP_HOME}/bin/hadoop jar wc.jar Main K start_date end_date {STORE_SALES_PATH}/store_sales.dat output_directory

### 1c
Print the top K days with the highest total net paid including tax in a given period
#### Query
SELECT ss_sold_date_sk AS sold_date, COALESCE(SUM(ss_net_paid_inc_tax), 0) AS net_paid_inc_tax FROM store_sales WHERE ss_sold_date_sk >= 2451146 AND ss_sold_date <= 2451894 GROUP BY ss_sold_date_sk ORDER BY net_paid_inc_tax DESC LIMIT 10;
#### Command
{HADOOP_HOME}/bin/hadoop jar wc.jar Main K start_date end_date {STORE_SALES_PATH}/store_sales.dat output_directory

### 2
Print top K stores by total net paid in a given period and the total floor space in each store. Sort based on floor space, breaking ties with total net paid
#### Query
SELECT store.s_store_sk AS store_sk, store.s_floor_space AS floor_space, COALESCE(SUM(store_sales.ss_net_paid), 0) AS net_paid FROM store_sales RIGHT OUTER JOIN store ON (STORE.s_store_sk = STORE_SALES.ss_store_sk) WHERE (STORE_SALES.ss_sold_date_sk >= 2451146 AND STORE_SALES.ss_sold_date_sk <= 2451894) OR (STORE_SALES.ss_net_paid IS NULL) GROUP BY STORE.s_store_sk, STORE.s_floor_space ORDER BY STORE.s_floor_space DESC, net_paid DESC LIMIT 10;
#### Command
{HADOOP_HOME}/bin/hadoop jar wc.jar Main K start_date end_date {STORE_SALES_PATH}/store_sales.dat {STORE_PATH}/store.dat output_directory

Scenario Based questions:

Q1.Will the reducer work or not if you use “Limit 1” in any HiveQL query?
   
     No,  It won't work if we use “Limit 1” in any HiveQL query
     
Q2.Suppose I have installed Apache Hive on top of my Hadoop cluster using default metastore configuration. Then, what will happen if we have multiple clients trying to access Hive at the same time? 

    The default metastore configuration allows only one Hive session to be opened at a time for accessing the metastore. 
    Therefore, if multiple clients try to access the metastore at the same time, they will get an error. One has to use a 
    standalone metastore, i.e. Local or remote metastore configuration in Apache Hive for allowing access to multiple clients concurrently. 


Q3.Suppose, I create a table that contains details of all the transactions done by the customers: CREATE TABLE transaction_details (cust_id INT, amount FLOAT, month STRING, country STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ ;
Now, after inserting 50,000 records in this table, I want to know the total revenue generated for each month. But, Hive is taking too much time in processing this query. How will you solve this problem and list the steps that I will be taking in order to do so?

      
      It's taking time because of lots of data are following over network to perform group by operation. It can be prevented if we store the data in hive using partition by keyword on the relevent column
      In this case month would be the ideal field. We idealy prtition data by the field's which more oftenly used in group by clause
      
      steps followd for partitioning:
         Step 01.creating partition table
           create table transaction_partition
            (
            cust_id int,
            amount float,
            month string,
            country string
            )
            partitioned by (month string);
            
         Step 02. Set hive properties for dynamic partitioning 
            set hive.exec.dynamic.partition.mode=nonstrict;
            
         Step 03. Load data into partition table
         
            insert overwrite transaction_partition partition(month) select cust_id,amount,month,country from transaction_details;
            
            
 Q4.How can you add a new partition for the month December in the above partitioned table?           
            
       ALTER TABLE transaction_partition ADD PARTITION (month=’Dec’)

       
 Q5.I am inserting data into a table based on partitions dynamically. But, I received an error – FAILED ERROR IN SEMANTIC ANALYSIS: Dynamic partition strict mode requires at least one static partition column. How will you remove this error?
 
  
      To remove this error We need to modify hive properties.
      
      SET hive.exec.dynamic.partition = true;
      SET hive.exec.dynamic.partition.mode = nonstrict;
      
Q6.Suppose, I have a CSV file – ‘sample.csv’ present in ‘/temp’ directory with the following entries:
   id first_name last_name email gender ip_address
   How will you consume this CSV file into the Hive warehouse using built-in SerDe?

       
       SerDe expands as  serializer/deserializer. A SerDe converts the data into encrypted binary format  
       that we can process using Hive. Hive comes with several built-in SerDes and many other third-party SerDes are also available. 

         Hive provides a specific SerDe for working with CSV files.

         CREATE EXTERNAL TABLE sample

         (id int, 
          first_name string, 
          last_name string, email string,
          gender string, ip_address string) 
          ROW FORMAT SERDE ‘org.apache.hadoop.hive.serde2.OpenCSVSerde’
          with serdeproperties (
          "separatorChar" = ",",
          "quoteChar" = """,
          "escapeChar" = "\"
          STORED AS TEXTFILE LOCATION ‘/temp’;
          
 
Q7. Suppose, I have a lot of small CSV files present in the input directory in HDFS and I want to create a single Hive table corresponding to these files. The data in these files are in the format: {id, name, e-mail, country}. Now, as we know, Hadoop performance degrades when we use lots of small files.So, how will you solve this problem where we want to create a single Hive table for lots of small files without degrading the performance of the system?

      
      ### Small files can be grouped by using SequenceFile format.
      create table details(
      id int,
      name string,
      e-mail string,
      country string)
      row format delimited
      fields terminated by ',';

      ### loading data into table 
      insert data inapath '/input/' into table details;

      creating table for storing data in sequencefile format
      create table details_seqfile(
      id int,
      name string,
      e-mail string,
      country string)
      row format delimited
      fields terminated by ','
      stored as sequencefile;

      ### load data from details table

      insert overwrite table details_seqfile select * from details;

      ### so the all file will store in single file.


Q8.LOAD DATA LOCAL INPATH ‘Home/country/state/’
   OVERWRITE INTO TABLE address;
   The following statement failed to execute. What can be the cause?
   
         When data loading from local the path should contain 'file:///' so the path would be
         'file:///Home/country/state/' 
         
Q9.Is it possible to add 100 nodes when we already have 100 nodes in Hive? If yes, how?

   
         Yes, we can add the nodes by following the below steps:

         Step 1: Take a new system; create a new username and password
         Step 2: Install SSH and with the master node setup SSH connections
         Step 3: Add ssh public_rsa id key to the authorized keys file
         Step 4: Add the new DataNode hostname, IP address, and other details in /etc/hosts slaves file:

                192.168.1.102 slave3.in slave3  
                
         Step 5: Start the DataNode on a new node
         Step 6: Login to the new node like suhadoop or:

                 ssh -X hadoop@192.168.1.103
                 
         Step 7: Start HDFS of the newly added slave node by using the following command:

                ./bin/hadoop-daemon.sh start data node
                
         Step 8: Check the output of the jps command on the new node
         
        


Hive Practical questions:

Hive Join operations

Create a  table named CUSTOMERS(ID | NAME | AGE | ADDRESS   | SALARY)
Create a Second  table ORDER(OID | DATE | CUSTOMER_ID | AMOUNT
)

Now perform different joins operations on top of these tables
(Inner JOIN, LEFT OUTER JOIN ,RIGHT OUTER JOIN ,FULL OUTER JOIN)



      create table customers(
          > id int,
          > name string,
          > age int,
          > address string,
          > salary int);
          
          
      create table orders(
          > o_id int,
          > o_date string,
          > customer_id int,
          > amount int
          > );
       
       orders table has date column and taken data as string, to cast the data into date datatype 
       creating another table 
       create table orders_casted(
          > o_id int,
          > o_date date,
          > customer_id int,
          > amount int)
          > row format delimited 
          > fields terminated by ','
          > tblproperties("skip.header.line.count" = "1");
          
              hive> select * from customers;
               OK
               
               customers.id       customers.name  customers.age   customers.address       customers.salary
               1       arjun      22      hyd             23000
               2       Dikshant   24      hyd             24000
               3       Akshat     25      banglore        26000
               4       Dhruv      31      delhi           90000
           
           Load data from original table to transformed table 
           
               from orders insert overwrite table orders_casted select*;

               hive> select * from orders_casted;
               
               
               OK
               102     2022-03-21      2       22000
               103     2022-03-21      2       22000
               104     2022-03-01      3       25000
               102     2022-03-21      4       32000
               Time taken: 0.117 seconds, Fetched: 4 row(s)

               hive> describe formatted orders_casted;

               OK
               # col_name              data_type               comment             

               o_id                    int                                         
               o_date                  date                                        
               customer_id             int                                         
               amount                  int                                         

               # Detailed Table Information             
               Database:               default                  
               Owner:                  root                     
               CreateTime:             Fri Mar 03 06:32:47 UTC 2023     
               LastAccessTime:         UNKNOWN                  
               Retention:              0                        
               Location:               hdfs://namenode:9000/user/hive/warehouse/orders_casted   
               Table Type:             MANAGED_TABLE            
               Table Parameters:                
                       COLUMN_STATS_ACCURATE   {\"BASIC_STATS\":\"true\"}
                       numFiles                1                   
                       numRows                 5                   
                       rawDataSize             110                 
                       skip.header.line.count  1                   
                       totalSize               115                 
                       transient_lastDdlTime   1677825376          

               # Storage Information            
               SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe       
               InputFormat:            org.apache.hadoop.mapred.TextInputFormat         
               OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat       
               Compressed:             No                       
               Num Buckets:            -1                       
               Bucket Columns:         []                       
               Sort Columns:           []                       
               Storage Desc Params:             
                       field.delim             ,                   
                       serialization.format    ,                   
               Time taken: 0.082 seconds, Fetched: 35 row(s)
               
              
INNER JOIN 

      select customers.*, casted_orders.* 
      from customers
      inner join casted_orders on customers.id=casted_orders.customer_id;
    

      customers.id    customers.name  customers.age   customers.address       customers.salary       casted_orders.o_id       casted_orders.o_date                           casted_orders.customer_id      casted_orders.amount
      2       Dikshant        24      hyd        24000   102     2022-03-21      2       22000
      2       Dikshant        24      hyd        24000   103     2022-03-21      2       22000
      3       Akshat          25      banglore   26000   104     2022-03-01      3       25000
      4       Dhruv           31      delhi      90000   102     2022-03-21      4       32000
      
      
      
LEFT OUTER JOIN


       select customers.*, casted_orders.* 
       from customers
       left outer join casted_orders on customers.id=casted_orders.customer_id;

      customers.id    customers.name  customers.age   customers.address  customers.salary casted_orders.o_id  
      casted_orders.o_date casted_orders.customer_id   casted_orders.amount
      1       arjun           22      hyd     23000   NULL    NULL    NULL    NULL
      2       Dikshant        24      hyd     24000   102     2022-03-21      2       22000
      2       Dikshant        24      hyd     24000   103     2022-03-21      2       22000
      3       Akshat          25      banglore26000   104     2022-03-01      3       25000
      4       Dhruv           31      delhi   90000   102     2022-03-21      4       32000
      Time taken: 6.506 seconds, Fetched: 5 row(s)
      
RIGHT OUTER JOIN

      hive> select customers.*, casted_orders.* 
      > from customers
      > right outer join casted_orders on customers.id=casted_orders.customer_id;
    
    
       customers.id   customers.name  customers.age   customers.address  customers.salary  casted_orders.o_id  
       casted_orders.o_date  casted_orders.customer_id       casted_orders.amount
      2       Dikshant        24      hyd     24000   102     2022-03-21      2       22000
      2       Dikshant        24      hyd     24000   103     2022-03-21      2       22000
      3       Akshat  25      banglore        26000   104     2022-03-01      3       25000
      4       Dhruv   31      delhi   90000   102     2022-03-21      4       32000
      Time taken: 6.263 seconds, Fetched: 4 row(s)
      
      
FULL OUTER JOIN 

      hive> select customers.*, casted_orders.* 
      > from customers
      > full outer join casted_orders on customers.id=casted_orders.customer_id;
    
      customers.id    customers.name  customers.age   customers.address customers.salary  casted_orders.o_id
      casted_orders.o_date    casted_orders.customer_id       casted_orders.amount
      1     arjun     22     hyd       23000   NULL    NULL    NULL    NULL
      2     Dikshant  24     hyd       24000   103     2022-03-21      2       22000
      2     Dikshant  24     hyd       24000   102     2022-03-21      2       22000
      3     Akshat    25     banglore  26000   104     2022-03-01      3       25000
      4     Dhruv     31     delhi     90000   102     2022-03-21      4       32000
      Time taken: 1.434 seconds, Fetched: 5 row(s)
      


BUILD A DATA PIPELINE WITH HIVE

Download a data from the given location - 
https://archive.ics.uci.edu/ml/machine-learning-databases/00360/


    create table AirQuality(
    activity_date string,
    time string,
    co float,
    S1 int,
    NMHC int,
    C6H6 float,
    S2 int,
    NOx int,
    S3 int,
    No2 int,
    S4 int,
    S5 int,
    T float,
    RH float,
    AH float )
    row format delimited
    fields terminated by ','
     ;
 
 --- here i forgot the table property while creating   
 
      hive> alter table AirQuality set TBLPROPERTIES('skip.header.line.count' = "1");  
  
 LOADING DATA 
 
      load data inpath 'data/airquality/airquality.csv' into table AirQuality
 
 SELECT STATEMENT
 
       hive> select activity_date,time,co from AirQuality limit 5;
      OK
      activity_date   time            co
      10-03-2004      18:00:00        2.6
      10-03-2004      19:00:00        2.0
      10-03-2004      20:00:00        2.2
      10-03-2004      21:00:00        2.2
      10-03-2004      22:00:00        1.6
      Time taken: 0.109 seconds, Fetched: 5 row(s)
   
GROUP BY STATEMENT 

      hive> select activity_date, sum(co)as sum_co,avg(s1) as s1_avg from AirQuality
          > group by activity_date limit 10;
          
          
      WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine using Hive 1.X releases.
      Query ID = root_20230305161438_44796de7-bd6d-4f57-a276-899ad0a1bc0c
      Total jobs = 1
      Launching Job 1 out of 1
      Number of reduce tasks not specified. Estimated from input data size: 1
      In order to change the average load for a reducer (in bytes):
        set hive.exec.reducers.bytes.per.reducer=<number>
      In order to limit the maximum number of reducers:
        set hive.exec.reducers.max=<number>
      In order to set a constant number of reducers:
        set mapreduce.job.reduces=<number>
      Job running in-process (local Hadoop)
      2023-03-05 16:14:40,188 Stage-1 map = 100%,  reduce = 100%
      Ended Job = job_local1181967307_0003
      MapReduce Jobs Launched: 
      Stage-Stage-1:  HDFS Read: 4714450 HDFS Write: 0 SUCCESS
      Total MapReduce CPU Time Spent: 0 msec
      OK
      activity_date   sum_co                  s1_avg
      01-01-2005      -150.90000021457672     1114.25
      01-02-2005      68.79999965429306       1250.1666666666667
      01-03-2005      24.499999850988388      820.1666666666666
      01-04-2004      61.19999951124191       1063.8333333333333
      01-04-2005      -174.5                  903.2916666666666
      01-05-2004      -152.70000022649765     1097.5
      01-06-2004      -757.9999999031425      1135.5833333333333
      01-07-2004      51.900000393390656      1130.5833333333333
      01-08-2004      23.600000113248825      974.1666666666666
      01-09-2004      53.70000022649765       1060.5833333333333
      Time taken: 1.309 seconds, Fetched: 10 row(s)

Fetch the result of the select operation in your local as a csv file 

      insert overwrite directory '/user/hive/warehouse/outputfile/airquality.csv' row format delimited fields terminated by ',' 
      select * from AirQuality;

ORDER BY 

     hive> select * from AirQuality
     > order by time desc limit 10;
      airquality.activity_date        airquality.time airquality.co   airquality.s1   airquality.nmhc airquality.c6h6 airquality.s2   airquality.nox  airquality.s3           airquality.no2  airquality.s4    airquality.s5     airquality.t      airquality.rh         airquality.ah
      16-03-2004      23:00:00        1.7     1201    88      9.1     943     130     935     117     1560    1362    16.7    48.9    0.9226
      15-03-2004      23:00:00        1.4     1142    67      6.9     852     89      1008    101     1547    1164    15.3    61.4    1.058
      13-03-2004      23:00:00        2.6     1418    116     10.9    1010    172     892     130     1603    1536    14.7    49.3    0.8193
      18-03-2004      23:00:00        1.7     1262    -200    8.3     911     95      948     99      1545    1062    13.1    64.2    0.9606
      14-03-2004      23:00:00        2.2     1349    79      8.8     933     152     933     119     1617    1349    14.7    55.9    0.9314
      11-03-2004      23:00:00        1.0     913     26      2.6     629     47      1565    53      1252    552     8.2     60.8    0.6657
      12-03-2004      23:00:00        5.4     1677    367     21.8    1346    300     741     134     2062    1657    9.7     64.6    0.7771
      19-03-2004      23:00:00        2.1     1215    -200    8.3     912     127     948     109     1547    993     14.2    58.3    0.938
      03-04-2005      23:00:00        1.2     1100    -200    5.1     769     170     722     128     1147    1049    14.3    52.5    0.8497
      10-03-2004      23:00:00        1.2     1197    38      4.7     750     89      1337    96      1393    949     11.2    59.2    0.7848
      Time taken: 1.329 seconds, Fetched: 10 row(s)
    

REGEXP


RLIKE FUNCTION 

      hive> select * from AirQuality
          > where activity_date RLIKE '(2004)$';

      hive> select * from AirQuality
          > where activity_date RLIKE '^(30)' LIMIT 20;
      OK
      30-03-2004      00:00:00        1.0     899     33      2.6     630     57      1418    76      1050    425     13.1    26.8    0.4023
      30-03-2004      01:00:00        0.7     866     33      1.8     569     41      1468    66      1055    436     11.6    33.6    0.4595
      30-03-2004      02:00:00        0.7     900     32      1.7     563     46      1402    73      1130    618     10.5    42.9    0.5442
      30-03-2004      03:00:00        0.8     949     25      1.4     540     -200    1457    -200    1104    747     11.7    38.7    0.5298
      30-03-2004      04:00:00        -200.0  897     29      1.3     529     41      1462    69      1101    693     12.2    37.2    0.5277
      30-03-2004      05:00:00        0.7     956     26      2.3     605     52      1341    71      1117    691     12.9    33.9    0.5037
      30-03-2004      06:00:00        1.1     1054    86      5.3     779     111     1080    98      1266    1013    11.3    40.3    0.5376
      30-03-2004      07:00:00        2.6     1343    294     13.4    1096    191     839     115     1587    1311    11.9    38.4    0.5341
      30-03-2004      08:00:00        4.0     1584    664     23.8    1399    244     642     130     1947    1675    13.3    35.6    0.5416
      30-03-2004      09:00:00        4.2     1556    695     21.5    1337    283     674     150     1852    1689    16.3    28.8    0.5305
      30-03-2004      10:00:00        4.7     1565    735     21.0    1324    320     695     159     1872    1688    17.9    28.2    0.5741
      30-03-2004      11:00:00        3.9     1429    649     18.4    1251    249     734     146     1754    1548    18.3    26.6    0.5529
      30-03-2004      12:00:00        3.7     1419    586     18.9    1267    219     742     138     1763    1490    20.8    22.5    0.5463
      30-03-2004      13:00:00        3.4     1373    546     17.1    1213    200     791     128     1711    1354    22.4    20.0    0.5352
      30-03-2004      14:00:00        2.2     1185    245     10.4    992     138     942     97      1448    1014    20.9    21.7    0.5295
      30-03-2004      15:00:00        1.9     1122    178     8.0     897     118     991     91      1372    831     19.1    26.5    0.579
      30-03-2004      16:00:00        1.6     1085    130     6.2     820     99      1060    81      1325    699     18.1    30.6    0.6295
      30-03-2004      17:00:00        2.1     1249    151     9.7     968     112     874     99      1527    926     17.3    34.7    0.6788
      30-03-2004      18:00:00        2.2     1236    272     9.6     962     117     853     101     1532    928     15.8    40.4    0.7228
      30-03-2004      19:00:00        2.7     1334    301     11.9    1047    129     815     106     1638    1038    15.9    42.0    0.7541
      Time taken: 0.089 seconds, Fetched: 20 row(s)


REGEXP FUNCTION

      hive> select regexp_extract(activity_date, '2004',0)as year_2004 from AirQuality;
  
ALTER 

      hive> alter table AirQuality rename to Airquality;
      OK
      Time taken: 0.067 seconds


DISTINCT
      
      hive> select distinct activity_date from AirQuality;
      
      
UNION 

      (select * from airquality order by co desc limit 1)
      union
      (select * from airquality order by co limit 1);
      
VIEWS

          > create view Red_zone as
          > select * from Airquality
          > where co > 1.5;
      select * from Red_zone;

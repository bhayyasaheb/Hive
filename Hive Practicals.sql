
show databases;

use niit;

show tables;

show table structure:-
--------------------
desc formatted customer;

Location:           	hdfs://localhost:54310/user/hive/warehouse/niit.db/customer	 
Table Type:         	MANAGED_TABLE 

select * from customer;

turncate will delete data from table & drop will delete data and structure of table.	

truncate table customer;

select * from customer;

upload data in hdfs:- 
----------------
hadoop fs -put /home/hduser/custs /niit

Load the data into table from hdfs system:-
------------------------------------------ 
LOAD DATA INPATH '/niit/custs' OVERWRITE INTO TABLE customer;

drop table customer;
-------------------------------------------------------------------------------------------------------------------------------------------------------

create folder in hdfs:-
hadoop fs -mkdir /user/training

upload data from local system to hdfs:- 
hadoop fs -put /home/hduser/custs /user/training

Create External Table:-
---------------------
create external table customer
(
custno string,
firstname string,
lastname string,
age int,
profession string
)
row format delimited
fields terminated by ","
stored as textfile
location "/user/training";

show table stucture:-
-------------------
desc formatted customer;

Location:           	hdfs://localhost:54310/user/training	 
Table Type:         	EXTERNAL_TABLE      	

select * from customer;

-----------------------------------------------------------------------------------------------------------------------------------------------------








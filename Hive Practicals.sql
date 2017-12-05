
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

Q. Create External Tables

create folder in hdfs:-
hadoop fs -mkdir /user/training

upload data from local system to hdfs:- 
hadoop fs -put /home/hduser/custs /user/training
hadoop fs -put /home/hduser/custs_add /user/training

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

Q.  Text data stored in table in different form like textfile(csv),avro,sequence,orc format

create database college;

use college;

students.csv
------------
Bhayyasaheb,Maths,91
Bhayyasaheb,Physics,48
Bhayyasaheb,Chemistry,66
Sayali,Maths,96
Sayali,Physics,64
Sayali,Chemistry,73

---------------------------------------

1. Create Hive table stored as textfile

create table csv_table
(
student_name string,
subject string,
marks int
)
row format delimited 
fields terminated by ','
stored as textfile;
---------------------------------------------------------------------------------------
Load csv_table with students.csv

LOAD DATA LOCAL INPATH "/home/hduser/students.csv" OVERWRITE INTO TABLE csv_table;


--------------------------------------------------------------------------------------
in csv_table data is in text format we can check using following command in linux terminal:-

hadoop fs -cat /user/hive/warehouse/college.db/csv_table/*		*/
Bhayyasaheb,Maths,91
Bhayyasaheb,Physics,48
Bhayyasaheb,Chemistry,66
Sayali,Maths,96
Sayali,Physics,64
Sayali,Chemistry,73

--------------------------------------------------------------------------------------
check data in table using hive terminal:-

select * from csv_table;

output is :-
csv_table.student_name	csv_table.subject	csv_table.marks
Bhayyasaheb	Maths	91
Bhayyasaheb	Physics	48
Bhayyasaheb	Chemistry	66
Sayali	Maths	96
Sayali	Physics	64
Sayali	Chemistry	73

------------------------------------------------------------------------------------------------

2. Create another Hive table using AvroSerDe

CREATE TABLE avro_table
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES
('avro.schema.literal'=
'{
"namespace": "abc",
"name": "student_marks",	
"type": "record",
"fields": [{"name":"student_name","type":"string"},{"name":"subject","type":"string"},{"name":"marks","type":"int"}]
}'
);

---------------------------------------------------------------------------------------------------------------------

Load avro_table with data from csv_table

INSERT OVERWRITE TABLE avro_table SELECT student_name, subject, marks FROM csv_table;

------------------------------------------------------------------------------------------
In the avro_table data is in avro form we can check using this command in linux terminal:-

hadoop fs -cat /user/hive/warehouse/college.db/avro_table/*	*/

output is:-
Objavro.schema�{"type":"record","name":"student_marks","namespace":"abc","fields":[{"name":"student_name","type":"string"},{"name":"subject","type":"string"},{"name":"marks","type":"int"}]}�S=!��+te�:�߻}
                                               �Bhayyasaheb
Maths�BhayyasahebPhysicsBhayyasahebChemistry�
                                                   Sayali
Maths�
       SayaliPhysics�
                      SayaliChemistry��S=!��+te�:�߻}

---------------------------------------------------------------------------------------------------------------------------------------

also download this file in local system using following command in linux terminal:-

hadoop fs -get /user/hive/warehouse/college.db/avro_table/000000_0

---------------------------------------------------------------------------
check this file using following command in linux terminal:-

nano 000000_0 or gedit 000000_0

output is :- 
Objavro.schema\DC{"type":"record","name":"student_marks","namespace":"abc","fields":[{"name":"student_name","type":"string"},{"name":"subject","type":"string"},{"name":"marks","type":"int"}]}\00\F7S=!\FF\B7+te\E4:\DF߻}\E8Bhayyasaheb
Maths\B6BhayyasahebPhysicsBhayyasahebChemistry\84Sayali
Maths\C0SayaliPhysics\80SayaliChemistry\92\F7S=!\FF\B7+te\E4:\DF߻}
 
-----------------------------------------------------------------------------------------------------------------------

actual data in avro_table is avro format but in hive terminal data is getting in text format
check using following command in hive:-

select * from avro_table;

output is :-
avro_table.student_name	avro_table.subject	avro_table.marks
Bhayyasaheb	Maths	91
Bhayyasaheb	Physics	48
Bhayyasaheb	Chemistry	66
Sayali	Maths	96
Sayali	Physics	64
Sayali	Chemistry	73

-----------------------------------------------------------------------------------------------------------------------------------------------

3. Create Another table orc_table

CREATE TABLE orc_table
(
student_name string,
subject string,
marks int
)
STORED AS ORC;

------------------------------------------------------------------------

Load orc_table  with data from csv_table

INSERT OVERWRITE TABLE orc_table SELECT student_name, subject, marks FROM csv_table;

------------------------------------------------------------------------------

actual data in orc_table is ORC format but we are getting in text format in hive terminal
check data in orc_table using following command in hive terminal:-

select * from orc_table;

output is :-
orc_table.student_name	orc_table.subject	orc_table.marks
Bhayyasaheb	Maths	91
Bhayyasaheb	Physics	48
Bhayyasaheb	Chemistry	66
Sayali	Maths	96
Sayali	Physics	64
Sayali	Chemistry	73

--------------------------------------------------------------------------------------------

check data of orc_table using hduser terminal using following terminal:-

hadoop fs -cat /user/hive/warehouse/college.db/orc_table/*	*/

output is:-
ORC
PM
$
"

BhayyasahebSayalifPK
#
"
	ChemistryPhysicsTP/

`��P	F�#BhayyasahebSayali	Ba�	F�p+ChemistryMathsPhysicsN�`������b�``���ь�`�IBL3KHiF�8;�f�l@�H��ř���HK�ř%���8�X@b`��g��q,�L�����N,I��
�b�`
    `���`S��v�H��L,N�HMb
�`X�$���,P&����$��#����,����c�8�3Rs3�K�*��2*�3��%B��|lB	%�
                                     !��;3�Y�p�0X�r�)�sq;e$VV&'f�&	�'V&�dJ��e��b\������%E�B��ř��!@Y>6!���o��9L����"
                                                      (V0��ORC


-------------------------------------------------------------------------------------------------------------------------------------------

4. Create Another table seq_table

CREATE TABLE seq_table
(
student_name string,
subject string,
marks int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS SEQUENCEFILE;

-------------------------------------------------------------

Load seq_table  with data from csv_table

INSERT OVERWRITE TABLE seq_table SELECT student_name, subject, marks FROM csv_table;

---------------------------------------------------------------------------------------

actual data in sequence format in seq_table but we are getting in text format in hiver terminal
check data of seq_table using following command:-

select * from seq_table;

output is:-
seq_table.student_name	seq_table.subject	seq_table.marks
Bhayyasaheb	Maths	91
Bhayyasaheb	Physics	48
Bhayyasaheb	Chemistry	66
Sayali	Maths	96
Sayali	Physics	64
Sayali	Chemistry	73

-----------------------------------------------------------------

check data of seq_table in linux terminal using following command:-

hadoop fs -cat /user/hive/warehouse/college.db/seq_table/*	*/

output is:-
SEQ"org.apache.hadoop.io.BytesWritableorg.apache.hadoop.io.Text_]�aAJ��u�h1w�=7Bhayyasaheb	Maths	91Bhayyasaheb	Physics	48Bhayyasaheb	Chemistry	66Sayali	Maths	96Sayali	Physics	64Sayali	Chemistry	73

--------------------------------------------------------------------------------------------------------------------------------------------------------

show databases;

use niit;

show tables;

#Views:- here  We are actually saving query instead of saving the data 
-----

Q. Create a view  in hive for customers whose age is more than 45 years.

CREATE VIEW age_45plus AS
SELECT * FROM customer WHERE age>45;

select * from age_45plus;
ans:- 5355 records

desc formatted age_45plus;
Table Type:         	VIRTUAL_VIEW  

--------------------------------------------------------------------------

Q. Create a view in hive for top 10 customers.

CREATE VIEW topten AS
select a.custno, b.firstname, b.lastname, b.age, b.profession, round(sum(a.amount),2) as amt 
from txnrecords a, customer b where a.custno = b.custno 
group by a.custno, b.firstname, b.lastname, b.age, b.profession 
order by amt desc limit 10;

select * from topten;

output is:-
topten.custno	topten.firstname	topten.lastname	topten.age	topten.profession	topten.amt
4009485	Stuart	House	58	Teacher	1973.3
4006425	Joe	Burns	30	Economist	1732.09
4000221	Glenda	Boswell	28	Civil engineer	1671.47
4003228	Elsie	Newton	54	Accountant	1640.63
4006606	Jackie	Lewis	66	Recreation and fitness worker	1628.94
4006467	Evelyn	Monroe	37	Financial analyst	1605.95
4004927	Joan	Lowry	30	Librarian	1576.71
4008321	Paul	Carey	64	Human resources assistant	1560.79
4000815	Julie	Galloway	53	Actor	1557.82
4001051	Arlene	Higgins	62	Police officer	1488.67

-------------------------------------------------------------------------------------------------------------------------------------------------------

Q. inserting output of one table into another table ( make sure Airsports table is created beforehand)

create table Airsports:-
-----------------------

CREATE TABLE airsports
(
txnno INT, txndate STRING, custno INT, amount DOUBLE, 
category STRING, product STRING, city STRING, state STRING, spendby STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

Load data in  Airsports table From txnrecords table:-
---------------------------------------------------

INSERT OVERWRITE TABLE airsports select * from txnrecords where category = 'Air Sports';


Select *  from airsports;
ans:-960 records

-------------------------------------------------------------------------------------------------------------------------------------------

Q. find sales done in each payment mode and their percentage:-
--------------------------------------------------------------

CREATE TABLE totalsales 
(total bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

Load data in totalsales table:-
----------------------------
INSERT OVERWRITE TABLE totalsales SELECT sum(amount) FROM txnrecords;


select * from totalsales;
totalsales.total
5110820


SELECT a.spendby, ROUND(SUM(a.amount),2) AS typesales, ROUND((SUM(a.amount)/b.total*100),2) AS salespercent 
FROM txnrecords a, totalsales b GROUP BY a.spendby, b.total;

a.spendby	typesales	salespercent
cash	187685.61	3.67
credit	4923134.93	96.33

-------------------------------------------------------------------------------------------------------------------------------------------------

Q. find sales based on age group with the % on totalsales:-
----------------------------------------------------------

Create table out1:-
------------------

CREATE TABLE out1
(
custno int,
firstname string,
age int,
profession string,
amount double,
product string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

---------------------------------------------------

load data in out1:-
-----------------

INSERT OVERWRITE TABLE out1 
SELECT a.custno, a.firstname, a.age, a.profession, b.amount, b.product 
FROM customer a JOIN txnrecords b ON a.custno = b.custno;

select * from out1;
ans:- 49997 records

------------------------------------------------

Create table out2:-
-----------------

CREATE TABLE out2
(
custno int,
firstname string,
age int,
profession string,
amount double,
product string,
level string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-------------------------------------------------------

Load data in out2:-
-----------------

INSERT OVERWRITE TABLE out2
SELECT * , case 
when age<30 then 'low' 
when age>=30 and age<50 then 'middle' 
when age >=50 then 'old'
else 'others' end
from out1;


select * from out2;
ans:- 49997 records

desc out2;
custno              	int                 	                    
firstname           	string              	                    
age                 	int                 	                    
profession          	string              	                    
amount              	double              	                    
product             	string

---------------------------------------------------

Create table out3:-
-----------------

CREATE TABLE out3
(
level string,
amount double,
salespercent double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-----------------------------------------------------

Load data in out3:-
-----------------

INSERT OVERWRITE TABLE out3
SELECT a.level, ROUND(SUM(a.amount),2) as totalspent, ROUND((SUM(a.amount)/total*100),2) as salespercent
FROM out2 a, totalsales b 
GROUP BY a.level, b.total;


select * from out3;

level	amount		salespercent
low	725221.34	14.19
middle	1855861.67	36.31
old	2529287.65	49.49

----------------------------------------------------------------------------

Q. find sales based on profession group with the % on totalsales:-
------------------------------------------------------------------

Create Table out4:-
-----------------

CREATE TABLE out4
(
profession string,
amount double,
salespercent double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

----------------------------------------------------

Load data in out4:-
-----------------

INSERT OVERWRITE table out4
SELECT a.profession, ROUND(SUM(a.amount),2) as totalspent, ROUND((SUM(a.amount)/total*100),2) as salespercent
FROM out1 a, totalsales b
GROUP BY a.profession, b.total
ORDER BY salespercent desc;

select * from out4 limit 10;

out4.profession	out4.amount	out4.salespercent
Firefighter	116516.99	2.28
Computer support specialist	114138.49	2.23
Librarian	114152.24	2.23
Politician	114030.35	2.23
Human resources assistant	112682.97	2.2
Photographer	111930.18	2.19
Pilot	111549.02	2.18
Police officer	110918.46	2.17
Loan officer	110836.27	2.17
Pharmacist	110087.46	2.15

------------------------------------------------------------------------------------------------------------------------------------------------

Partitioning:- segregating data physically into seperate block with some criteria.

Dynamic Partitioning:- Partitioning data on particular column for all category  
--------------------

Q. Create Partitioned table (Single bucket):-
--------------------------------------------

CREATE TABLE txnrecsByCat
(
txnno INT,
txndate STRING,
custno INT,
amount DOUBLE,
product STRING,
city STRING,
state STRING,
spendby STRING
)
PARTITIONED BY (category STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
-------------------------------------------------

set properties:-
--------------
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

------------------------------------------------------------------------

Q. Loading data into Partition table txnrecsByCat (single bucket):-
------------------------------------------------------------------

FROM txnrecords txn INSERT OVERWRITE TABLE txnrecsByCat PARTITION(category) 
SELECT txn.txnno, txn.txndate, txn.custno, txn.amount, txn.product, txn.city, txn.state, txn.spendby, txn.category 
DISTRIBUTE BY category;


select sum(amount) from txnrecords  where category='Air Sports';
ans:- 99316.89999999994
It is 64 kb file


select sum(amount) from txnrecsbycat where category='Air Sports';
ans:- 99316.90000000005
It is 4.21 Mb file

--------------------------------------------------------------------------------------------------------------------------------------

Q. Create Partitioned table (single bucket) on a derived column:-
----------------------------------------------------------------


CREATE TABLE txnrecsByCat3
(
txnno INT,
txndate STRING,
custno INT,
amount DOUBLE,
category STRING,
product STRING,
city STRING,
state STRING,
spendby STRING
)
PARTITIONED BY (month STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

--------------------------------------------------------------------------------------------------------

Q. Load data into partition table (single bucket) using a derived partition column
-----------------------------------------------------------------------------------

FROM txnrecords txn INSERT OVERWRITE TABLE txnrecsByCat3 PARTITION(month) 
SELECT txn.txnno, txn.txndate, txn.custno, txn.amount, txn.category, txn.product, txn.city, txn.state, txn.spendby,
SUBSTRING(txn.txndate,1,2) as month 
DISTRIBUTE BY SUBSTRING(txndate,1,2);

-------------------------------------------------------------------------------------------------------------------------------------------------

Q. Create partitioned table (single bucket) on multiple columns:-
----------------------------------------------------------------

CREATE TABLE txnrecsByCat4
(
txnno INT,
txndate STRING,
custno INT,
amount DOUBLE,
product STRING,
city STRING,
state STRING
)
PARTITIONED BY (category STRING, spendby STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
--------------------------------------------------------------------------------------

Q. Load data into partition table (single bucket) using multiple partition columns:-
-----------------------------------------------------------------------------------

FROM txnrecords txn INSERT OVERWRITE TABLE txnrecsByCat4 PARTITION(category,spendby) 
SELECT txn.txnno, txn.txndate, txn.custno, txn.amount, txn.product, txn.city, txn.state, txn.category, txn.spendby
DISTRIBUTE BY category, spendby;

--------------------------------------------------------------------------------------------------------------------------------------------------------


Static Partitioning:- Partitioning data on particular column for one specific category
-------------------- - When you want work on particular column then you go for static partitioner

Q. Create a Partition table on category:-
---------------------------------------

CREATE TABLE txnrecsByCat5
(
txnno INT,
txndate STRING,
custno INT,
amount DOUBLE,
product STRING,
city STRING,
state STRING,
spendby STRING
)
PARTITIONED BY (category STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-----------------------------------------------------------

Loading data into Partition table txnrecsByCat5 (single bucket) for a particular category like Air sports, Gymnastics, Team Sports statically:-
----------------------------------------------------------------------------------------------------------------------------------------------

It is for category = 'Gymnastics':-

FROM txnrecords txn INSERT OVERWRITE TABLE txnrecsByCat5 PARTITION(category='Gymnastics') 
SELECT txn.txnno, txn.txndate, txn.custno, txn.amount, txn.product, txn.city, txn.state, txn.spendby
WHERE txn.category='Gymnastics';


It is for category = 'Team Sports':-

FROM txnrecords txn INSERT OVERWRITE TABLE txnrecsByCat5 PARTITION(category='Team Sports') 
SELECT txn.txnno, txn.txndate, txn.custno, txn.amount, txn.product, txn.city, txn.state, txn.spendby
WHERE txn.category='Team Sports';

select * from txnrecsbycat5;
-------------------------------------------------------------------------------------------------------------------------------------------------------

Bucketing :- Bucketting creates different groups & each group contains some data.
--------   - all common hash key falls in that bucket

clustering the data into various groups

select sum(amount) from txnrecords where category = 'Exercise & Fitness' and (state = 'California' or state = 'New Jesery');
 

Q. Create partitioned table (with multiple buckets):-
-----------------------------------------------------

CREATE TABLE txnrecsByCat2
(
txnno INT,
txndate STRING,
custno INT,
amount DOUBLE,
product STRING,
city STRING,
state STRING,
spendby STRING
)
PARTITIONED BY (category STRING)
CLUSTERED BY (state) into 10 buckets
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

---------------------------------------------------------------------------------

set hive.enforce.bucketing=true;

------------------------------------------------------------------------------------


Q. Load data into partition table (with multiple buckets)
----------------------------------------------------------

FROM txnrecords txn INSERT OVERWRITE TABLE txnrecsByCat2 PARTITION(category) 
SELECT txn.txnno, txn.txndate, txn.custno, txn.amount, txn.product, txn.city, txn.state, txn.spendby, txn.category
DISTRIBUTE BY category;

-------------------------------------------------------------------------------------------------------------------------------------------------------

Q. create an index on customer:-
------------------------------

use niit;

show tables;

***deferred rebuild will create an empty index:-

CREATE INDEX prof_index ON TABLE customer(profession) AS 'compact' WITH deferred rebuild;

--------------------------------------------------------------------------------------------

***alter index will actually create the index

ALTER INDEX prof_index ON customer rebuild;

---------------------------------------------------------------------------------------------

******list all the indexes on the table

show indexes on customer;

output is:-
idx_name	tab_name	col_names	idx_tab_name			idx_type
prof_index      customer        profession    niit__customer_prof_index__	compact    

---------------------------------------------------------------------------------------------

*****schema of the index

describe niit__customer_prof_index__;

output is:-
col_name	data_type	
profession       string              	                    
_bucketname      string              	                    
_offsets       array<bigint> 

----------------------------------------------------------------------------------------------

select * from niit__customer_prof_index__ where profession = "Pilot";

output is:-
niit__customer_prof_index__.profession	niit__customer_prof_index__._bucketname		niit__customer_prof_index__._offsets
Pilot					hdfs://localhost:54310/user/training/custs		[0,226,9027,9057,.....]
Pilot					hdfs://localhost:54310/user/training/custs_add		[0]

-----------------------------------------------------------------------------------------------------------------------------------

****Time taken without index
-----------------------------
select profession, count(*) as total from customer group by profession;

list of all the profession and the count of customers

output is:- 
profession	total
	83
Accountant	197
Actor	196
Agricultural and food scientist	195
Architect	202
Artist	175
Athlete	196
Automotive mechanic	193
Carpenter	180
Chemist	206
Childcare worker	207
Civil engineer	193
Coach	199
Computer hardware engineer	204
Computer software engineer	216
Computer support specialist	222
Dancer	178
Designer	204
Doctor	189
Economist	189
Electrical engineer	192
Electrician	194
Engineering technician	204
Environmental scientist	176
Farmer	196
Financial analyst	198
Firefighter	217
Human resources assistant	212
Judge	189
Lawyer	201
Librarian	218
Loan officer	221
Musician	204
Nurse	191
Pharmacist	213
Photographer	222
Physicist	201
Pilot	210
Police officer	209
Politician	227
Psychologist	194
Real estate agent	191
Recreation and fitness worker	210
Reporter	199
Secretary	200
Social worker	212
Statistician	196
Teacher	189
Therapist	187
Veterinarian	208
Writer	95
Time taken: 1.24 seconds, Fetched: 51 row(s)

--------------------------------------------------------------------------

****Time taken with index
--------------------------
select profession, SIZE(`_offsets`) as total from niit__customer_prof_index__;

list of all the profession and the count of customers

output is:-
profession	total
	83
Accountant	197
Actor	196
Agricultural and food scientist	195
Architect	202
Artist	175
Athlete	196
Automotive mechanic	193
Carpenter	180
Chemist	206
Childcare worker	207
Civil engineer	193
Coach	199
Computer hardware engineer	204
Computer software engineer	216
Computer support specialist	222
Dancer	178
Designer	204
Doctor	189
Economist	189
Electrical engineer	192
Electrician	194
Engineering technician	204
Environmental scientist	176
Farmer	196
Financial analyst	198
Firefighter	217
Human resources assistant	212
Judge	189
Lawyer	201
Librarian	218
Loan officer	221
Musician	204
Nurse	191
Pharmacist	213
Photographer	222
Physicist	201
Pilot	209
Pilot	1
Police officer	209
Politician	227
Psychologist	194
Real estate agent	191
Recreation and fitness worker	210
Reporter	199
Secretary	200
Social worker	212
Statistician	196
Teacher	189
Therapist	187
Veterinarian	208
Writer	95
Time taken: 0.047 seconds, Fetched: 52 row(s)

here 52 rows because in external table 1 records coming from custs_add and 51 from custs
-------------------------------------------------------------------------------------------------------------------------------------------------------

















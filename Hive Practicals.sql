
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












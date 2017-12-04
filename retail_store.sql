Retail Store data D01,D02,D11,D12:-
---------------------------------


create database retail;

create table retailstore
(trans_date string,
custId string,
age string,
residence_area string,
category string,
product string,
qty int,
cost bigint,
sales bigint)
row format delimited
fields terminated by '\;'
stored as textfile;

--------------------------------------------
uploading data from local system to hdfs:-

LOAD DATA LOCAL INPATH '/home/hduser/retail/D01' OVERWRITE INTO TABLE retailstore;

LOAD DATA LOCAL INPATH '/home/hduser/retail/D02'  INTO TABLE retailstore;

LOAD DATA LOCAL INPATH '/home/hduser/retail/D11'  INTO TABLE retailstore;

LOAD DATA LOCAL INPATH '/home/hduser/retail/D12'  INTO TABLE retailstore;

------------------------------------------------------------------------------------

find the total number of records in table for all month
select count(*) from retailstore; =  817741

find the total number of records in table for each month
select month(trans_date),count(*) from retailstore group by month(trans_date);
ans:- 
1	216864
2	199039
11	223622
12	178216


select * from retailstore where sales is null or cost is null; = 0

select * from retailstore where sales<= 0 or cost<=0;  = 63


create table retailstore1
(trans_date string,
custId string,
age string,
residence_area string,
category string,
product string,
qty int,
cost bigint,
sales bigint)
row format delimited
fields terminated by '\;'
stored as textfile;


inserting data in retailstore1 table from retailstore table
in that adding those values for sales and cost whose sales is +ve  & cost is +ve using following command

insert overwrite table retailstore1 
select * from retailstore where sales>0 and cost>0;

select count(*) from retailstore1; = 817678

select * from retailstore1 where sales<= 0 or cost<=0; =0

------------------------------------------------------------------------------------------------------------------------------------------------------



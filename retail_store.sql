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


(A1)Find out the customer I.D for the customer and the date of transaction who has spent the maximum amount in a month and in all the 4 months. 
Answer would be - total 5 customer IDs
1) One for each month
2) One for all the 4 months.


Ans:- For A1 From 4 Months Retail data:-

INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/HighTransIn4Months' row format delimited fields terminated by ',' 
select custId,sales,trans_date from retailstore1 where sales in (select max(sales) from retailstore1);
ans:- 
custid		sales	date_time
01622362  	444000	2001-02-17 00:00:00

INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/HighTransInJan' row format delimited fields terminated by ',' 
select custId,sales,trans_date from retailstore1 a where  month(a.trans_date) = 1 and sales in (select max(sales) from retailstore1 b where month(b.trans_date) = 1);
ans:- 
custId	  	sales 	date_time
01062489,	45554,	2001-01-03 00:00:00

INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/HighTransInFeb' row format delimited fields terminated by ',' 
select custId,sales,trans_date from retailstore1 a where  month(a.trans_date) = 2 and sales in (select max(sales) from retailstore1 b where month(b.trans_date) = 2);
ans:- 
custId	  	sales 	date_time
01622362,	444000,	2001-02-17 00:00:00

INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/HighTransInNov' row format delimited fields terminated by ',' 
select custId,sales,trans_date from retailstore1 a where  month(a.trans_date) = 11 and sales in (select max(sales) from retailstore1 b where month(b.trans_date) = 11);
ans:- 
custId	  	sales 	date_time
02119083,	62688,	2000-11-28 00:00:00

INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/HighTransInDec' row format delimited fields terminated by ',' 
select custId,sales,trans_date from retailstore1 a where  month(a.trans_date) = 12 and sales in (select max(sales) from retailstore1 b where month(b.trans_date) = 12);
ans:- 	
custId	  	sales 	date_time
02131221,	70589,	2000-12-27 00:00:00
02134819,	70589,	2000-12-27 00:00:00

------------------------------------------------------------------------------------------------------------------------------------------------

Que:- (A2)Find total gross profit made by each product and also by each category for all the 4 months data.
Ans:- 
INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/GrossProfitByProduct' row format delimited fields terminated by ',' 
select product, sum(sales - cost) as profit from retailstore1 group by product order by profit desc limit 5;
ans:- 
productid	profit
4909978112950	71312
8712045008539	46586
20564100     	38699
4710628131012	34429
0729238191921	33645

INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/GrossProfitByCategory' row format delimited fields terminated by ',' 
select category, sum(sales - cost) as profit from retailstore1 group by category order by profit desc limit 5;
ans:- 
category	profit
320402		356563
560402		340999
560201		320217
100205		201537
530101		184621

------------------------------------------------------------------------------------------------------------------------------------------------

Que:- (A3)Find total gross profit % made by each product and also by each category for all the 4 months data.
Ans:- 
INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/GrossProfitPercentByProduct' row format delimited fields terminated by ','
select product, round((sum(sales) -sum(cost))/sum(cost)*100,2) as margin from retailstore1 group by product order by margin desc limit 5; 
ans:- 
productid	margin
20562687,    	3733.33
4714298810208,	279.33
4714298808236,	197.73
4711821100584,	160.91
20454388,     	142.5

INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/GrossProfitPercentByCategory' row format delimited fields terminated by ',' 
select category, round((sum(sales) -sum(cost))/sum(cost)*100,2) as margin from retailstore1 group by category order by margin desc limit 5;
ans:- 
category	margin
760112,		116.62
590106,		94.78
751207,		93.2
590105,		87.66
520423,		84.43
----------------------------------------------------------------------------------------------------------------------------------------------------

Que:- B)Find out the top 4 or top 10 product being sold in the monthly basis and in all the 4 months.. Criteria for top should be sales amount.
Ans:- 
INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/Top10ProductIn4Months' row format delimited fields terminated by ',' 
select product, sum(sales) as amount from retailstore1 group by product order by amount desc limit 10;
ans:- 
productid	amount
8712045008539,	1540503
4710628131012,	675112
4710114128038,	514601
4711588210441,	491292
20553418,     	470501
4710628119010,	433380
4909978112950,	432596
8712045000151,	428530
7610053910787,	392581
4719090900065,	385626

INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/Top10ProductInJan' row format delimited fields terminated by ',' 
select product, sum(sales) as amount from retailstore1 where month(trans_date) = 1 group by product order by amount desc limit 10;
ans:-
productid	amount
8712045008539,	611874
4710628119010,	278230
4710628131012,	227840
4719090900065,	225456
4710174053691,	180273
0300086780026,	179569
4909978112950,	178544
4712425010712,	138601
4710265849066,	132550
4710043552102,	118297

INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/Top10ProductInFeb' row format delimited fields terminated by ',' 
select product, sum(sales) as amount from retailstore1 where month(trans_date) = 2 group by product order by amount desc limit 10;
ans:-
productid	amount
4711588210441,	444000
0022972004664,	213803
4710036003581,	210339
4710265849066,	194669
4710114128038,	147813
4710114362029,	130475
4719864060056,	124580
4710036008562,	122794
4712162000038,	122762
4710114105046,	116787

INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/Top10ProductInNov' row format delimited fields terminated by ',' 
select product, sum(sales) as amount from retailstore1 where month(trans_date) = 11 group by product order by amount desc limit 10;
ans:-
productid	amount
20553418,     	470501
8712045008539,	460282
4902430493437,	271542
4710628131012,	188149
4710114128038,	187895
4719090900058,	178385
4710291112172,	153023
4710908131589,	150204
4902430040334,	125583
4909978112950,	117920

INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/Top10ProductInDec' row format delimited fields terminated by ',' 
select product, sum(sales) as amount from retailstore1 where month(trans_date) = 12 group by product order by amount desc limit 10;
ans:-
productid	amount
8712045008539,	442482
8712045000151,	298013
8712045011317,	252688
7610053910787,	233383
4710628131012,	207681
7610053910794,	200086
4710291112172,	110881
4902430493437,	108414
4710114128038,	89971
4719090900065,	85935

------------------------------------------------------------------------------------------------------------------------------------------------
Que:- C1)Find out the (top 5*) viable products and the (top 5*) product subclass for the age group A, B, C etc..... Data should be taken for all the 4 months
Ans:-
Top5ViableProductAgeWise:-
------------------------

create table table_VPA (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPA
select product,age,(SUM(sales)-SUM(cost)) as profit from retailstore1  group by product,age having trim(age)="A"  order by profit desc limit 5;

create table table_VPB (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPB
select product,age,(SUM(sales)-SUM(cost)) as profit from retailstore1  group by product,age having trim(age)="B"  order by profit desc limit 5;


create table table_VPC (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPC
select product,age,(SUM(sales)-SUM(cost)) as profit from retailstore1  group by product,age having trim(age)="C"  order by profit desc limit 5;

create table table_VPD (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPD
select product,age,(SUM(sales)-SUM(cost)) as profit from retailstore1  group by product,age having trim(age)="D"  order by profit desc limit 5;

create table table_VPE (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPE
select product,age,(SUM(sales)-SUM(cost)) as profit from retailstore1  group by product,age having trim(age)="E"  order by profit desc limit 5;

create table table_VPF (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPF
select product,age,(SUM(sales)-SUM(cost)) as profit from retailstore1  group by product,age having trim(age)="F"  order by profit desc limit 5;

create table table_VPG (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPG
select product,age,(SUM(sales)-SUM(cost)) as profit from retailstore1  group by product,age having trim(age)="G"  order by profit desc limit 5;

create table table_VPH (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPH
select product,age,(SUM(sales)-SUM(cost)) as profit from retailstore1  group by product,age having trim(age)="H"  order by profit desc limit 5;

create table table_VPI (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPI
select product,age,(SUM(sales)-SUM(cost)) as profit from retailstore1  group by product,age having trim(age)="I"  order by profit desc limit 5;

create table table_VPJ (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPJ
select product,age,(SUM(sales)-SUM(cost)) as profit from retailstore1  group by product,age having trim(age)="J"  order by profit desc limit 5;

create table table_VPK (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPK
select product,age,(SUM(sales)-SUM(cost)) as profit from retailstore1  group by product,age having trim(age)="K"  order by profit desc limit 5;


INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/Top5ViableProductAgeWise' row format delimited fields terminated by ',' 
select * from(
select product,age,totalsales from table_VPA
UNION
select product,age,totalsales from table_VPB
UNION
select product,age,totalsales from table_VPC
UNION
select product,age,totalsales from table_VPD
UNION
select product,age,totalsales from table_VPE
UNION
select product,age,totalsales from table_VPF
UNION
select product,age,totalsales from table_VPG
UNION
select product,age,totalsales from table_VPH
UNION
select product,age,totalsales from table_VPI
UNION
select product,age,totalsales from table_VPJ
UNION
select product,age,totalsales from table_VPK) top5viableproduct
order by age,totalsales desc; 

ans:- 

top5viableproduct.product	top5viableproduct.age	top5viableproduct.totalsales
4711588210441	A 	12025
20559045     	A 	3290
4973167032060	A 	2163
4973167738757	A 	1854
20556433     	A 	1776
8712045008539	B 	7318
4710628119010	B 	6827
4902430493437	B 	6419
7610053910787	B 	6344
8712045011317	B 	6072
8712045008539	C 	10153
0729238191921	C 	7840
4909978112950	C 	7386
20564100     	C 	6530
4902430040334	C 	6528
4909978112950	D 	17612
8712045008539	D 	15155
4710628131012	D 	10462
0729238191921	D 	9905
4902430493437	D 	8735
4909978112950	E 	14628
4710628131012	E 	7810
4901422038939	E 	7317
20564100     	E 	7008
4710114128038	E 	6863
4909978112950	F 	10276
20556433     	F 	6388
20564100     	F 	4770
4901422038939	F 	4301
4710114128038	F 	4246
4909978112950	G 	9370
0729238191921	G 	4190
4710114128038	G 	2704
4711713491530	G 	2478
4710043552102	G 	2365
8712045011317	H 	4706
0300086780026	H 	3254
4909978112950	H 	3148
20421151     	H 	3135
7610053910794	H 	2037
8712045011317	I 	2291
4909978112950	I 	2142
8712045000151	I 	2096
4710628119010	I 	1890
4710043552102	I 	1501
4710043552102	J 	1460
0041736007284	J 	1459
4710960918036	J 	1377
4902430493437	J 	1312
4712603669091	J 	1284
20564100     	K 	2340
4902430493437	K 	2172
4711863590077	K 	1980
20563745     	K 	1583
20456245     	K 	1577

-----------------------------------------------------------------------------------------------------------------------------------------------

Top5ViableCategoryAgeWise:-
-------------------------

create table table_VCA (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCA
select category,age,(sum(sales)-sum(cost)) as profit from retailstore1  group by age,category having trim(age)="A" order by profit desc limit 5;

create table table_VCB (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCB
select category,age,(sum(sales)-sum(cost)) as profit from retailstore1  group by age,category having trim(age)="B" order by profit desc limit 5;

create table table_VCC (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCC
select category,age,(sum(sales)-sum(cost)) as profit from retailstore1  group by age,category having trim(age)="C" order by profit desc limit 5;

create table table_VCD (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCD
select category,age,(sum(sales)-sum(cost)) as profit from retailstore1  group by age,category having trim(age)="D" order by profit desc limit 5;

create table table_VCE (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCE
select category,age,(sum(sales)-sum(cost)) as profit from retailstore1  group by age,category having trim(age)="E" order by profit desc limit 5;

create table table_VCF (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCF
select category,age,(sum(sales)-sum(cost)) as profit from retailstore1  group by age,category having trim(age)="F" order by profit desc limit 5;

create table table_VCG (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCG
select category,age,(sum(sales)-sum(cost)) as profit from retailstore1  group by age,category having trim(age)="G" order by profit desc limit 5;

create table table_VCH (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCH
select category,age,(sum(sales)-sum(cost)) as profit from retailstore1  group by age,category having trim(age)="H" order by profit desc limit 5;

create table table_VCI (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCI
select category,age,(sum(sales)-sum(cost)) as profit from retailstore1  group by age,category having trim(age)="I" order by profit desc limit 5;

create table table_VCJ (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCJ
select category,age,(sum(sales)-sum(cost)) as profit from retailstore1  group by age,category having trim(age)="J" order by profit desc limit 5;

create table table_VCK (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCK
select category,age,(sum(sales)-sum(cost)) as profit from retailstore1  group by age,category having trim(age)="K" order by profit desc limit 5;


INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/Top5ViableCategoryAgeWise' row format delimited fields terminated by ',' 
select * from(
select category,age,totalsales from table_VCA
UNION
select category,age,totalsales from table_VCB
UNION
select category,age,totalsales from table_VCC
UNION
select category,age,totalsales from table_VCD
UNION
select category,age,totalsales from table_VCE
UNION
select category,age,totalsales from table_VCF
UNION
select category,age,totalsales from table_VCG
UNION
select category,age,totalsales from table_VCH
UNION
select category,age,totalsales from table_VCI
UNION
select category,age,totalsales from table_VCJ
UNION
select category,age,totalsales from table_VCK) top5viablepcategory
order by age,totalsales desc; 

ans:-

top5viablepcategory.category	top5viablepcategory.age	top5viablepcategory.totalsales
320402	A 	17133
100516	A 	12863
530101	A 	8923
560402	A 	7992
560201	A 	7613
560402	B 	52282
560201	B 	50066
320402	B 	39799
530101	B 	16646
100205	B 	15262
560201	C 	95169
560402	C 	75138
320402	C 	50696
100205	C 	36280
470105	C 	31025
560402	D 	102704
560201	D 	85341
320402	D 	63010
100205	D 	47211
530101	D 	41139
320402	E 	70820
560402	E 	40781
530101	E 	39274
100205	E 	37936
530110	E 	33054
320402	F 	55540
530101	F 	28190
100205	F 	24074
530110	F 	20874
560402	F 	20820
320402	G 	18486
470103	G 	18135
500804	G 	13860
100205	G 	11962
520457	G 	11542
560402	H 	12319
320402	H 	10255
560201	H 	7588
500804	H 	5780
520457	H 	5493
560402	I 	10406
560201	I 	7085
320402	I 	6120
520457	I 	6018
110333	I 	6008
100401	J 	8023
100205	J 	6990
560201	J 	4973
130206	J 	4709
100505	J 	4507
320402	K 	22023
560201	K 	7130
320501	K 	6203
470105	K 	5810
560402	K 	5732

-----------------------------------------------------------------------------------------------------------------------------------------------

Que:- (C2)Find out the (top 5*) loss making products and the (top 5*) loss making product subclass for the age group A, B, C etc..... Data should be taken for all the 4 months
Ans:-

Top5LossMakingProductAgeWise:-
-----------------------------

create table table_LPA (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPA
select product,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,product having trim(age)="A" order by loss desc limit 5;


create table table_LPB (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPB
select product,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,product having trim(age)="B" order by loss desc limit 5;

create table table_LPC (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPC
select product,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,product having trim(age)="C" order by loss desc limit 5;

create table table_LPD (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPD
select product,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,product having trim(age)="D" order by loss desc limit 5;

create table table_LPE (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPE
select product,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,product having trim(age)="E" order by loss desc limit 5;

create table table_LPF (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPF
select product,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,product having trim(age)="F" order by loss desc limit 5;

create table table_LPG (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPG
select product,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,product having trim(age)="G" order by loss desc limit 5;

create table table_LPH (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPH
select product,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,product having trim(age)="H" order by loss desc limit 5;

create table table_LPI (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPI
select product,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,product having trim(age)="I" order by loss desc limit 5;

create table table_LPJ (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPJ
select product,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,product having trim(age)="J" order by loss desc limit 5;

create table table_LPK (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPK
select product,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,product having trim(age)="K" order by loss desc limit 5;


INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/Top5LossProductAgeWise' row format delimited fields terminated by ',' 
select * from(
select product,age,totalsales from table_LPA
UNION
select product,age,totalsales from table_LPB
UNION
select product,age,totalsales from table_LPC
UNION
select product,age,totalsales from table_LPD
UNION
select product,age,totalsales from table_LPE
UNION
select product,age,totalsales from table_LPF
UNION
select product,age,totalsales from table_LPG
UNION
select product,age,totalsales from table_LPH
UNION
select product,age,totalsales from table_LPI
UNION
select product,age,totalsales from table_LPJ
UNION
select product,age,totalsales from table_LPK) top5lossproduct
order by age,totalsales desc; 

ans:-

top5lossproduct.product	top5lossproduct.age	top5lossproduct.totalsales
4714981010038	A 	6947
4711271000014	A 	2194
4710265849066	A 	1875
4719090900065	A 	1845
4712425010712	A 	986
4714981010038	B 	9237
4711271000014	B 	3425
4719090900065	B 	2554
4710265849066	B 	2080
4712425010712	B 	1490
4714981010038	C 	17215
4711271000014	C 	6008
4719090900065	C 	5867
4710265849066	C 	4966
4710908110232	C 	3002
4714981010038	D 	23550
4711271000014	D 	8739
4719090900065	D 	7248
4710265849066	D 	6406
2110119000377	D 	5819
4714981010038	E 	21157
4719090900065	E 	8667
4711271000014	E 	7989
4710265849066	E 	6645
4712425010712	E 	2917
4714981010038	F 	17456
4719090900065	F 	6283
4711271000014	F 	6120
4710265849066	F 	5460
4710265847666	F 	2315
2110119000377	G 	11638
4714981010038	G 	10290
4711271000014	G 	4185
4719090900065	G 	3559
4710265849066	G 	3335
4714981010038	H 	5812
4719090900065	H 	2455
4711271000014	H 	2132
4710265849066	H 	1482
4712425010712	H 	796
4714981010038	I 	5354
4710265849066	I 	2114
4711271000014	I 	2038
4719090900065	I 	1926
4719090900058	I 	1084
4714981010038	J 	10196
4710265849066	J 	3388
4719090900065	J 	3180
4711271000014	J 	2703
4710060000099	J 	1180
4714981010038	K 	3788
4710265849066	K 	1218
4710683100015	K 	852
4719090900065	K 	747
4711271000014	K 	680

-----------------------------------------------------------------------------------------------------------------------------------------------

Top5LossMakingCategoryAgeWise:-
-----------------------------

create table table_LCA (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCA
select category,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,category having trim(age)="A" order by loss desc limit 5;

create table table_LCB (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCB
select category,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,category having trim(age)="B" order by loss desc limit 5;

create table table_LCC (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCC
select category,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,category having trim(age)="C" order by loss desc limit 5;

create table table_LCD (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCD
select category,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,category having trim(age)="D" order by loss desc limit 5;

create table table_LCE (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCE
select category,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,category having trim(age)="E" order by loss desc limit 5;

create table table_LCF (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCF
select category,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,category having trim(age)="F" order by loss desc limit 5;

create table table_LCG (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCG
select category,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,category having trim(age)="G" order by loss desc limit 5;

create table table_LCH (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCH
select category,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,category having trim(age)="H" order by loss desc limit 5;

create table table_LCI (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCI
select category,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,category having trim(age)="I" order by loss desc limit 5;

create table table_LCJ (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCJ
select category,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,category having trim(age)="J" order by loss desc limit 5;

create table table_LCK (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCK
select category,age,(sum(cost)-sum(sales)) as loss from retailstore1 group by age,category having trim(age)="K" order by loss desc limit 5;


INSERT OVERWRITE DIRECTORY '/hive/RetailStore1/Top5LossCategoryAgeWise' row format delimited fields terminated by ',' 
select * from(
select category,age,totalsales from table_LCA
UNION
select category,age,totalsales from table_LCB
UNION
select category,age,totalsales from table_LCC
UNION
select category,age,totalsales from table_LCD
UNION
select category,age,totalsales from table_LCE
UNION
select category,age,totalsales from table_LCF
UNION
select category,age,totalsales from table_LCG
UNION
select category,age,totalsales from table_LCH
UNION
select category,age,totalsales from table_LCI
UNION
select category,age,totalsales from table_LCJ
UNION
select category,age,totalsales from table_LCK) top5losscategory
order by age,totalsales desc; 

ans:-

top5losspcategory.category	top5losspcategory.age	top5losspcategory.totalsales

130315	A 	6797
110217	A 	3598
110106	A 	1968
720507	A 	623
500202	A 	417
130315	B 	8691
110217	B 	4870
110106	B 	2467
530411	B 	68
780102	B 	16
130315	C 	16124
110217	C 	11841
110106	C 	4141
530411	C 	885
340101	C 	682
130315	D 	21697
110217	D 	12562
110106	D 	5784
711409	D 	4859
340101	D 	538
130315	E 	19254
110217	E 	16222
110106	E 	5283
713901	E 	1224
340101	E 	1192
130315	F 	16523
110217	F 	12510
110106	F 	4413
340101	F 	702
710703	F 	309
711409	G 	11102
130315	G 	9752
110217	G 	8434
110106	G 	3189
720507	G 	895
130315	H 	5462
110217	H 	4294
110106	H 	1513
711501	H 	400
100608	H 	137
130315	I 	4883
110217	I 	4609
110106	I 	1548
714601	I 	203
720507	I 	161
130315	J 	9922
110217	J 	6409
110106	J 	2031
530407	J 	849
340101	J 	384
130315	K 	3527
110217	K 	1874
110106	K 	409
110110	K 	155
340101	K 	48

-----------------------------------------------------------------------------------------------------------------------------------------------



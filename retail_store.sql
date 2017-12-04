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
select product,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,product having trim(age)="A" order by totalsales desc limit 5;

create table table_VPB (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPB
select product,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,product having trim(age)="B" order by totalsales desc limit 5;

create table table_VPC (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPC
select product,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,product having trim(age)="C" order by totalsales desc limit 5;

create table table_VPD (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPD
select product,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,product having trim(age)="D" order by totalsales desc limit 5;

create table table_VPE (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPE
select product,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,product having trim(age)="E" order by totalsales desc limit 5;

create table table_VPF (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPF
select product,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,product having trim(age)="F" order by totalsales desc limit 5;

create table table_VPG (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPG
select product,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,product having trim(age)="G" order by totalsales desc limit 5;

create table table_VPH (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPH
select product,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,product having trim(age)="H" order by totalsales desc limit 5;

create table table_VPI (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPI
select product,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,product having trim(age)="I" order by totalsales desc limit 5;

create table table_VPJ (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VPJ
select product,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,product having trim(age)="J" order by totalsales desc limit 5;


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
select product,age,totalsales from table_VPJ) top5viableproduct
order by age,totalsales desc; 

ans:- 

top5viableproduct.product	top5viableproduct.age	top5viableproduct.totalsales
4711588210441,A ,12025
20559045     ,A ,3290
4973167032060,A ,2163
4973167738757,A ,1854
20556433     ,A ,1776
8712045008539,B ,8452
4710628119010,B ,6827
7610053910787,B ,6696
4902430493437,B ,6419
4710628131012,B ,6364
8712045008539,C ,12754
4710054380619,C ,8912
4902430040334,C ,8617
0729238191921,C ,7840
4710628131012,C ,7694
8712045008539,D ,18630
4909978112950,D ,18612
4710628131012,D ,11523
0729238191921,D ,10105
20564100     ,D ,9445
4909978112950,E ,14628
4710628131012,E ,8226
4901422038939,E ,7347
20564100     ,E ,7008
4710114128038,E ,6923
4909978112950,F ,11376
20556433     ,F ,6388
20564100     ,F ,4770
4901422038939,F ,4348
4710114128038,F ,4289
4909978112950,G ,9770
0729238191921,G ,4190
4710114128038,G ,2769
4710043552102,G ,2691
4711713491530,G ,2574
8712045011317,H ,4706
4909978112950,H ,3348
0300086780026,H ,3254
20421151     ,H ,3135
20568399     ,H ,2200
8712045011317,I ,2291
8712045000151,I ,2226
4909978112950,I ,2142
4710628119010,I ,1890
4710043552102,I ,1553
4710043552102,J ,1563
0041736007284,J ,1489
4710960918036,J ,1377
4710421019081,J ,1343
4902430493437,J ,1312

-----------------------------------------------------------------------------------------------------------------------------------------------

Top5ViableCategoryAgeWise:-
-------------------------

create table table_VCA (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCA
select category,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,category having trim(age)="A" order by totalsales desc limit 5;

create table table_VCB (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCB
select category,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,category having trim(age)="B" order by totalsales desc limit 5;

create table table_VCC (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCC
select category,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,category having trim(age)="C" order by totalsales desc limit 5;

create table table_VCD (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCD
select category,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,category having trim(age)="D" order by totalsales desc limit 5;

create table table_VCE (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCE
select category,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,category having trim(age)="E" order by totalsales desc limit 5;

create table table_VCF (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCF
select category,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,category having trim(age)="F" order by totalsales desc limit 5;

create table table_VCG (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCG
select category,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,category having trim(age)="G" order by totalsales desc limit 5;

create table table_VCH (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCH
select category,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,category having trim(age)="H" order by totalsales desc limit 5;

create table table_VCI (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCI
select category,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,category having trim(age)="I" order by totalsales desc limit 5;

create table table_VCJ (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_VCJ
select category,age,sum(sales-cost) as totalsales from retailstore1 where (sales-cost)>0 group by age,category having trim(age)="J" order by totalsales desc limit 5;


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
select category,age,totalsales from table_VCJ) top5viablepcategory
order by age,totalsales desc; 

ans:-

top5viablepcategory.category	top5viablepcategory.age	top5viablepcategory.totalsales
320402,A ,17237
100516,A ,12915
560201,A ,9121
530101,A ,8980
530110,A ,8285
560201,B ,56890
560402,B ,56452
320402,B ,39799
530110,B ,17218
530101,B ,16985
560201,C ,112984
560402,C ,89068
320402,C ,50848
100205,C ,37696
470105,C ,33337
560402,D ,116159
560201,D ,101511
320402,D ,63861
100205,D ,48803
530101,D ,41792
320402,E ,71144
560402,E ,43932
530101,E ,40076
100205,E ,39193
530110,E ,36983
320402,F ,55840
530101,F ,28950
100205,F ,25272
530110,F ,24536
560402,F ,21670
470103,G ,18535
320402,G ,18486
500804,G ,14641
100205,G ,12614
520457,G ,12198
560402,H ,12788
320402,H ,10919
560201,H ,9257
500804,H ,5863
520457,H ,5851
560402,I ,10751
560201,I ,8582
520457,I ,6288
320402,I ,6120
110333,I ,6008
100401,J ,8930
100205,J ,7351
560201,J ,5679
110117,J ,5127
130206,J ,4771

-----------------------------------------------------------------------------------------------------------------------------------------------

Que:- (C2)Find out the (top 5*) loss making products and the (top 5*) loss making product subclass for the age group A, B, C etc..... Data should be taken for all the 4 months
Ans:-

Top5LossMakingProductAgeWise:-
-----------------------------

create table table_LPA (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPA
select product,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,product having trim(age)="A" order by totalsales desc limit 5;

create table table_LPB (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPB
select product,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,product having trim(age)="B" order by totalsales desc limit 5;

create table table_LPC (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPC
select product,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,product having trim(age)="C" order by totalsales desc limit 5;

create table table_LPD (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPD
select product,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,product having trim(age)="D" order by totalsales desc limit 5;

create table table_LPE (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPE
select product,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,product having trim(age)="E" order by totalsales desc limit 5;

create table table_LPF (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPF
select product,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,product having trim(age)="F" order by totalsales desc limit 5;

create table table_LPG (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPG
select product,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,product having trim(age)="G" order by totalsales desc limit 5;

create table table_LPH (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPH
select product,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,product having trim(age)="H" order by totalsales desc limit 5;

create table table_LPI (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPI
select product,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,product having trim(age)="I" order by totalsales desc limit 5;

create table table_LPJ (product string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LPJ
select product,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,product having trim(age)="J" order by totalsales desc limit 5;


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
select product,age,totalsales from table_LPJ) top5lossproduct
order by age,totalsales desc; 

ans:-

top5lossproduct.product	top5lossproduct.age	top5lossproduct.totalsales
4714981010038,A ,6953
4711271000014,A ,2252
4719090900065,A ,2006
4710265849066,A ,1988
4712425010712,A ,1236
4714981010038,B ,9283
4711271000014,B ,3561
4719090900065,B ,3213
4710265849066,B ,2409
4712425010712,B ,2073
4714981010038,C ,17339
4719090900065,C ,6649
4711271000014,C ,6385
4710265849066,C ,5510
4712425010712,C ,3784
4714981010038,D ,23681
4711271000014,D ,9146
4719090900065,D ,8544
4710265849066,D ,7602
2110119000377,D ,5819
4714981010038,E ,21333
4719090900065,E ,9792
4711271000014,E ,8361
4710265849066,E ,7377
4712425010712,E ,4247
4714981010038,F ,17569
4719090900065,F ,7448
4711271000014,F ,6427
4710265849066,F ,6047
4712425010712,F ,3150
2110119000377,G ,11638
4714981010038,G ,10331
4711271000014,G ,4411
4719090900065,G ,4393
4710265849066,G ,3634
4714981010038,H ,5825
4719090900065,H ,2762
4711271000014,H ,2229
4710265849066,H ,1646
4719090900058,H ,879
4714981010038,I ,5380
4710265849066,I ,2408
4719090900065,I ,2222
4711271000014,I ,2157
4719090900058,I ,1354
4714981010038,J ,10229
4710265849066,J ,3987
4719090900065,J ,3842
4711271000014,J ,2822
4710363352000,J ,1476

-----------------------------------------------------------------------------------------------------------------------------------------------

Top5LossMakingCategoryAgeWise:-
-----------------------------

create table table_LCA (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCA
select category,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,category having trim(age)="A" order by totalsales desc limit 5;

create table table_LCB (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCB
select category,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,category having trim(age)="B" order by totalsales desc limit 5;

create table table_LCC (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCC
select category,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,category having trim(age)="C" order by totalsales desc limit 5;

create table table_LCD (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCD
select category,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,category having trim(age)="D" order by totalsales desc limit 5;

create table table_LCE (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCE
select category,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,category having trim(age)="E" order by totalsales desc limit 5;

create table table_LCF (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCF
select category,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,category having trim(age)="F" order by totalsales desc limit 5;

create table table_LCG (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCG
select category,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,category having trim(age)="G" order by totalsales desc limit 5;

create table table_LCH (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCH
select category,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,category having trim(age)="H" order by totalsales desc limit 5;

create table table_LCI (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCI
select category,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,category having trim(age)="I" order by totalsales desc limit 5;

create table table_LCJ (category string, age string, totalsales bigint) 
row format delimited
fields terminated by ',';

insert overwrite table table_LCJ
select category,age,sum(cost-sales) as totalsales from retailstore1 where (cost-sales)>0 group by age,category having trim(age)="J" order by totalsales desc limit 5;


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
select category,age,totalsales from table_LCJ) top5losspcategory
order by age,totalsales desc; 

ans:-

top5losspcategory.category	top5losspcategory.age	top5losspcategory.totalsales
130315,A ,7041
110217,A ,5242
110117,A ,2256
110106,A ,2252
500201,A ,2126
130315,B ,9583
110217,B ,8750
560201,B ,6824
500201,B ,5094
560402,B ,4170
110217,C ,19026
130315,C ,17898
560201,C ,17815
560402,C ,13930
500201,C ,8253
110217,D ,26170
130315,D ,24274
560201,D ,16170
560402,D ,13455
500201,D ,11077
110217,E ,28835
130315,E ,21854
110117,E ,9690
500201,E ,9417
110106,E ,8423
110217,F ,22647
130315,F ,18144
110117,F ,7609
500201,F ,6443
110106,F ,6439
110217,G ,13819
711409,G ,11638
130315,G ,10692
110117,G ,5407
110106,G ,4485
110217,H ,7561
130315,H ,5924
110117,H ,2880
110106,H ,2229
500201,H ,1891
110217,I ,7813
130315,I ,5513
110117,I ,2478
110106,I ,2157
560201,I ,1497
110217,J ,10823
130315,J ,10489
110117,J ,3625
110106,J ,2822
500201,J ,1891

-----------------------------------------------------------------------------------------------------------------------------------------------



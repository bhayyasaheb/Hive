
Create tabele without header:-
----------------------------

CREATE TABLE Airlines
(
year int,
quarter int,
avg_revenue double,
seat_booked bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");


Load data in table from local system:-
-------------------------------------
LOAD DATA LOCAL INPATH '/home/hduser/airlines.csv' OVERWRITE INTO TABLE Airlines;


select * from airlines; ans:- 84 rows

select count(*) from airlines; ans:- 84 row

select distinct(year) from airlines; ans:-21 row

------------------------------------------------------------------------------------------

Q. Find the total sales/revenue done in each year.

select year,cast(sum(avg_revenue*seat_booked) as bigint) as totalsales from airlines group by year;

output is:-
year	totalsales
1995	43494243
1996	46358778
1997	45385236
1998	42035717
1999	48757714
2000	52342926
2001	55533779
2002	47499146
2003	49273210
2004	50631364
2005	46376786
2006	50437898
2007	57309216
2008	57653170
2009	46746446
2010	54861521
2011	51888286
2012	62199127
2013	66363208
2014	62624175
2015	62378990

-------------------------------------------------------------------------------------------------------------------------------------------------------

Q.Find the total sales in each year in Million.

select year,round(sum(avg_revenue*seat_booked)/1000000, 2)  as totalsales from airlines group by year;

output is:-
year	totalsales
1995	43.49
1996	46.36
1997	45.39
1998	42.04
1999	48.76
2000	52.34
2001	55.53
2002	47.5
2003	49.27
2004	50.63
2005	46.38
2006	50.44
2007	57.31
2008	57.65
2009	46.75
2010	54.86
2011	51.89
2012	62.2
2013	66.36
2014	62.62
2015	62.38

-------------------------------------------------------------------------------------------------------------------------------------------------------

Find Growth of sales on each year

growth = (next year sales -first year sales)/first year sales *100

year 	sales
1995	43.49
1996	46.36

like growth for 1996 is

growth of 1996 = (46.36-43.49)/43.49*100
     	  1996 = 6.59

like this for all 21 years

Year	Growth
1995	
1996	6.59
1997	-2.1
1998	-7.38
1999	15.99
2000	7.35
2001	6.1
2002	-14.47
2003	3.73
2004	2.76
2005	-8.4
2006	8.76
2007	13.62
2008	0.6
2009	-18.92
2010	17.36
2011	-5.42
2012	19.87
2013	6.69
2014	-5.64
2015	-0.39


-------------------------------------------------------------------------------------------------------------------------------------------------------

Q. Find Number of passengers travlled in each year.

select year,sum(total_seat_booked) as totalseats from airlines group by year;

output is:-
year	totalseats
1995	148520
1996	167223
1997	157972
1998	135678
1999	150000
2000	154376
2001	173598
2002	152195
2003	156153
2004	164800
2005	150610
2006	153789
2007	176299
2008	166897
2009	150308
2010	163741
2011	142647
2012	166076
2013	173676
2014	159823
2015	165438
-------------------------------------------------------------------------------------------------------------------------------------------------------

Q. Find Growth of Passenger on each year

growth = (next year passenger -first year passenger)/first year passenger *100

year 	sales
1995	148520
1996	167223

like growth for 1996 is

growth of 1996 = (167223-148520)/148520*100
     	  1996 = 12.59

like this for all 21 years

Year 	Growth 
1995	
1996	12.59
1997	-5.53
1998	-14.11
1999	1.56
2000	2.92
2001	12.45
2002	-12.33
2003	2.6
2004	5.54
2005	-8.61
2006	2.11
2007	14.64
2008	-5.33
2009	-9.94
2010	8.94
2011	-12.88
2012	16.42
2013	4.58
2014	-7.98
2015	3.51


------------------------------------------------------------------------------------------------------------------------------------------------------

Q. Find Maximum revenue/sales on each quarter.

select quarter,cast(max(avg_revenue*seat_booked) as bigint) as maxsales from airlines group by quarter;

quarter	maxsales
1	18572613
2	17316167
3	18177814
4	18819408
-------------------------------------------------------------------------------------------------------------------------------------------------------


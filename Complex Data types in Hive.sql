
create database complex;

use complex;

------------------------------


1.ARRAY:-
-------

arrayfile
---------
1,abc,40000,a$b$c,city1
2,def,3000,d$f,city2

create table tab7(id int,name string,sal bigint,sub array<string>,city string) row format delimited fields terminated by ',' collection items terminated by '$';

load data local inpath '/home/hduser/arrayfile' overwrite into table tab7;
--------------------------------------------------------------------------

select * from tab7;

output is:-
tab7.id	tab7.name	tab7.sal	tab7.sub	tab7.city
1	abc	40000	["a","b","c"]	city1
2	def	3000	["d","f"]	city2
-----------------------------------------------------------------------

select * from tab7 where id=1;

output is:-
tab7.id	tab7.name	tab7.sal	tab7.sub	tab7.city
1	abc	40000	["a","b","c"]	city1
---------------------------------------------------------------------

select id, name, sal, sub[2] from tab7;

output is:-
id	name	sal	_c3
1	abc	40000	c
2	def	3000	NULL
-------------------------------------------------------------------------------------------------------------------------------------------------------

2. MAP:-

mapfile
-------
1,abc,40000,a$b$c,bonus#500$insurance#200,city1
2,def,3000,d$f,bonus#500,city2

create table tab10(id int,name string,sal bigint,sub array<string>,dud map<string,int>,city string)
row format delimited 
fields terminated by ','
collection items terminated by '$'
map keys terminated by '#';

load data local inpath '/home/hduser/mapfile' overwrite into table tab10;
-----------------------------------------------------------------------------------------------------

select * from tab10;

output is:-
tab10.id	tab10.name	tab10.sal	tab10.sub	tab10.dud	tab10.city
1	abc	40000	["a","b","c"]	{"bonus":500,"insurance":200}	city1
2	def	3000	["d","f"]	{"bonus":500}	city2
---------------------------------------------------------------------------------------------------

select dud["bonus"] from tab10;
 
output is:-
500
500
---------------------------------------------------------------------------------------------------

select dud["bonus"],dud["insurance"] from tab10; 

output is:- 
500	200
500	NULL
-------------------------------------------------------------------------------------------------------------------------------------------------------

3. STRUCT:
----------

structfile
----------
1,abc,40000,a$b$c,pf#500$epf#200,hyd$ap$500001
2,def,3000,d$f,pf#500,bang$kar$600038

create table tab11(id int,name string,sal bigint,sub array<string>,dud map<string,int>,addr struct<city:string,state:string,pin:bigint>)
row format delimited 
fields terminated by ','
collection items terminated by '$'
map keys terminated by '#';

load data local inpath '/home/hduser/structfile' into table tab11;
----------------------------------------------------------------------------------

select * from tab11;

output is:- 
tab11.id	tab11.name	tab11.sal	tab11.sub	tab11.dud	tab11.addr
1	abc	40000	["a","b","c"]	{"bonus":500,"insurance":200}	{"city":"city1","state":"state1","pin":111}
2	def	3000	["d","f"]	{"bonus":500}	{"city":"city2","state":"state2","pin":222}
-------------------------------------------------------------------------------------------------------------------------

select addr.city from tab11;

output is:-
city1
city2

------------------------------------------------------------------------------------------------------------------------------------------------------



create database complex;

use complex;

------------------------------


1.ARRAY:-
-------

arrayfile
---------
1,abc,40000,a$b$c,city1
2,def,3000,d$f,city2


CREATE TABLE tab7
(
id int,
name string,
sal bigint,
sub array<string>,
city string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '$';


LOAD DATA LOCAL INPATH '/home/hduser/arrayfile' OVERWRITE INTO TABLE tab7;

--------------------------------------------------------------------------

desc tab7;

output is:-
col_name	data_type
id                  int                 	                    
name               string              	                    
sal                bigint              	                    
sub                array<string>       	                    
city               string   
-----------------------------------------------------------------------

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

select id,name,sub[0] from tab7;

output is:-
id	name	_c2
1	abc	a
2	def	d
----------------------------------------------------------------------

select id, name, sal, sub[2] from tab7;

output is:-
id	name	sal	_c3
1	abc	40000	c
2	def	3000	NULL
----------------------------------------------------------------------

select * from tab7 where array_contains(sub,'c');

output is:-
tab7.id	tab7.name	tab7.sal	tab7.sub	tab7.city
1	abc	40000	["a","b","c"]	city1
-----------------------------------------------------------------------

select * from tab7 where array_contains(sub,'c') or array_contains(sub,'f');

output is:-

tab7.id	tab7.name	tab7.sal	tab7.sub	tab7.city
1	abc	40000	["a","b","c"]	city1
2	def	3000	["d","f"]	city2

-------------------------------------------------------------------------------------------------------------------------------------------------------

2. MAP:-

mapfile
-------
1,abc,40000,a$b$c,bonus#500$insurance#200,city1
2,def,3000,d$f,bonus#500,city2

CREATE TABLE tab10
(
id int,
name string,
sal bigint,
sub array<string>,
dud map<string,int>,
city string
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '$'
MAP KEYS TERMINATED BY '#';

LOAD DATA LOCAL INPATH '/home/hduser/mapfile' OVERWRITE INTO TABLE tab10;

-----------------------------------------------------------------------------------------------------

desc tab10;

output is:-
col_name	data_type	
id                  int                 	                    
name                string              	                    
sal                 bigint              	                    
sub                 array<string>       	                    
dud                 map<string,int>     	                    
city                string    
------------------------------------------------------------------------------------------------

select * from tab10;

output is:-
tab10.id	tab10.name	tab10.sal	tab10.sub	tab10.dud	tab10.city
1	abc	40000	["a","b","c"]	{"bonus":500,"insurance":200}	city1
2	def	3000	["d","f"]	{"bonus":500}	city2
---------------------------------------------------------------------------------------------------

select * from tab10 where dud["bonus"]>0;

output is:-
tab10.id	tab10.name	tab10.sal	tab10.sub	tab10.dud	tab10.city
1	abc	40000	["a","b","c"]	{"bonus":500,"insurance":200}	city1
2	def	3000	["d","f"]	{"bonus":500}	city2
--------------------------------------------------------------------------------------------------

select * from tab10 where dud["insurance"] is not null;

output is:-
tab10.id	tab10.name	tab10.sal	tab10.sub	tab10.dud	tab10.city
1	abc	40000	["a","b","c"]	{"bonus":500,"insurance":200}	city1
--------------------------------------------------------------------------------------------------

select sum(dud["bonus"]) from tab10 where dud["bonus"] is not null;

output is :- 1000
--------------------------------------------------------------------------------------------------

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

CREATE TABLE tab11
(
id int,
name string,
sal bigint,
sub array<string>,
dud map<string,int>,
addr struct<city:string,state:string,pin:bigint>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '$'
MAP KEYS TERMINATED BY '#';

LOAD DATA LOCAL INPATH '/home/hduser/structfile' INTO TABLE tab11;

----------------------------------------------------------------------------------
desc tab11;

output is:-
col_name	data_type	comment
id                  	int                 	                    
name                	string              	                    
sal                 	bigint              	                    
sub                 	array<string>       	                    
dud                 	map<string,int>     	                    
addr                	struct<city:string,state:string,pin:bigint>
---------------------------------------------------------------------------------

select addr.city,addr.state,addr.pin from tab11;

output is:-
city	state	pin
city1	state1	111
city2	state2	222
---------------------------------------------------------------------------------

select * from tab11 where addr.pin=222;

output is:-
tab11.id	tab11.name	tab11.sal	tab11.sub	tab11.dud	tab11.addr
2	def	3000	["d","f"]	{"bonus":500}	{"city":"city2","state":"state2","pin":222}
 

---------------------------------------------------------------------------------

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


# bas_challenge

Project structure

For this project I used PostgreSQL database hosted inside a Docker container and used Python script to load the file into the database. However, for the purpose of this challenge I will describe loading process for the given tables on a conceptual level, tool independent. 

Table Design


Initialy 5 tables have been loaded: 

1. Employee - containing data about the employee (Employee ID) and hourly rate (Hourly Rate) for that employee for the given period (Start - End date). This table would be loaded incrementally based on the Start date (where Start date >= last_load_date). Load on start date has been set on purpose since there should not be retroactive changes in previously invoiced period. Granularity is on the Hourly Rate level for the given period. 
2. TimeBooking - containing data about hourly entries (Hours) of an Employee (Employee ID) for a given SLA (SLA) on a daily level (Date). This table would be loaded incrementaly based on the Date column (where Date >= last_loat_date). Granularity is on an entry level (hours booked by an Employee per SLA per Day).
3. Customers - containing data about customers (Customer ID, Customer). Full load, as it is not expected that this table has often changes or that many rows. Granularuty level is per customer.
4. SLA - containing data about SLAs (SLA ID) for the given period (Start - End date) and the Budget for that period (Budget). This table would be loaded incrementaly based on the Start Date column (where Start date >= last_load_date). Granularity is on a SLA level for the given period, that is on the SLA period level. This is under hypothesis that the Budget should be planned in advanced, without retroactive changes. 
5. Customer2Sla - containing data that connects Customers (Customer ID) to the SLAs (SLA ID) for the given period (Start- End Date) together with budget Allocation (Allocation) that corresponds to the given Customer. This table would be loaded incrementaly based on the Start Date column (where Start date >= last_load_date). Granularity is on the level of a Customer per given SLA period (one row for each Customer within the given period). Again, hypothesis is that Allocation rate cannot be retroactively changed. 

In case that we want to trace retroactive changes and update certain values, we would need to add Modified timestamp column to the each table and include that column in the where clause.  

![image](https://user-images.githubusercontent.com/56403895/126695839-86e448de-f5da-439c-9da3-79c7db595d72.png)


Data Healtcheck

Upon the initial data load, we undertake several steps to check data health:

1. Checking for duplicate entries in TimeBooking table. Those duplicates will be filtered out in the second stage load where we do denormalization. 
-If there is more then one distinct combination of "Employee ID", "Hours", "SLA", "Date", we consider that to be a duplicate:
select "Employee ID", "Hours", "SLA", "Date", extract(year from "Date") as "year", count(*) as no_of_rows from homework."TimeBooking" tb 
group by "Employee ID", "Hours", "SLA", "Date", extract(year from "Date")
having count(*) > 1

Sample result: 
E02	7	S03	2021-09-18 00:00:00	2021.0	2
E03	7	S01	2021-11-08 00:00:00	2021.0	2
E02	7	S01	2021-10-23 00:00:00	2021.0	2
E01	4	S04	2021-10-14 00:00:00	2021.0	2


2. Checking if Allocation for the given period is always 100 percent:

--checking if Allocation for SLA is not 100 percent
select "SLA ID", extract(year from "StartDate") as "year", total_allocation from (
select *,
sum("Allocation") over (partition by "SLA ID", "StartDate" order by "SLA ID", "StartDate") as total_allocation
from homework."Customer2SLA" cs
) t
where total_allocation <> 1
;
Sample result: 
S02	2021.0	0.5
S03	2020.0	0.9
S03	2020.0	0.9
S03	2020.0	0.9
S03	2021.0	0.8

3. Checking if SLA End Date matches in both SLAs table and Customer2SLA table. We use End Date from SLAs table as valid. 

select distinct "SLA ID", Customer2SLA_end, SLAs_end from(
select
cl."SLA ID",
to_char(cl."StartDate" , 'YYYY') as SLA_YEAR,
date(cl."StartDate") as Customer2SLA_start,
date(s."StartDate") as SLAs_start, 
date(cl."EndDate") as Customer2SLA_end, 
date(s."EndDate") as SLAs_end

FROM homework."Customer2SLA" cl
left join homework."SLAs" s on s."SLA ID" = cl."SLA ID" and s."StartDate" = cl."StartDate"  --and s."EndDate" = cl."EndDate" 
) t 
where Customer2SLA_end <> SLAs_end 
;

Sample result:
S01	2021-12-31	2021-11-30
S05	2021-12-31	2021-11-30


Second Stage Table Load 

After we have performed Data healtcheck, we proceed to the second stage load where we join tables. 

1. Employee table is added to the TimeBooking table by left join ( on	e."Employee ID" = tb."Employee ID" and tb."Date" between e."StartDate" and e."EndDate")
While performing join, by using PostgreSQL Select Distinct ON ("Employee ID","SLA","Date","Hours"), we filter out duplicate entries from TimeBooking table.
At the same time we aggregate data on a daily level per employee by doing sum of Hours column from the same table. 

For detailed code see timebooking_staging.sql file. 

2. 

Data Validation

1. Checking if the Budget for a certain SLA has been breached after we removed duplicate booking entries. 

select distinct  "SLA", SLA_YEAR, used_budget, "Budget", round(used_budget/ "Budget", 2) as utilization_rate   from(
SELECT 
"SLA", 
to_char(tb."Date", 'YYYY') as SLA_YEAR,
SUM(booked_amount) as used_budget,
"Budget"
FROM homework.timebooking_staging tb
left join homework."Employee" e on tb."Employee ID" = e."Employee ID" and tb."Date" between e."StartDate" and e."EndDate" 
inner join homework."SLAs" s on s."SLA ID" = tb."SLA" and tb."Date" between s."StartDate" and s."EndDate" 
group by "SLA", to_char(tb."Date", 'YYYY'), "Budget"
order by "SLA"
) t
where used_budget > "Budget"
order by  utilization_rate desc;

(In PostgreSQL quitation mark refers to the column like in "Budget", not the string)

Sample result:
S05	2020	79360	12000	6.61
S03	2021	91350	15000	6.09
S05	2021	85400	42000	2.03
S04	2020	80960	55000	1.47
S02	2021	85400	65500	1.30

2. Checking if there were booking entries outside valid SLA period, that is after an SLA has been ended. 

select "Employee ID", "SLA", "Hours", "Date", "StartDate", "EndDate" from homework."TimeBooking" tb 
left join homework."SLAs" s on s."SLA ID" = tb."SLA" and "Date" between "StartDate"  and "EndDate" 
where "StartDate" is null or "EndDate" is null

Sample result: 
E02	S05	5	2021-12-04 00:00:00		
E01	S05	2	2021-12-19 00:00:00		
E02	S01	2	2021-12-19 00:00:00		
E02	S05	2	2021-12-20 00:00:00		
E01	S05	4	2021-12-24 00:00:00		
E02	S05	5	2021-12-04 00:00:00		






Budget utilization per SLA in 2021 for the Customer with lowest Budget utilization rate in 2020: 
C01	S04	2021	72800	105600	0.69
C01	S05	2021	85400	37800	2.26

SELECT 
cls."Customer ID",
"SLA", 
cls."year",
SUM(booked_amount) as used_budget,
customer_budget,
round(SUM(booked_amount)/ customer_budget, 2) as utilization_rate
FROM homework.timebooking_staging tb
inner join homework.sla_staging cls on cls."SLA ID" = tb."SLA" and tb."Date" between cls."StartDate" and cls."EndDate" 
inner join  (
select  "Customer ID"  from (
SELECT 
cls."Customer ID",
cls."year",
sum(booked_amount) as booked_amount,
customer_budget,
round(SUM(booked_amount)/ customer_budget, 2) as utilization_rate
FROM homework.timebooking_staging tb
inner join homework.sla_staging cls on cls."SLA ID" = tb."SLA" and tb."Date" between cls."StartDate" and cls."EndDate"
where to_char(tb."Date", 'YYYY') = '2020'
group by cls."Customer ID", cls."year", customer_budget
) t
order by  utilization_rate asc
limit 1
) p on p."Customer ID" = cls."Customer ID" 
where tb."year" = 2021
group by cls."Customer ID", "SLA", cls."year", customer_budget

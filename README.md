# bas_challenge

**Project structure**

For this project, I used a PostgreSQL database hosted inside a Docker container and used Python script to load the file into the database. However, for the purpose of this challenge, I will describe the loading process for the given tables on a conceptual level, tool independent. 

Important notice - Since PostgreSQL is case sensitive, quitation marks are required around column names that contain capital letters (example "Budget"), or column names that use keywords (example "year"), while strings are marked with a single quote (').

**1. and 2. Table Design and Load**

Initially, 5 tables have been loaded: 

1. Employee - containing data about the employee (Employee ID) and hourly rate (Hourly Rate) for that employee for the given period (Start-End date). This table would be loaded incrementally based on the Start date (where Start date >= last_load_date). Load on start date has been set on purpose since there should not be retroactive changes in the previously invoiced period. Granularity is on the Hourly Rate level for the given period. 
2. TimeBooking - containing data about hourly entries (Hours) of an Employee (Employee ID) for a given SLA (SLA) on a daily level (Date). This table would be loaded incrementally based on the Date column (where Date >= last_loat_date). Granularity is on an entry-level (hours booked by an Employee per SLA per Day).
3. Customers - containing data about customers (Customer ID, Customer). Full load, as it is not expected that this table has often changed or that many rows. The granularity level is per customer.
4. SLA - containing data about SLAs (SLA ID) for the given period (Start-End date) and the Budget for that period (Budget). This table would be loaded incrementally based on the Start Date column (where Start date >= last_load_date). Granularity is on an SLA level for the given period, which is on the SLA period level. This is under the hypothesis that the Budget should be planned in advance, without retroactive changes. 
5. Customer2Sla - containing data that connects Customers (Customer ID) to the SLAs (SLA ID) for the given period (Start- End Date) together with budget Allocation (Allocation) that corresponds to the given Customer. This table would be loaded incrementally based on the Start Date column (where Start date >= last_load_date). Granularity is on the level of a Customer per given SLA period (one row for each Customer within the given period). Again, the hypothesis is that the Allocation rate cannot be retroactively changed. 

In case that we want to trace retroactive changes and update certain values, we would need to add a Modified timestamp column to each table and include that column in the where clause.  

![image](https://user-images.githubusercontent.com/56403895/126695839-86e448de-f5da-439c-9da3-79c7db595d72.png)

**Second Stage Table Load**

After we have performed the Data health check, we proceed to the second stage load where we join tables. 

1. Employee table is added to the TimeBooking table by left join ( on	e."Employee ID" = tb."Employee ID" and tb."Date" between e."StartDate" and e."EndDate")
While performing join, by using PostgreSQL Select Distinct ON ("Employee ID","SLA","Date","Hours"), we filter out duplicate entries from the TimeBooking table.
At the same time we aggregate data on a daily level per employee by doing the sum of Hours column from the same table. Also, we calculated booked_amount (Hours* Hourly Rate).
Granularity is on a daily level per Employee since we booked hours on a daily level. Load would be incremental (where Data >= last_load_date).

For detailed code see timebooking_staging.sql file. 

2. Customer, SLAs and Customer2SLA tables have been joined into sla_staging table ("SLAs" s left join Customer2SLA cs on SLA ID and Start Date and left join Customer on Customer ID). 
Here we calculated SLA duration in months, SLA budget per month (Budget/number of Months), the total allocation of an SLA and flagged SLAs where total allocation is not 100%, and also calculated customer budget (Budget * Allocation). Granularity is on SLA period level. Load would be incremental (Start Date >= last_load_date). 

For detailed code see sla_staging.sql

![image](https://user-images.githubusercontent.com/56403895/126714916-8959c622-9ec5-4bc3-8ec6-88cd0a6944f6.png)

**3. Data Healtcheck and Validation**

Upon the initial data load, we undertake several steps to check data health:

1. Checking for duplicate entries in the TimeBooking table. Those duplicates will be filtered out in the second stage load where we do denormalization. 
-If there is more than one distinct combination of "Employee ID", "Hours", "SLA", "Date", we consider that to be a duplicate:
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
left join homework."SLAs" s on s."SLA ID" = cl."SLA ID" and s."StartDate" = cl."StartDate"  --and s."EndDate" = cl."EndDate" 
) t 
where Customer2SLA_end <> SLAs_end 
;

Sample result:
S01	2021-12-31	2021-11-30
S05	2021-12-31	2021-11-30


4. Checking if the Budget for a certain SLA has been breached after we removed duplicate booking entries. 

select distinct  "SLA", SLA_YEAR, used_budget, "Budget", round(used_budget/ "Budget", 2) as utilization_rate   from(
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
order by  utilization_rate desc;

Sample result:
S05	2020	79360	12000	6.61
S03	2021	91350	15000	6.09
S05	2021	85400	42000	2.03
S04	2020	80960	55000	1.47
S02	2021	85400	65500	1.30

5. Checking if there were booking entries outside the valid SLA period, that is after an SLA has been ended. 

select "Employee ID", "SLA", "Hours", "Date", "StartDate", "EndDate" from homework."TimeBooking" tb 
left join homework."SLAs" s on s."SLA ID" = tb."SLA" and "Date" between "StartDate"  and "EndDate" 
where "StartDate" is null or "EndDate" is null

Sample result: 
E02	S05	5	2021-12-04 00:00:00		
E01	S05	2	2021-12-19 00:00:00		
E02	S01	2	2021-12-19 00:00:00		
E02	S05	2	2021-12-20 00:00:00		
E01	S05	4	2021-12-24 00:00:00		
E02	S05	5	2021-12-04 00:00:00		


**4. 5 SLAs  by budget utilization for June 2021:** 

S05	2021	11900	3818	3.12
S03	2021	3850	1250	3.08
S01	2021	11200	8818	1.27
S02	2021	3850	5458	0.71
S04	2021	7000	13333	0.53



**5. Budget utilization per SLA in 2021 for the Customer with the lowest Budget utilization rate in 2020**

The customer with the lowest Budget utilization rate in 2020 is C01. The budget utilization rate of this Customer per SLA for 20201 is: 
C01	S04	2021	72800	105600	0.69
C01	S05	2021	85400	37800	2.26

For detailed code see worst_customer_2020.sql 

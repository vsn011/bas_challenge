--data validation scripts:

--finding duplicate entries in the Booking table
select  "Employee ID", "Hours", "SLA", "Date", extract(year from "Date") as "year", count(*) as no_of_rows from homework."TimeBooking" tb 
group by "Employee ID", "Hours", "SLA", "Date", extract(year from "Date")
having count(*) > 1
limit 5

 
--counting duplicate hours per SLA & Year
select distinct "SLA", "year", sum("Hours") from (
select count(*) as no_of_rows, "Employee ID", "Hours", "SLA", "Date", extract(year from "Date") as "year" from homework."TimeBooking" tb 
group by "Employee ID", "Hours", "SLA", "Date", extract(year from "Date")
having count(*) > 1
) t 
group by "SLA", "year"
order by "SLA", "year"

--checking if Allocation for SLA is not 100 percent
select "SLA ID", extract(year from "StartDate") as "year", total_allocation from (

select *,
sum("Allocation") over (partition by "SLA ID", "StartDate" order by "SLA ID", "StartDate") as total_allocation
from homework."Customer2SLA" cs
) t

where total_allocation <> 1
;

--checking if SLA end date match in both Customer2SLA and SLAs tables
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


--checking if the Budget for a certain SLA has been breached
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


--checking if there were bookings outside valid SLA period
select "Employee ID", "SLA", "Hours", "Date", "StartDate", "EndDate" from homework."TimeBooking" tb 
left join homework."SLAs" s on s."SLA ID" = tb."SLA" and "Date" between "StartDate"  and "EndDate" 
where "StartDate" is null or "EndDate" is null


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

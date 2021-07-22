select distinct  "SLA", "year", booked_amount, "Budget", round(booked_amount/ "Budget", 2) as utilization_rate   from(
SELECT distinct
"SLA", 
tb."year",
sum(booked_amount) as booked_amount,
"Budget"
FROM homework.timebooking_staging tb  
inner join 
(select distinct "SLA ID", "StartDate", "EndDate",budget_by_month as "Budget" from homework.sla_staging ss) s 
on s."SLA ID" = tb."SLA" and tb."Date" between s."StartDate" and s."EndDate" 
where  tb."month" = 6 and tb."year" = 2021 
group by "SLA",tb."year", "Budget"
order by "SLA"
) t 
order by  utilization_rate desc
limit 5;

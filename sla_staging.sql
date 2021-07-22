

CREATE table if not exists homework.sla_staging (
	"SLA ID" text not NULL,
	"year" int8 not null,
	"StartDate" timestamp not NULL,
	"EndDate" timestamp not NULL,
	"Budget" int8 null, 
	sla_duration_in_months int8 not null,
	budget_by_month int8 not null,
	"Customer ID" text not null,
	"Customer" text not null,
	"Allocation" float8 not null,
	total_allocation float8 not null,
	customer_budget int8 not null,
	allocation_flag int8 not null,
	primary KEY("SLA ID", "StartDate", "Customer")
);


insert into homework.sla_staging

select 
t."SLA ID", "year", t."StartDate", t."EndDate","Budget", sla_duration_in_months, ("Budget" / sla_duration_in_months)::int as budget_by_month,
b."Customer ID", "Customer", "Allocation", total_allocation, "Budget" * "Allocation" as customer_budget,
case when total_allocation = 1 then 0 else 1 end as allocation_flag_warning
from(

select "SLA ID", extract(year from "StartDate") as "year", "StartDate", "EndDate", "Budget", -1*extract(month from age("StartDate", "EndDate" ))+1 as sla_duration_in_months,
"Budget" / -1*extract(month from age("StartDate", "EndDate" ))+1 as budget_by_month  
from homework."SLAs" s 
 
) t 

left join (
select *,
sum("Allocation") over (partition by "SLA ID", "StartDate" order by "SLA ID", "StartDate") as total_allocation
from homework."Customer2SLA" cs 
) b on t."SLA ID" = b."SLA ID" and t."StartDate" = b."StartDate" 
left join homework."Customers" c on c."Customer ID" = b."Customer ID"
group by t."SLA ID", "year", t."StartDate", t."EndDate", "Budget",sla_duration_in_months, "Allocation", total_allocation, b."Customer ID" ,"Customer"
order by (t."SLA ID", t."StartDate", "Customer")

on conflict ("SLA ID", "StartDate", "Customer")
do nothing; 

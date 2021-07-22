--Timebooking staging where we join Employee and TimeBooking tables and remove duplicate entries from TimeBooking
--granularity on Employee level per day
--by using select distinct on we filter out duplicate row from TibeBooking table based on distinct combination of EmpID, Date, SLA and Hours booked

create table if not exists homework.timebooking_staging ( "Employee ID" text not null,
"SLA" text not null,
"Date" timestamp not null,
"year" int8 not null,
"month" int8 not null,
total_daily_hours int8 not null,
"Hourly Rate" int8 not null,
"StartDate" timestamp not null,
"EndDate" timestamp not null,
booked_amount int8 not null ,
primary key ("Employee ID",
"SLA",
"Date",
total_daily_hours) );

insert
	into
	homework.timebooking_staging
select
	tb."Employee ID",
	"SLA",
	"Date",
	"year",
	"month",
	sum("Hours") as total_daily_hours,
	e."Hourly Rate",
	e."StartDate",
	e."EndDate",
	sum("Hours")* e."Hourly Rate" as booked_amount
from
	(
	select
		distinct on
		("Employee ID",
		"SLA",
		"Date",
		"Hours") "Employee ID",
		"SLA",
		"Date",
		extract(year
	from
		"Date") as "year",
		extract(month
	from
		"Date") as "month",
		"Hours"
	from
		homework."TimeBooking" tb ) tb
left join homework."Employee" e on
	e."Employee ID" = tb."Employee ID"
	and tb."Date" between e."StartDate" and e."EndDate"
group by
	tb."Employee ID",
	"SLA",
	"Date",
	"year",
	"month",
	e."Hourly Rate",
	e."StartDate",
	e."EndDate" on
	conflict ("Employee ID",
	"SLA",
	"Date",
	total_daily_hours) do nothing;


select * from homework.timebooking_staging


--create table for storing duplicate and false entries 

CREATE table if not exists homework.timebooking_duplicates (
	"Employee ID" text not NULL,
	"SLA" text not NULL,
	"Hours" int8 not NULL,
	"Date" timestamp not null,
	flag text not null,
	primary key("Employee ID", "SLA", "Hours", "Date")
);

insert into homework.timebooking_duplicates

select distinct on ("Employee ID", "SLA", "Hours", "Date")
"Employee ID", "SLA", "Hours", "Date", 'duplicate' as flag
from (
select count(*) as no_of_rows, "Employee ID", "Hours", "SLA", "Date"from homework."TimeBooking" tb 
group by "Employee ID", "Hours", "SLA", "Date"
having count(*) > 1) t
on conflict ("Employee ID", "SLA", "Hours", "Date")
do nothing; 

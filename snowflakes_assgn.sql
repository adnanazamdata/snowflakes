create database project;
use database project;

create or replace table initial (xamdata VARIANT);

select * from initial;

//creating Department table
create or replace table Dept(
dept_id int,
name varchar(20),
primary key(dept_id)
);


//creating Employee table
create table Emp(
dept_id int,
emp_id int,
emp_name varchar(30),
emp_title varchar(30),
emp_ssn varchar(30),
primary key(emp_id),
FOREIGN KEY (dept_id) REFERENCES Dept(dept_id)
);

//creating Address table
create table Addr(
emp_id int not null,
add_history int not null autoincrement,
street_1 varchar(100),
street_2 varchar(50),
city varchar(20),
state varchar(10),
zipcode int,
start_date date,
end_date date,
FOREIGN KEY (emp_id) REFERENCES Emp(emp_id)
);


create or replace procedure populate()
returns string
language sql
as
$$
begin

insert into Dept 
select 
XMLGET(xamdata, 'dept_id'):"$"::integer as dept_id, 
XMLGET(xamdata, 'dept_name'):"$"::string as name  
from initial;

insert into Emp
select
XMLGET(xamdata, 'dept_id'):"$"::integer as dept_id,
XMLGET(emp.value, 'emp_id'):"$"::integer as emp_id,
concat(XMLGET(emp.value, 'emp_fname'):"$"::string, ' ',XMLGET(emp.value, 'emp_lname'):"$"::string)::string as emp_name,
XMLGET(emp.value, 'emp_title'):"$"::string as emp_title,
XMLGET(emp.value, 'emp_ssn'):"$"::string as emp_ssn
from initial, lateral FLATTEN(initial.xamdata:"$") emp
where GET(emp.value, '@') = 'employee';

insert into addr(emp_id, street_1, street_2, city, state, zipcode, start_date, end_date)
select
XMLGET(emp.value, 'emp_id'):"$"::integer as emp_id,
XMLGET(addr.value,'street_1'):"$"::string as street_1,
XMLGET(addr.value,'street_2'):"$"::string as street_2,
XMLGET(addr.value,'city'):"$"::string as city,
XMLGET(addr.value,'state'):"$"::string as state,
XMLGET(addr.value,'zipcode'):"$"::integer as zipcode,
XMLGET(addr.value,'start_date'):"$"::date as start_date,
try_cast(XMLGET(addr.value,'end_date'):"$":: string as date) as end_date
from initial, lateral FLATTEN(initial.xamdata:"$") emp, lateral FLATTEN(emp.value:"$") addr
where GET(emp.value, '@') = 'employee'and GET(addr.value, '@') = 'address';

return 'executed';
end;
$$
;

create or replace stream rawstream on table initial;

create task task_populate
WAREHOUSE = COMPUTE_WH
when
system$stream_has_data('rawstream')
AS CALL populate();

show tasks;

alter task rawstream resume;

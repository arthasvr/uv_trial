Shift+Alt+click - duplicate a ROW
Alt+ up or down - move a ROW
ctrl+Alt+arrow - extend the cursor

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/0197-rising-temperature


Insert into Weather values (1, '2015-01-01', 10),          
(2, '2015-01-02', 25),          
(3, '2015-01-03', 20),          
(4, '2015-01-04', 30)      


create table if not exists Weather (
    id INT PRIMARY KEY,
    recordDate DATE,
    temperature INT
)

select t.id 
from (select id,recorddate,temperature, lag(temperature) over () as previous_temp from weather) t 
where temperature > previous_temp;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/1661-average-time-of-process-per-machine

drop table if exists activity;

create table activity (
    machine_id VARCHAR(12),
    process_id VARCHAR(12),
    activity_type VARCHAR(12),
    timestamp FLOAT
 );


 Insert into activity values ('0','0','start',0.712),     
 ('0','0','end  ',1.520),     
 ('0','1','start',3.140),     
 ('0','1','end  ',4.120),     
 ('1','0','start',0.550),     
 ('1','0','end  ',1.550),     
 ('1','1','start',0.430),     
 ('1','1','end  ',1.420),     
 ('2','0','start',4.100),     
 ('2','0','end  ',4.512),     
 ('2','1','start',2.500),     
 ('2','1','end  ',5.000);


-- below are 2 solutions created by you on different times without the knowledge of the other solution. 
-- Both creates the correct output.

with cte as (
    select 
    machine_id, 
    process_id, 
    min(timestamp) over (partition by machine_id,process_id) as start_time, 
    max(timestamp) over(partition by machine_id,process_id) as end_time
    from activity 
     ),
     cte2 as (select machine_id,process_id,end_time-start_time as processing_time from cte) 
     select machine_id, round(avg(processing_time),3) as processing_time from cte2 group by machine_id;



with temp as (
    select *, lead(timestamp) over (partition by machine_id, process_id) as stop_time from activity
),
temp1 as (select * from temp where stop_time is not null)
select machine_id, round(avg((stop_time-timestamp)),3) as processing_time from temp1 group by machine_id;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://leetcode.com/problems/second-highest-salary/description/

with temp as (select *, dense_rank() over (order by salary DESC) as rank from employee)
select max(salary) as second_highest_salary from temp where rank = 2

-- if we dont use max() above it will return 0 values if there is only one record in employee table. by using the aggregating function max(),
-- max() of an empty set is "NULL".
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

180 - https://leetcode.com/problems/consecutive-numbers/description/


with temp as (select *, lag(id) over (partition by num) as prev_id from logs),
temp1 as (select * from temp where id=(prev_id+1)),
temp2 as (select num, count(num) as count from temp1 group by num)
select num as ConsecutiveNums from temp2 where count >=2;

insert into logs values 
 (1,1),   
 (2,1),   
 (3,1),   
 (4,3),   
 (5,1),   
 (6,2),   
 (7,2),
 (8,2),
 (9,3),
 (10,3);

 create table logs(
    id int,
    num int
 );


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/0185-department-top-three-salaries


drop table if exists employee;
drop table if exists department;


create table department (
    id int,
    name varchar(10)
);

 create table employee (
    id int,
    name varchar(10),
    salary int,
    departmentid int
 );

 insert into employee values (1,'Joe  ',85000,1),
 (2,'Henry',80000,2),
 (3,'Sam  ',60000,2),
 (4,'Max  ',90000,1),
 (5,'Janet',69000,1),
 (6,'Randy',85000,1),
 (7,'Will ',70000,1);

 Insert into department values (1, 'IT'),(2,'Sales');

--  note that using rank as an alias with out single quotes gives error since rank is a reserved keyword in sql, 
-- also you will get wrong result if you give a string in filter, as shown below.

with temp as (select e.id, e.name,e.salary,d.name as department_name from employee e join department d on e.departmentId = d.id),
temp1 as (select *, DENSE_RANK() over (partition by department_name order by salary desc) as 'rank' from temp)
select department_name as Department, name as Employee, Salary from temp1 where 'rank' <=3;

+------------+----------+--------+
| Department | Employee | Salary |
+------------+----------+--------+
| IT         | Max      |  90000 |
| IT         | Joe      |  85000 |
| IT         | Randy    |  85000 |
| IT         | Will     |  70000 |
| IT         | Janet    |  69000 |
| Sales      | Henry    |  80000 |
| Sales      | Sam      |  60000 |
+------------+----------+--------+
7 rows in set, 1 warning (0.00 sec)

with temp as (select e.id, e.name,e.salary,d.name as department_name from employee e join department d on e.departmentId = d.id),
temp1 as (select *, DENSE_RANK() over (partition by department_name order by salary desc) as 'rank' from temp)
select department_name as Department, name as Employee, Salary from temp1 where temp1.rank <=3;

+------------+----------+--------+
| Department | Employee | Salary |
+------------+----------+--------+
| IT         | Max      |  90000 |
| IT         | Joe      |  85000 |
| IT         | Randy    |  85000 |
| IT         | Will     |  70000 |
| Sales      | Henry    |  80000 |
| Sales      | Sam      |  60000 |
+------------+----------+--------+
6 rows in set (0.00 sec)

with temp as (select e.id, e.name,e.salary,d.name as department_name from employee e join department d on e.departmentId = d.id),
temp1 as (select *, dense_rank() over (partition by department_name order by salary desc) as drank from temp)
select department_name as Department, name as Employee, Salary from temp1 where drank <=3;

+------------+----------+--------+
| Department | Employee | Salary |
+------------+----------+--------+
| IT         | Max      |  90000 |
| IT         | Joe      |  85000 |
| IT         | Randy    |  85000 |
| IT         | Will     |  70000 |
| Sales      | Henry    |  80000 |
| Sales      | Sam      |  60000 |
+------------+----------+--------+
6 rows in set (0.00 sec)


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/0196-delete-duplicate-emails

insert into person values (1,'john@example.com'),
(2,'bob@example.com '),
(3,'john@example.com');

drop table if EXISTS Person;


create table person (
    id int,
    email varchar(50)
);

with temp as (select *, DENSE_RANK() over (PARTITION BY email order by id) as drank from person)
delete from person 
where id in (select id from temp where drank > 1);


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/0550-game-play-analysis-iv


drop table if exists activity;

create table activity (
    player_id int,
    device_id int,
    event_date date,
    games_played int
);



insert into activity values 
(1,2,'2016-03-01',5),
(1,2,'2016-03-02',6),
(2,3,'2017-06-25',1),
(3,1,'2016-03-02',0),
(3,4,'2018-07-03',5);

with temp as (select *,lead(event_date) over (partition by player_id order by event_date ) as next_date from activity),
temp1 as (select *,DATEDIFF(next_date,event_date) as difference_ from temp)
select round(difference_ /count(distinct player_id),2) from temp1;

with temp as (select *,lead(event_date) over (partition by player_id order by event_date ) as next_date from activity),
temp1 as (select *,DATEDIFF(next_date,event_date) as difference_ from temp)
select round(difference_ /count(distinct player_id),2) from temp1 where difference_ is not null;


with sub as (
    select player_id, event_date, 
           first_value(event_date) over (partition by player_id order by event_date) first_login,
           lead(event_date, 1) over (partition by player_id order by event_date) next_login 
           from activity)
select 
    round(
            count(distinct player_id) / 
            (select count(distinct player_id) from activity)
            ,2) fraction 
            from sub
            where event_date = first_login AND
            next_login = event_date + interval 1 day;


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/0570-managers-with-at-least-5-direct-reports



drop table if exists employee;

create table employee (
    id int,
    name varchar(50),
    department varchar(50),
    managerid INT
);


insert into employee values 
(101,'John ','A',null),
(102,'Dan  ','A',101),
(103,'James','A',101),
(104,'Amy  ','A',101),
(105,'Anne ','A',101),
(106,'Ron  ','B',101);


with cte as (select 
managerid
from employee
GROUP BY managerid
having count(id) >= 5
)
select name from employee where id in (select * from cte
 where managerid is not null
 );


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/0577-employee-bonus

drop table if exists employee;

create table employee (
    empid int,
    name varchar(50),
    supervisor int,
    salary INT
);

drop table if exists bonus;

create table bonus (
    empid int,
    bonus INT
);



insert into employee values (3,'Brad  ',null,4000),
(1,'John  ',3   ,1000),
(2,'Dan   ',3   ,2000),
(4,'Thomas',3   ,4000);


insert into bonus values 
(2, 500),
(4, 2000);



select e.name, b.bonus from employee e left join bonus b on e.empid=b.empid where b.bonus <1000 or b.bonus is null;


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/0584-find-customer-referee

drop table if exists customer;

create table customer (
    id int,
    name varchar(50),
    referee_id int
);



insert into customer values 
(1,'Will',null),
(2,'Jane',null),
(3,'Alex',2   ),
(4,'Bill',null),
(5,'Zack',1   ),
(6,'Mark',2   );


select name from customer where referee_id != 2 or referee_id is null;


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/0585-investments-in-2016

drop table if exists insurance;

create table insurance (
    pid int,
    tiv_2015 float,
    tiv_2016 float,
    lat float,
    lon float
);



insert into insurance values 
(1,10,5 ,10,10),
(2,20,20,20,20),
(3,10,30,20,20),
(4,10,40,40,40);



select round(sum(tiv_2016),2) as tiv_2016 
from insurance 
where tiv_2015 in (select tiv_2015 from insurance group by tiv_2015 having count(*) > 1)
 and (lat,lon) in (select lat,lon from insurance group by lat, lon having count(*) = 1);

 
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/0595-big-countries

drop table if exists world;

create table world (
    name varchar(50),
    continent varchar(50),
    area int,
    population int,
    gdp bigint
);

insert into world values 
('Afghanistan','Asia  ',652230 ,25500100,20343000000 ),
('Albania    ','Europe',28748  ,2831741 ,12960000000 ),
('Algeria    ','Africa',2381741,37100000,188681000000),
('Andorra    ','Europe',468    ,78115   ,3712000000  ),
('Angola     ','Africa',1246700,20609294,100990000000);

select name, population, area from world where area >= 3000000 or population >= 25000000;


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/0596-classes-more-than-5-students

drop table if exists courses;

create table courses (
    student varchar(50),
    class varchar(50)
);

insert into courses values 
('A','Math    '),
('B','English '),
('C','Math    '),
('D','Biology '),
('E','Math    '),
('F','Computer'),
('G','Math    '),
('H','Math    '),
('I','Math    ');


select class from courses group by class having count(*) >= 5;


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/0602-friend-requests-ii-who-has-the-most-friends

drop table if exists Requestaccepted;

create table requestaccepted (
    requester_id int,
    accepter_id int,
    accept_date date
);


Insert into requestaccepted values 
(1,2,'2016/06/03'),
(1,3,'2016/06/08'),
(2,3,'2016/06/08'),
(3,4,'2016/06/09');


with cte as (select requester_id from requestaccepted 
union all 
select accepter_id from requestaccepted) 
select *, count(*) as num from cte group by requester_id order by num desc limit 1;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/0610-triangle-judgement

drop table if exists triangle;

create table triangle (
    x int,
    y int,
    z INT
);



insert into triangle values 
(13,15,30),
(10,20,15);


select x,y,z, 
case 
when x+y>z and x+z>y and y+z>x then 'yes'
else 'no' 
end as triangle from triangle;
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/0619-biggest-single-number

drop table if exists mynumbers;

create table mynumbers (
    num int
);

insert into mynumbers values 
(8),
(3),
(3),
(8),
(1),
(4),
(5),
(6);

truncate table mynumbers;

insert into mynumbers values 
(8),
(8),
(7),
(7),
(3),
(3),
(3),
(3);

with cte as (select *, count(*) as occurence from mynumbers
group by num 
having count(*) = 1)
select max(num) as num from cte;
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/0620-not-boring-movies

drop table if exists cinema;

create table cinema (
    id int,
    movie varchar(50),
    description varchar(50),
    rating float
);

insert into cinema values 
(1,'War       ','great 3D   ',8.9),
(2,'Science   ','fiction    ',8.5),
(3,'irish     ','boring     ',6.2),
(4,'Ice song  ','Fantacy    ',8.6),
(5,'House card','Interesting',9.1);


select * from cinema where id%2!=0 and description != 'boring' order by rating desc;
+------+------------+-------------+--------+
| id   | movie      | description | rating |
+------+------------+-------------+--------+
|    5 | House card | Interesting |    9.1 |
|    1 | War        | great 3D    |    8.9 |
|    3 | irish      | boring      |    6.2 |
+------+------------+-------------+--------+


select * from cinema where id%2!=0 and Not(instr('description','boring')) order by rating desc;
+------+------------+-------------+--------+
| id   | movie      | description | rating |
+------+------------+-------------+--------+
|    5 | House card | Interesting |    9.1 |
|    1 | War        | great 3D    |    8.9 |
|    3 | irish      | boring      |    6.2 |
+------+------------+-------------+--------+


-- it seems if we dont use the single quotes for our column in the instr function, it shows wrong result.

select * from cinema where id%2!=0 and Not(instr(description,'boring')) order by rating desc;
+------+------------+-------------+--------+
| id   | movie      | description | rating |
+------+------------+-------------+--------+
|    5 | House card | Interesting |    9.1 |
|    1 | War        | great 3D    |    8.9 |
+------+------------+-------------+--------+

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/0626-exchange-seats

drop table if exists seat;

create table seat (
    id int,
    student varchar(50)
);

insert into seat values 
(1,'Abbot  '),
(2,'Doris  '),
(3,'Emerson'),
(4,'Green  '),
(5,'Jeames ');



select 
case 
when id = (select max(id) from seat) and id % 2 != 0 then id
when id % 2  = 1 then id+1 
else id-1
end as id,
student
from seat
order by id;


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/1045-customers-who-bought-all-products

drop table if exists customer;

create table customer (
    customer_id int,
    product_key int
);

drop table if exists product;

create table product (
    product_key int
);

insert into customer values 
(1,5),
(2,6),
(3,5),
(3,6),
(1,6);

insert into product values 
(5),
(6);

with cte as (select * from customer 
union
select * from customer),
cte1 as (select customer_id from cte group by customer_id having count(customer_id) = (select count(*) from product))
select * from cte1;



select customer_id
from customer
group by customer_id
having count(distinct product_key ) = (select count(product_key) from product );

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/1068-product-sales-analysis-i

drop table if exists sales;

create table sales (
    sale_id int,
    product_id int,
    year int,
    quantity int,
    price int
);

drop table if exists product;

create table product (
    product_id int,
    product_name varchar(50)
);

insert into sales values 
(1,100,2008,10,5000),
(2,100,2009,12,5000),
(7,200,2011,15,9000);

insert into product values 
(100,'Nokia  '),
(200,'Apple  '),
(300,'Samsung');


select p.product_name, s.year, s.price from sales s join product p on s.product_id = p.product_id;


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/1070-product-sales-analysis-iii

with cte as (select s.sale_id, s.product_id, s.year, s.quantity, s.price, p.product_name from sales s join product p on s.product_id = p.product_id),
cte2 as (select *, ROW_NUMBER() over (partition by product_name order by year) as row_num1 from cte)
select product_id, year as first_year, quantity, price from cte2 where row_num1 = 1;


select
product_id ,
year  as first_year , quantity , price
from sales
where ((product_id, year) IN (select product_id, min(year) from sales group by product_id));


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/1075-project-employees-i

drop table if exists project;

create table project (
    project_id int,
    employee_id int
);

drop table if exists employee;

create table employee(
    employee_id int,
    name varchar (50),
    experience_years int
);

insert into project values 
(1,1),
(1,2),
(1,3),
(2,1),
(2,4);

insert into employee values 
(1,'Khaled',3),
(2,'Ali   ',2),
(3,'John  ',1),
(4,'Doe   ',2);



with cte as (select * from employee join project using (employee_id))
select project_id, round(avg(experience_years),2) as average_years from cte group by project_id;
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

https://github.com/arfin-parween/SQL-50-Leetcode-/tree/main/1141-user-activity-for-the-past-30-days-i

drop table if exists activity;

create table activity (
    user_id int,
    session_id int,
    activity_date date,
    activity_type enum('open_session', 'end_session', 'scroll_down', 'send_message')
);


insert into activity values 
(1,1,'2019-07-20','open_session'),
(1,1,'2019-07-20','scroll_down'),
(1,1,'2019-07-20','end_session'),
(2,4,'2019-07-20','open_session'),
(2,4,'2019-07-21','send_message'),
(2,4,'2019-07-21','end_session'),
(3,2,'2019-07-21','open_session'),
(3,2,'2019-07-21','send_message'),
(3,2,'2019-07-21','end_session'),
(4,3,'2019-06-25','open_session'),
(4,3,'2019-06-25','end_session');




select p.activity_date as day, count(distinct p.user_id) as active_users 
from activity p
where activity_date between date_sub('2019-07-27', interval 29 day) and '2019-07-27'
group by p.activity_date;


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


1148

drop table if exists views;

create table views (
    article_id int,
    author_id int,
    viewer_id int,
    view_date date
);

insert into views values 
(1,3,5,'2019-08-01'),
(1,3,6,'2019-08-02'),
(2,7,7,'2019-08-01'),
(2,7,6,'2019-08-02'),
(4,7,1,'2019-07-22'),
(3,4,4,'2019-07-21'),
(3,4,4,'2019-07-21');

select DISTINCT author_id as id from views where author_id = viewer_id order by id;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


1164

drop if exists products;

create table products (
    product_id int,
    new_price int,
    change_date date
);

insert into products values 
(1,20,'2019-08-14'),
(2,50,'2019-08-14'),
(1,30,'2019-08-15'),
(1,35,'2019-08-16'),
(2,65,'2019-08-17'),
(3,20,'2019-08-18');


-- below commented are your failed tries

-- select 

-- case 
-- when change_date < '2019-08-16' then ()
-- when change_date = '2019-08-16' then new_price
-- when product_id not in (select distinct product_id from products where change_date > '2019-08-16') then 10
-- else new_price 
-- from products 


-- select distinct product_id from products where change_date > '2019-08-16'

select product_id ,  new_price as price
from products
where (product_id, change_date) IN
(
-- here we are trying to find the latest record from which we need to take the last price
    select product_id, max(change_date) 
    from products
    where change_date <='2019-08-16'
    group by product_id
)
UNION 
select product_id ,  10 as price
from products
-- using "not in" we are trying to filter the values which have not occured before the said date and marking its price as 10 
where (product_id) not IN
(
    select product_id
    from products
    where change_date <='2019-08-16'
    group by product_id
);

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1174

drop table if exists delivery;

create table delivery (
    delivery_id int,
    customer_id int,
    order_date date,
    customer_pref_delivery_date date
);

insert into delivery values 
(1,1,'2019-08-01','2019-08-02'),
(2,2,'2019-08-02','2019-08-02'),
(3,1,'2019-08-11','2019-08-12'),
(4,3,'2019-08-24','2019-08-24'),
(5,3,'2019-08-21','2019-08-22'),
(6,2,'2019-08-11','2019-08-13'),
(7,4,'2019-08-09','2019-08-09');




-- my try solution below and it works, but not as efficient as the final one.
with cte as (select *,ROW_NUMBER() over (partition by customer_id order by order_date ASC) as Order_number from delivery),
cte2 as (select * from cte where order_number = 1),
cte3 as (select *, case 
when order_date=customer_pref_delivery_date then 1
else 0 
end
as tbc from cte2)
select round(avg(tbc)*100,2) as immediate_percentage from cte3;




SELECT 
    ROUND(avg(IF(min_order_date = min_customer_pref_delivery_date, 1, 0)) * 100 , 2) AS immediate_percentage 
FROM (SELECT MIN(order_date) AS min_order_date, MIN(customer_pref_delivery_date) AS min_customer_pref_delivery_date 
    FROM Delivery 
    GROUP BY customer_id) AS min_delivery_table;
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1193

drop table if exists transactions;

create table transactions (
    id int,
    country varchar(50),
    state enum('approved','declined'),
    amount int,
    trans_date date
);

insert into transactions values 
(121,'US','approved',1000,'2018-12-18'),
(122,'US','declined',2000,'2018-12-19'),
(123,'US','approved',2000,'2019-01-01'),
(124,'DE','approved',2000,'2019-01-07');



-- Note below we can do aggregations based onspecific filters as well as based on the output of case when then statements as well.
select date_format(trans_date, '%Y-%m') as month,
country,
count(id) as trans_count,
sum(state='approved') as approved_count,
sum(amount) as trans_total_amount,
sum(case when state='approved' then amount else 0  end) as approved_total_amount
from transactions
group by month,country;


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1204

drop table if exists queue;

create table queue (
    person_id int,
    person_name varchar(50),
    weight int,
    turn int
);

insert into queue values 
(5,'Alice    ',250,1),
(4,'Bob      ',175,5),
(3,'Alex     ',350,2),
(6,'John Cena',400,3),
(1,'Winston  ',500,6),
(2,'Marie    ',200,4);


-- my try solution below and it works, but not as efficient as the final one.
with cte as (select * from queue order by turn),
cte2 as (select turn, person_name, weight, sum(weight) over (order by turn) as cum_weight from queue),
cte3 as (select person_name, cum_weight from cte2 where cum_weight <= 1000 order by cum_weight desc)
select distinct first_value(person_name) over (order by cum_weight desc) as person_name from cte3;



with cte as
(select
    person_name
    ,sum(Weight) over (order by turn asc) as accumulative_weights
from Queue
)
select person_name
from cte
where accumulative_weights = (
            select max(accumulative_weights) 
            from cte
            where accumulative_weights <= 1000
)

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1211


drop table if exists queries;

create table queries (
    query_name varchar(50),
    result varchar(50),
    position int,
    rating int
);

insert into queries values 
('Dog','Golden Retriever',1  ,5),
('Dog','German Shepherd ',2  ,5),
('Dog','Mule            ',200,1),
('Cat','Shirazi         ',5  ,2),
('Cat','Siamese         ',3  ,3),
('Cat','Sphynx          ',7  ,4);





-- my try solution below and it works, but not as efficient as the final one.
select a.query_name, round(avg(a.query_rating_temp),2) as quality, round(avg(a.poor_query_temp)*100,2) as poor_query_percentage
from (select *, (rating/position) as query_rating_temp, 
case 
when rating < 3 then 1
else 0 
end as poor_query_temp from queries) a
group by a.query_name;




SELECT 
    query_name,
    ROUND(AVG(rating/position), 2) AS quality,
    ROUND(SUM(CASE WHEN rating < 3 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS poor_query_percentage
FROM 
    Queries
WHERE query_name is not null
GROUP BY
    query_name;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1251

drop table if exists prices;

create table prices (
    product_id int,
    start_date date,
    end_date date,
    price int
);

drop table if exists unitssold;

create table unitssold (
    product_id int,
    purchase_date date,
    units int
);

insert into prices values 
(1,'2019-02-17','2019-02-28',5 ),
(1,'2019-03-01','2019-03-22',20),
(2,'2019-02-01','2019-02-20',15),
(2,'2019-02-21','2019-03-31',30),
(3,'2019-02-21','2019-03-31',40);

insert into unitssold values 
(1,'2019-02-25',100),
(1,'2019-03-01',15 ),
(2,'2019-02-10',200),
(2,'2019-03-22',30 );


-- my try solution below and it works, but not as efficient as the final one.
with cte as 
(select p.product_id, p.start_date, p.end_date, p.price, a.purchase_date, a.units 
from prices p left join unitssold a 
on p.product_id=a.product_id and a.purchase_date between p.start_date and p.end_date),
cte2 as (select 
product_id, 
ifnull(round((sum(units*price)/sum(units)),2),0) as average_price 
from cte group by product_id)
select * from cte2;



SELECT p.product_id, IFNULL(round(SUM(p.price*u.units)/sum(u.units),2),0) as average_price
FROM Prices p 
LEFT JOIN UnitsSold u
ON p.product_id = u.product_id AND 
u.purchase_date BETWEEN p.Start_date and p.end_date
GROUP BY p.product_id;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1280

drop table if exists students;

create table students (
    student_id int,
    student_name varchar(50)
);

drop table if exists subjects;

create table subjects (
    subject_name varchar(50)
);

drop table if exists examinations;

create table examinations (
    student_id int,
    subject_name varchar(50)
);

insert into students values 
(1 ,'Alice'),
(2 ,'Bob  '),
(13,'John '),
(6 ,'Alex ');

insert into subjects values 
('Math       '),
('Physics    '),
('Programming');

insert into examinations values 
(1 ,'Math       '),
(1 ,'Physics    '),
(1 ,'Programming'),
(2 ,'Programming'),
(1 ,'Physics    '),
(1 ,'Math       '),
(13,'Math       '),
(13,'Programming'),
(13,'Physics    '),
(2 ,'Math       '),
(1 ,'Math       ');




-- my try solution below and it works, but not as efficient as the below ones.
with cte as (select s.student_id, s.student_name, a.subject_name from students s cross join subjects a),
cte2 as (select s.student_id, s.student_name, s.subject_name, case 
when a.subject_name is Null then 0 
else 1 
end as examination
from cte s left join examinations a on s.student_id=a.student_id and s.subject_name=a.subject_name),
cte3 as (select student_id,student_name,subject_name, sum(examination) as attended_exams from cte2 group by student_id, student_name,subject_name)
select * from cte3 order by student_id, subject_name;



SELECT s.student_id, s.student_name, sub.subject_name, COUNT(e.student_id) AS attended_exams
FROM Students s
CROSS JOIN Subjects sub
LEFT JOIN Examinations e ON s.student_id = e.student_id AND sub.subject_name = e.subject_name
GROUP BY s.student_id, s.student_name, sub.subject_name
ORDER BY s.student_id, sub.subject_name;


SELECT s.student_id, s.student_name, sub.subject_name, COALESCE(e.attended_exams, 0) AS attended_exams
FROM Students s
CROSS JOIN Subjects sub
LEFT JOIN (
    SELECT student_id, subject_name, COUNT(*) AS attended_exams
    FROM Examinations
    GROUP BY student_id, subject_name
) e USING (student_id, subject_name)
ORDER BY s.student_id, sub.subject_name;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1321

drop table if exists customer;

create table customer (
    customer_id int,
    name varchar(50),
    visited_on date,
    amount int
);


insert into customer values 
(1,'Jhon   ','2019-01-01',100),
(2,'Daniel ','2019-01-02',110),
(3,'Jade   ','2019-01-03',120),
(4,'Khaled ','2019-01-04',130),
(5,'Winston','2019-01-05',110), 
(6,'Elvis  ','2019-01-06',140), 
(7,'Anna   ','2019-01-07',150),
(8,'Maria  ','2019-01-08',80 ),
(9,'Jaze   ','2019-01-09',110), 
(1,'Jhon   ','2019-01-10',130), 
(3,'Jade   ','2019-01-10',150);


-- my failed tries below
select visited_on, sum(amount) over (order by visited_on range between visited_on and date_add(visited_on,interval 6 days)) 
from customer group by visited_on order by visited_on;

select visited_on, sum(amount) over (order by visited_on range between current row and date_add(visited_on,interval 6 days)) 
from customer group by visited_on order by visited_on;

select visited_on, date_add(visited_on,interval 6 days), sum() as end_window from customer ;

-- not my try but was trying to experiment with the final correct answer
-- this is wrong because the moving avarage should be between current row and past preceding values and not for future following values
SELECT visited_on, amount, average_amount 
FROM (
    SELECT DISTINCT 
    visited_on, 
    SUM(amount) OVER (ORDER BY visited_on RANGE BETWEEN current row and INTERVAL 6 DAY following) AS amount, 
    ROUND(SUM(amount) OVER (ORDER BY visited_on RANGE BETWEEN current row and INTERVAL 6 DAY following)/7,2) AS average_amount
FROM Customer) as whole_totals
WHERE DATEDIFF(visited_on, (SELECT MIN(visited_on) FROM Customer)) >= 6;

-- tried without distinct but since the last date in the values have a duplicate value it was creating duplicate output rows
SELECT visited_on, amount, average_amount 
FROM (
    SELECT  
    visited_on, 
    SUM(amount) OVER (ORDER BY visited_on RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW) AS amount, 
    ROUND(SUM(amount) OVER (ORDER BY visited_on RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW)/7,2) AS average_amount
FROM Customer) as whole_totals
WHERE DATEDIFF(visited_on, (SELECT MIN(visited_on) FROM Customer)) >= 6;

-- correct answer below
SELECT visited_on, amount, average_amount 
FROM (
    SELECT DISTINCT 
    visited_on, 
    SUM(amount) OVER (ORDER BY visited_on RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW) AS amount, 
    ROUND(SUM(amount) OVER (ORDER BY visited_on RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW)/7,2) AS average_amount
FROM Customer) as whole_totals
WHERE DATEDIFF(visited_on, (SELECT MIN(visited_on) FROM Customer)) >= 6;



+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1327

drop table if exists products;

create table products (
    product_id int,
    product_name varchar(50),
    product_category varchar(50)
);


drop table if exists orders;

create table orders (
    product_id int,
    order_date date,
    unit int
);


insert into products values 
(1,'Leetcode Solutions   ','Book   '),
(2,'Jewels of Stringology','Book   '),
(3,'HP                   ','Laptop '),
(4,'Lenovo               ','Laptop '),
(5,'Leetcode Kit         ','T-shirt');


insert into orders values 
(1,'2020-02-05',60),
(1,'2020-02-10',70),
(2,'2020-01-18',30),
(2,'2020-02-11',80),
(3,'2020-02-17',2 ),
(3,'2020-02-24',3 ),
(4,'2020-03-01',20),
(4,'2020-03-04',30),
(4,'2020-03-04',60),
(5,'2020-02-25',50),
(5,'2020-02-27',50),
(5,'2020-03-01',50);


-- my try solution below and it works, but not as efficient as the below ones.
with cte as (select p.product_id,p.product_name,o.order_date,o.unit 
from products p join orders o using (product_id) where month(order_date) = 2 and year(order_date)=2020),
cte2 as (select product_name, sum(unit) from cte group by product_name  having sum(unit) >= 100 )
select * from cte2 ;


SELECT p.product_name AS product_name, sum(o.unit) AS unit FROM Products p
JOIN Orders o USING (product_id)
WHERE YEAR(o.order_date)='2020' AND MONTH(o.order_date)='02'
GROUP BY p.product_name
HAVING SUM(o.unit)>=100;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1341

drop table if exists movies;

create table movies (
    movie_id int,
    title varchar(50)
);

drop table if exists users;

create table users (
    user_id int,
    name varchar(50)
);

drop table if exists movierating;

create table movierating (
    movie_id int,
    user_id int,
    rating int,
    created_at date
);

insert into movies values 
(1,'Avengers'),
(2,'Frozen 2'),
(3,'Joker   ');

insert into users values 
(1,'Daniel'),
(2,'Monica'),
(3,'Maria '),
(4,'James ');

insert into movierating values 
(1,1,3,'2020-01-12'),
(1,2,4,'2020-02-11'),
(1,3,2,'2020-02-12'),
(1,4,1,'2020-01-01'),
(2,1,5,'2020-02-17'), 
(2,2,2,'2020-02-01'), 
(2,3,2,'2020-03-01'),
(3,1,3,'2020-02-22'), 
(3,2,4,'2020-02-25');


-- my try solution below and it works, but not as efficient as the below one.
(select a.name
from users a join 
(select user_id,count(*) as temp_count from movierating group by user_id) p 
using (user_id) 
order by p.temp_count desc,a.name asc
limit 1)

UNION

(select m.title 
from movies m join 
(select movie_id, avg(rating) as avg_rating from movierating where extract(year_month from created_at) = 202002 group by movie_id) c 
using (movie_id)
order by c.avg_rating desc, m.title ASC
limit 1);




(SELECT name AS results
FROM MovieRating JOIN Users USING(user_id)
GROUP BY name
ORDER BY COUNT(*) DESC, name
LIMIT 1)

UNION ALL

(SELECT title AS results
FROM MovieRating JOIN Movies USING(movie_id)
WHERE EXTRACT(YEAR_MONTH FROM created_at) = 202002
GROUP BY title
ORDER BY AVG(rating) DESC, title
LIMIT 1);
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1378

drop table if exists employees;

create table employees (
    id int,
    name varchar(50)
);

drop table if exists employeeuni;

create table employeeuni (
    id int,
    unique_id int
);

insert into employees values 
(1 ,'Alice   '),
(7 ,'Bob     '),
(11,'Meir    '),
(90,'Winston '),
(3 ,'Jonathan');

insert into employeeuni values 
(3 ,1),
(11,2),
(90,3);


select p.unique_id, c.name from employees c left join employeeuni p using (id);



+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1484

drop table if exists activities;

create table activities (
    sell_date date,
    product varchar(50)
);

insert into activities values 
('2020-05-30','Headphone '),
('2020-06-01','Pencil    '),
('2020-06-02','Mask      '),
('2020-05-30','Basketball'),
('2020-06-01','Bible     '),
('2020-06-02','Mask      '),
('2020-05-30','T-Shirt   ');

-- your tries, but didnt know about group_concat earlier so failed
-- know about the group_concat method
select sell_date, count(distinct product), (select product from activities p where p.sell_date=a.sell_date) 
from activities a group by sell_date;


select  
sell_date, 
count(distinct product) as num_sold, 
GROUP_CONCAT(distinct product order by product asc separator ',') as products 
from Activities
group by sell_date
order by sell_date;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1517

drop table if exists users;

create table users (
    user_id int,
    name varchar(50),
    mail varchar(50)
);

insert into users values 
(1,'Winston  ','winston@leetcode.com'),
(2,'Jonathan ','jonathanisgreat'),
(3,'Annabelle','bella-@leetcode.com'),
(4,'Sally    ','sally.come@leetcode.com'),
(5,'Marwan   ','quarz#2020@leetcode.com'),
(6,'David    ','david69@gmail.com'),
(7,'Shapiro  ','.shapo@leetcode.com');





select * from Users
where mail REGEXP '^[a-zA-Z][a-zA-Z0-9_.-]*@leetcode[.]com$';


^ - start of the string
[a-zA-Z] - ensures first symbol is a letter
[a-zA-Z0-9_.-] - allows symbols like "_.-"
[.] - covers a point symbol
$ - end of the string

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1527

drop table if exists patients;

create table patients (
    patient_id int,
    patient_name varchar(50),
    conditions varchar(50)
);

insert into patients values 
(1,'Daniel','YFEV COUGH  '),
(2,'Alice ',''),
(3,'Bob   ','DIAB100 MYOP'),
(4,'George','ACNE DIAB100'),
(5,'Alain ','DIAB201     ');






-- Going through different solutions, I didn't see anyone use a regular expression with a boundary. So, I decided to post one.
-- The expression conditions REGEXP '\\bDIAB1' is actually the same as conditions LIKE '% DIAB1%' OR conditions LIKE 'DIAB1%';
--  but it is obviously shorter. 😉

-- The reason they are the same is that \b matches either a non-word character (in our case, a space) or 
-- the position before the first character in the string. Also, you need to escape a backslash with another backslash, 
-- like so: \\b. Otherwise, the regular expression won't evaluate.

-- P.S. \b also matches the position after the last character, but it doesn't matter in the context of this problem.

SELECT * FROM patients WHERE conditions REGEXP '\\bDIAB1';


SELECT *
FROM Patients
WHERE conditions LIKE '% DIAB1%' OR conditions LIKE 'DIAB1%';

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1581

drop table if exists visits;

create table visits (
    visit_id int,
    customer_id int
);

drop table if exists transactions;

create table transactions (
    transaction_id int,
    visit_id int,
    amount int
);

insert into visits values 
(1,23),
(2,9 ),
(4,30),
(5,54),
(6,96),
(7,54),
(8,54);

insert into transactions values 
(2 ,5,310),
(3 ,5,300),
(9 ,5,200),
(12,1,910),
(13,2,970);

-- my try solution below and it works, but not as efficient as the below ones.
with cte as (select p.customer_id, p.visit_id, a.transaction_id from visits p left join transactions a using (visit_id)
where a.transaction_id is null)
select customer_id, count(*) from cte group by customer_id;



SELECT customer_id, COUNT(v.visit_id) as count_no_trans 
FROM Visits v
LEFT JOIN Transactions t ON v.visit_id = t.visit_id
WHERE transaction_id IS NULL
GROUP BY customer_id;


SELECT customer_id, COUNT(visit_id) as count_no_trans 
FROM Visits
WHERE visit_id NOT IN (
	SELECT visit_id FROM Transactions
	)
GROUP BY customer_id;


SELECT customer_id, COUNT(visit_id) as count_no_trans 
FROM Visits v
WHERE NOT EXISTS (
	SELECT visit_id FROM Transactions t 
	WHERE t.visit_id = v.visit_id
	)
GROUP BY customer_id;


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1633

drop table if exists users;

create table users (
    user_id int,
    user_name varchar(50)
);

drop table if exists register;

create table register (
    contest_id int,
    user_id int
);

insert into users values 
(6,'Alice'),
(2,'Bob  '),
(7,'Alex ');

insert into register values 
(215,6),
(209,2),
(208,2),
(210,6),
(208,6),
(209,7),
(209,6),
(215,7),
(208,7),
(210,2),
(207,2),
(210,7);


select contest_id, round(count(distinct user_id)/(select count(*) from users)*100,2) as percentage
from register 
group by contest_id
order by percentage desc, contest_id;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1667

drop table if exists users;

create table users (
    user_id int,
    name varchar(50)
);

insert into users values 
(1,'aLice'),
(2,'bOB'),
(3,'m');


SELECT user_id, 
    CONCAT(UPPER(SUBSTRING(name,1,1)), LOWER(SUBSTRING(name,2))) as name
FROM Users
ORDER BY user_id;


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1683

drop table if exists tweets;

create table tweets (
    tweet_id int,
    content varchar(50)
);

insert into tweets values 
(1,'Vote for Biden'),
(2,'Let us make America great again!');


select tweet_id from tweets where LENGTH(content) > 15;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1729

drop table if exists followers;

create table followers (
    user_id int,
    follower_id int
);

insert into followers values 
(0,1),
(1,0),
(2,0),
(2,1);


select user_id, count(follower_id) as followers_count
from 
Followers
group by user_id
order by user_id;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1731

drop table if exists employees;

create table employees (
    employee_id int,
    name varchar(50),
    reports_to int,
    age int
);

insert into employees values 
(9,'Hercy  ',null,43),
(6,'Alice  ',9   ,41),
(4,'Bob    ',9   ,36),
(2,'Winston',null,37);


-- couldn't figure this out, so copied the solution from leetcode below.

select distinct(e2.employee_id) , e2.name,
round(count(e1.employee_id) over(partition by e1.reports_to),0) reports_count,
round(avg(e1.age)over(partition by e1.reports_to),0) average_age
from employees e1 left join employees e2 on e1.reports_to=e2.employee_id
where e1.reports_to is not null
order by e2.employee_id;



-- explanation or step by step output below.


mysql> select * from employees e1 left join employees e2 on e1.reports_to=e2.employee_id;
+-------------+---------+------------+------+-------------+---------+------------+------+
| employee_id | name    | reports_to | age  | employee_id | name    | reports_to | age  |
+-------------+---------+------------+------+-------------+---------+------------+------+
|           9 | Hercy   |       NULL |   43 |        NULL | NULL    |       NULL | NULL |
|           6 | Alice   |          9 |   41 |           9 | Hercy   |       NULL |   43 |
|           4 | Bob     |          9 |   36 |           9 | Hercy   |       NULL |   43 |
|           2 | Winston |       NULL |   37 |        NULL | NULL    |       NULL | NULL |
+-------------+---------+------------+------+-------------+---------+------------+------+
4 rows in set (0.00 sec)


mysql> select distinct(e2.employee_id) , e2.name,
    -> round(count(e1.employee_id) over(partition by e1.reports_to),0) reports_count,
    -> round(avg(e1.age)over(partition by e1.reports_to),0) average_age
    -> from employees e1 left join employees e2 on e1.reports_to=e2.employee_id;
+-------------+---------+---------------+-------------+
| employee_id | name    | reports_count | average_age |
+-------------+---------+---------------+-------------+
|        NULL | NULL    |             2 |          40 |
|           9 | Hercy   |             2 |          39 |
+-------------+---------+---------------+-------------+
2 rows in set (0.00 sec)

mysql> select distinct(e2.employee_id) , e2.name,
    -> round(count(e1.employee_id) over(partition by e1.reports_to),0) reports_count,
    -> round(avg(e1.age)over(partition by e1.reports_to),0) average_age
    -> from employees e1 left join employees e2 on e1.reports_to=e2.employee_id
    -> where e1.reports_to is not null
    -> order by e2.employee_id;
+-------------+---------+---------------+-------------+
| employee_id | name    | reports_count | average_age |
+-------------+---------+---------------+-------------+
|           9 | Hercy   |             2 |          39 |
+-------------+---------+---------------+-------------+
1 row in set (0.00 sec)


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1757

drop table if exists products;

create table products (
    product_id int,
    low_fats enum('Y','N'),
    recyclable enum('Y','N')
);

insert into products values 
(0,'Y','N'),
(1,'Y','Y'),
(2,'N','Y'),
(3,'Y','Y'),
(4,'N','N');


select product_id from products where low_fats='Y' and recyclable ='Y';


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1789

drop table if exists employee;

create table employee (
    employee_id int,
    department_id int,
    primary_flag varchar(50)
);

insert into employee values 
(1,1,'N'),
(2,1,'Y'),
(2,2,'N'),
(3,3,'N'),
(4,2,'N'),
(4,3,'Y'),
(4,4,'N');


-- below was your try to see what error you will get. you already knew that this will not work as you cannot specify having and
-- where clause in the same base query.
select employee_id, department_id from employee group by employee_id where count(employee_id) = 1 or primary_flag = 'Y';


SELECT employee_id, department_id
FROM Employee
WHERE primary_flag='Y' OR 
    employee_id in
    (SELECT employee_id
    FROM Employee
    Group by employee_id
    having count(employee_id)=1)



select employee_id , department_id
from employee
where primary_flag='Y'
group by employee_id

UNION

select employee_id , department_id
from employee
group by employee_id
having count(employee_id) =1

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1907

drop table if exists accounts;

create table accounts (
    account_id int,
    income int
);

insert into accounts values 
(3,108939),
(2,12747 ),
(8,87709 ),
(6,91796 );



WITH cte AS (
    SELECT 
        CASE 
            WHEN income < 20000 THEN 'Low Salary'
            WHEN income BETWEEN 20000 AND 50000 THEN 'Average Salary'
            ELSE 'High Salary'
        END AS category, 
        account_id
    FROM accounts
), 
CategoryList AS (
    SELECT 'Low Salary' AS category
    UNION ALL
    SELECT 'Average Salary'
    UNION ALL
    SELECT 'High Salary'
)
SELECT 
    cl.category, 
    COUNT(cte.account_id) AS account_count
FROM CategoryList cl 
LEFT JOIN cte ON cl.category = cte.category
GROUP BY cl.category
ORDER BY cl.category;





SELECT "Low Salary" AS category,
       sum(income < 20000) AS accounts_count
  FROM Accounts

UNION

SELECT "Average Salary" AS category,
       sum(income BETWEEN 20000 AND 50000) AS accounts_count
  FROM Accounts

UNION

SELECT "High Salary" AS category,
       sum(income > 50000) AS accounts_count
  FROM Accounts;




select "High Salary" as category,
        COUNT(income) as accounts_count
from Accounts
where income >50000
UNION
select "Low Salary" as category,
        COUNT(income) as accounts_count
from Accounts
where income <20000
UNION
select "Average Salary" as category,
        COUNT(income) as accounts_count
from Accounts
where income >=20000 and income <=50000;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1934

drop table if exists signups;

create table signups (
    user_id int,
    time_stamp datetime
);

drop table if exists confirmations;

create table confirmations (
    user_id int,
    time_stamp datetime,
    action enum('confirmed','timeout')
);

insert into signups values 
(3,'2020-03-21 10:16:13'),
(7,'2020-01-04 13:57:59'),
(2,'2020-07-29 23:09:44'),
(6,'2020-12-09 10:39:37');

insert into confirmations values 
(3,'2021-01-06 03:30:46','timeout'),
(3,'2021-07-14 14:00:00','timeout'),
(7,'2021-06-12 11:57:29','confirmed'),
(7,'2021-06-13 12:58:28','confirmed'),
(7,'2021-06-14 13:59:27','confirmed'),
(2,'2021-01-22 00:00:00','confirmed'),
(2,'2021-02-28 23:59:59','timeout');


-- was not able to achieve the correct answer. Below was my failed try
with cte as (select p.user_id, p.time_stamp, a.time_stamp as confirmations_timestamp, a.action 
from signups p left join confirmations a using (user_id)),
cte2 as (select user_id, count(action) as total_action, action from cte group by user_id),
cte3 as (select distinct user_id, action, count(action) as specific_action, total_action from cte2 group by user_id,action)
select * from cte3;


https://leetcode.com/problems/confirmation-rate/solutions/6543578/step-by-step-breakdown-visualized-sql-guide-code-diagrams-97-87/
SELECT
    s.user_id,
    ROUND(AVG(
            CASE 
                WHEN c.action = 'confirmed' THEN 1.00
                ELSE 0
            END
        ),
        2
    ) as confirmation_rate
FROM
    Signups s
LEFT JOIN
    Confirmations c
    ON
    s.user_id = c.user_id
GROUP BY
    s.user_id;



select s.user_id , round(avg(if(c.action='confirmed',1,0)),2) as confirmation_rate
from signups s 
left join confirmations c
on s.user_id = c.user_id
group by s.user_id



+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

1978

drop table if exists employees;

create table employees (
    employee_id int,
    name varchar(50),
    manager_id int,
    salary int
);

insert into employees values 
(3 ,'Mila     ',9   ,60301),
(12,'Antonella',null,31000),
(13,'Emery    ',null,67084),
(1 ,'Kalel    ',11  ,21241),
(9 ,'Mikaela  ',null,50937),
(11,'Joziah   ',6   ,28485);



select employee_id 
from employees 
where salary < 30000 and 
manager_id not in (select distinct employee_id from employees);


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

2356

drop table if exists teacher;

create table teacher (
    teacher_id int,
    subject_id int,
    dept_id int
);

insert into teacher values 
(1,2,3),
(1,2,4),
(1,3,3),
(2,1,1),
(2,2,1),
(2,3,1),
(2,4,1);


select teacher_id, count(distinct subject_id) as cnt from teacher group by teacher_id;

+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++





stratascratch

https://www.youtube.com/watch?v=WS0fM1agxTk&list=PLv6MQO1Zzdmq5w4YkdkWyW8AaWatSQ0kX&index=1

Recommendation system

-- You are given the list of Facebook friends and the list of Facebook pages that users follow. 
-- Your task is to create a new recommendation system for Facebook. 
-- For each Facebook user, find pages that this user doesnt follow but at least one of their friends does. 
-- Output the user ID and the ID of the page that should be recommended to this user.

-- Tables: users_friends, users_pages


-- users_friends
-- friend_id:
-- bigint
-- user_id:
-- bigint


-- users_pages
-- page_id:
-- bigint
-- user_id:
-- bigint


-- there is one table "user_friends" with columns "user_id" and "friend_id", 
-- one more table is "user_pages" with columns "user_id" and "page_id"


-- "exists" is different from the "In / not in" command, exists returns true even if a single record is present in the sub query.

-- here we are first joining the 2 tables, but note that the join key we are comparing between friend_id and user_id. 
-- Thats because we need to map the page_id of friend to the user_id. If we use user_id in both tables for join key,
-- then we will not be able to map the pages that a friend of a user_id likes to that specific user_id.
-- Once that is done, we are using the filter to remove the pages that the user_id already follows by removing the 
-- combination of user_id and page_id, which already exists in the original table.

select distinct f.user.id, p.page_id 
from user_friends f 
join users_pages p 
on f.user_id = p.user_id 
where not exists 
(select * from users_pages pg where pg.user_id=f.user_id and pg.page_id=p.page_id);
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


-- stratascratch

-- File shuffle problem


drop table if exists file_contents;

create table file_contents (
    file_name VARCHAR(50),
    contents varchar(500)
);

insert into file_contents values 
    ('krishiv.txt','He is an intelligent boy studying in Narayana school'),
    ('vivek.txt','He is a very good father with a slight tantrums and naughtiness')
;




select concat('["',replace(lower(contents),' ','","'),'"]') from file_contents 
where file_name like 'vivek%';
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
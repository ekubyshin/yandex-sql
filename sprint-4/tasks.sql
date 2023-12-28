
-- задание 1
-- выполнялся запрос медленно, потому что каждый раз сканилась таблица на высчитывание нового ордера
-- здесь надо поменять тип данных для столбца order_id, чтобы он был serial
--- но сделать это не просто, вот пример
CREATE SEQUENCE orders_order_id_seq OWNED BY orders.order_id;
SELECT SETVAL('orders_order_id_seq', (select max(order_id) from orders), false);
ALTER TABLE orders ALTER COLUMN order_id SET DEFAULT nextval('orders_order_id_seq');
ALTER TABLE orders ADD PRIMARY KEY (order_id);
alter table orders alter column order_dt set default current_timestamp;
alter table orders drop column city_id;
alter table orders drop column final_cost;
--теперь можно поправить запрос на insert
INSERT INTO orders (user_id, device_type, total_cost)
VALUES(
    '329551a1-215d-43e6-baee-322f2467272d', 
    'Mobile', 1000.00);--кажется, что тут был лишним city_id тк он в users и можно убарь final_cost, тк его можно считать при выставлении скидки


--- задание 2
-- снова плохие данные, которые занимают много места
-- вместо фикс длинны предлагаю использовать varchar, который сэкономит место а следовательно быстрее будут выгребаться данные
-- также в запросах не используются индексы - добавлю
-- преобразую таблицу
alter table users
	alter column user_id type uuid using user_id::text::uuid,
	alter column user_id set default gen_random_uuid(),
	alter column first_name  type varchar(100),
	alter column last_name type varchar(100),
	alter column gender type varchar(10),
	alter column birth_date type date using to_date(birth_date, 'yyyy-mm-dd'),
	alter column registration_date type timestamp with time zone 
	using to_timestamp(registration_date, 'yyyy-mm-dd hh24:mi:ss');
-- также добавляю индексы - тут добавлю функциональный, чтобы выгребать данные по более короткому запросу
create index if not exists users_birth_date2_idx ON users ((date_part('day',birth_date)::text||date_part('month',birth_date)::text));
create index if not exists users_city_id_ix on users(city_id);
-- теперь правлю запрос
SELECT user_id, first_name, last_name, 
    city_id, gender
FROM users
WHERE city_id = 4 AND (date_part('day',birth_date)::text || date_part('month',birth_date)::text) = '3112';
-- чтобы еще быстрее выполнялся запрос, можно сдеалть materialized view, но кажется что и без него уже выполняется быстро
create materialized view new_years_birth_date (user_id, first_name, last_name, 
    city_id, gender) as 
SELECT user_id, first_name, last_name, 
    city_id, gender
FROM users
WHERE city_id = 4 AND (date_part('day',birth_date)::text || date_part('month',birth_date)::text) = '3112';
-- далее вызывать
SELECT *
FROM new_years_birth_date;

-- задание 3
-- здесь вообще не понял в чем проблема и действовал наугад
-- мне не понятна суть использозованя NEXTVAL вместо автоинкримента
-- добавил limit 1 в селект
create index if not exists orders_order_id_idx on orders (order_id);
DROP PROCEDURE public.add_payment;
CREATE PROCEDURE public.add_payment(IN p_order_id bigint, IN p_sum_payment numeric)
    LANGUAGE plpgsql
    AS $$BEGIN
    INSERT INTO order_statuses (order_id, status_id, status_dt)
    VALUES (p_order_id, 2, statement_timestamp());
    
   INSERT INTO payments (payment_id, order_id, payment_sum)
    VALUES (nextval('payments_payment_id_sq'), p_order_id, p_sum_payment);
	
	-- тут кажется, что платежи избыточная таблица, тк она хранит тоже самое, что и sales, но без привязки к юзеру
	-- потому, что при необходимости получения данных, можно взять все в ордере и в платежах
    -- INSERT INTO sales(sale_id, sale_dt, user_id, sale_sum)
    -- SELECT NEXTVAL('sales_sale_id_sq'), _current_time, user_id, p_sum_payment
    -- FROM orders WHERE order_id = p_order_id LIMIT 1;
END;$$;

--- задание 4
-- таблица очень большая и ее надо разбить на партиции
--- разобью по хешу на 3 таблицы
-- также удалю ненужный столбец event_date, тк он дублирует datetime
-- с наследованием лень было разбираться, поэтмоу сделал декларативно с переносом данных ))
alter table user_logs rename to user_logs_old;

create table user_logs (
	visitor_uuid uuid,
    user_id uuid,
    event varchar(128),
    datetime timestamp without time zone,
    log_id bigserial
) PARTITION BY HASH(user_id)

CREATE TABLE user_logs_0 PARTITION OF user_logs
    FOR VALUES WITH (MODULUS 3, REMAINDER 0);
	
CREATE TABLE user_logs_1 PARTITION OF user_logs
    FOR VALUES WITH (MODULUS 3, REMAINDER 1);
	
CREATE TABLE user_logs_2 PARTITION OF user_logs
    FOR VALUES WITH (MODULUS 3, REMAINDER 2);
	
insert into user_logs (visitor_uuid, user_id, event, datetime, log_id)
select visitor_uuid::text::uuid, user_id, event, datetime, log_id from user_logs_old;

drop table user_logs_old;


--- задание 5
--- учитывая, что отчет запрашивается регулярно и в нем не учитываются данные за текущий день
--- то можно создать материализованное представление и обновлять его каждый день, например, с утра
-- создам вьюхи и индекс для быстрого поиска по возрасту
create or replace function calc_user_year(in birth_date TIMESTAMP)
returns int
IMMUTABLE
LANGUAGE sql
AS $$
select date_part('year', age(now() - (1||'day')::interval, birth_date)) 
$$;

create or replace function yesterday(in d timestamp with time zone)
returns BOOLEAN
immutable
language sql
as $$
select d <= date_trunc('day',NOW()) - (1||'second')::interval
$$;

create index if not exists users_date_years on users (calc_user_year(birth_date));
create index if not exists orders_order_dt_yesterday on orders (yesterday(order_dt));
create index if not exists order_items_order_id_idx on order_items(order_id);
create index if not exists dishes_object_id_idx on dishes(object_id);

analyze;

create or replace view user_less_20 as (
	select user_id 
	from users
	where calc_user_year(birth_date) between 1 and 20
);
create or replace view user_less_30 as (
select user_id 
	from users
	where 
		calc_user_year(birth_date) between 21 and 30
);
create or replace view user_less_40 as (
select user_id 
	from users
	where 
		calc_user_year(birth_date) between 31 and 40
);
create or replace view user_more_40 as (
select user_id 
	from users
	where 
		calc_user_year(birth_date) between 41 and 100
);

create or replace view orders_before_today as (
	select 
		o.order_id as order_id,
		o.user_id as user_id,
		date_trunc('day', o.order_dt)::date as day,
		d.spicy,
		d.fish,
		d.meat,
		oi.count
	FROM
		orders o
	JOIN
		order_items oi on oi.order_id = o.order_id
	JOIN
		dishes d on d.object_id = oi.item
	where yesterday(o.order_dt)
);

create or replace view dishes_for_less_20 as (select
	o.day,
	'0-20' as group,
	o.spicy,
	o.fish,
	o.meat,
	o.count
from
	orders_before_today o
WHERE
	o.user_id in (select * from user_less_20)
);

create or replace view dishes_for_less_30 as (select 
	o.day,
	'20-30' as group,
	o.spicy,
	o.fish,
	o.meat,
	o.count
from
	orders_before_today o
WHERE
	o.user_id in (select * from user_less_30)
);

create or replace view dishes_for_less_40 as (select 
	o.day,
	'30-40' as group,
	o.spicy,
	o.fish,
	o.meat,
	o.count
from
	orders_before_today o
WHERE
	o.user_id in (select * from user_less_40)
);

create or replace view dishes_for_more_40 as (select 
	o.day,
	'40-100' as group,
	o.spicy,
	o.fish,
	o.meat,
	o.count
from
	orders_before_today o
WHERE
	o.user_id in (select * from user_more_40)
);

-- тут будет вьюха, которую можно апдейтить раз в день по расписанию
create materialized view users_preferences_report (day, "group", spicy, fish, meat) as (
    with all_orders as (
		select * from dishes_for_less_20
		union
		select * from dishes_for_less_30
		union
		select * from dishes_for_less_40
		union
		select * from dishes_for_more_40
	),
	total as (
		select count(*) from all_orders
	)
	select 
		all_orders.day, 
		all_orders.group,
		((sum(all_orders.spicy)::numeric/ total.count)*100)::numeric(4,2) as spicy,
		((sum(all_orders.fish)::numeric/ total.count)*100)::numeric(4,2) as fish,
		((sum(all_orders.meat)::numeric/ total.count)*100)::numeric(4,2) as meat 
	from all_orders, total
	group by (all_orders.day,all_orders.group, total.count)
	order by all_orders.day,all_orders.group
);

select * from users_preferences_report;
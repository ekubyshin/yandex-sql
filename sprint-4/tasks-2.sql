-- ВОПРОСОВ НЕТ! 
-- Курс просто отличный! Спасибо!


SELECT pg_stat_statements_reset();
-- как искал самые медленные запросы
--- 1. подключил модуль pg_stat
-- 2. сбросил статистику
-- 3. запустил analyze
-- 4. потом выполнил все запросы в скрипте.
-- 5. сделал запрос для вывода 5 самых медлыннх
-- 6. далее explain analyze каждого запроса в отдельности
/*
SELECT  
    query,
    ROUND(mean_exec_time::numeric,2),                
    ROUND(total_exec_time::numeric,2),
    ROUND(min_exec_time::numeric,2), 
    ROUND(max_exec_time::numeric,2),
    calls,
    rows                          
FROM pg_stat_statements
-- Подставьте своё значение dbid.
WHERE dbid = 30391 ORDER BY mean_exec_time DESC
LIMIT 5;
*/


-- 9 самый медленный запрос
-- в ходе анализа установлено, что здесь существует проблема N+1 запрос изза вложенного коррелирующего запрос с подсчетом count
-- которые выполняются при aggregate и проводится seq scan
-- вообще запрос выглядит избыточным, потому что достаточно подсчитать только количество неоплаченных заказов
-- тут можно вообще сделать один селект с join без вложенных запросов, тк нам надо подсчитать заказы со статусом 2
-- но мне не понятно, зачем в запросе присутствует city_id = 1, хотя задача стоит подсчитать просто кол-во неоплаченных заказов
-- без учета конкретного города
-- предлагаю сократить выборку поэтапно
-- 1. выбираем ордера для определенного города
-- 2. далее джойним один раз таблицу и оставляем только ордера с 1 и 2
-- 3. считаем количество для каждого типа
-- 4. считаем разницу между количеством созданых и количеством оплаченных
-- запрос выполняется за 200мс
-- вообще для ускорения запрос хорошо бы повесить индекс на orders.city_id
-- также вижу, что в ордерах индекс висит на поле user_id, следовательно надо его использовать
-- и вместо city_id = 1 искать внутри отобраных юзеров,: которые живут в этом городе
-- но это не дало ощутимого прироста, потому что users не имеет индексов ))

with for_city as (
  select o.order_id
  from orders o
  where o.city_id = 1
),
joined as (
  select f.order_id, os.status_id
  from for_city f
  join order_statuses os on os.order_id = f.order_id
  where status_id between 1 and 2
),
paid as (SELECT
	count(*)
FROM joined f
WHERE f.status_id = 2
),
created as (
  SELECT
	count(*)
FROM joined f
WHERE f.status_id = 1
)
SELECT created.count - paid.count from created, paid;


-- запрос 8
-- здесь в целом таблица избыточная, а столбец по которому ищется имеет тип varchar
-- те здесь для ускорения поидее надо убрать операция приведения типов из varchar в date
-- для этого можно воспользоваться столбцом log_date
-- но тут для большего ускорения, можно повесить индекс на это поле
-- но тк индекс сделан на поле datetime, то проще воспользоваться им
-- поменяв запнос на between, таким образом используя индекс таблицы
select *
from user_logs
where datetime BETWEEN '2021-05-01 00:00:00' AND '2021-05-02 23:59:59';


-- запрос 7
-- здесь основное время тратится на сортировку
-- но тут есть недостаток в самой таблице - отсутствует индекс по визитеру, хотя ищем по нему
-- кроме того, можно ограничить запрос по переоду за который искать
-- в целом с такой таблицей не сделаешь оптимизаций, без соответствующих индексов
with e as (
SELECT event, datetime
FROM user_logs
WHERE visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'
),
c as (
   select count(datetime) from e
)
SELECT * from e
ORDER BY 2
limit (select c.count from c);

-- запрос 2
-- здесь в запросе указано лишнее условие для выполнения фильтрации
-- предлагаю обойтись просто сортировкой и limit 1
-- индексов на соединяемых таблицах нет, поэтому особо не ускорить
-- после того как убрал лишнее6 то запрос стал отрабатывать по индексам
select o.order_id, o.order_dt, o.final_cost, s.status_name
from orders o
JOIN order_statuses os on os.order_id = o.order_id
JOIN statuses s ON s.status_id = os.status_id
WHERE o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid
order by os.status_dt desc limit 1;


-- запрос 15
-- по анализу видно, что запрос отрабатывает снова на seq сканах
-- в тч subsequence scan который выполняется довольно долго
-- имеется коррелирующий запрос и лишние вычисления
-- далее идет соедниенине hash join - надо поработать над этими моментами
-- кроме того расчет среднего лучше вынести в cte
-- в итоге удалось ускорить запрос в 3х раза
with total as (
SELECT oi.item, SUM(oi.count) AS total_sales
	FROM order_items oi
	GROUP BY 1
),
av as (
SELECT SUM(t.total_sales) / COUNT(*) as avg
		FROM total t
),
avgs as (
  select item, total_sales from total where total.total_sales > (select av.avg from av)
)
SELECT d.name, SUM(total_sales) AS orders_quantity
FROM avgs
JOIN dishes d ON d.object_id = avgs.item
group by d.name
order by 2 desc;
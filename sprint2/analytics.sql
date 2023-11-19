-- задание 1
DROP VIEW IF EXISTS cafe.top3_restaurants;
CREATE VIEW cafe.top3_restaurants AS
	WITH 
	av AS (
	SELECT
		r."name",
		r."type",
		ROUND(AVG(s.avg_check), 2) as avg_profit
	FROM cafe.sales AS s
	join cafe.restaurants r using(restaurant_uuid)
	GROUP BY r.type, r.name
	ORDER BY r.type ASC
	),
	res AS (SELECT 
		av.name,
		av.type,
		av.avg_profit,
		ROW_NUMBER() OVER (PARTITION BY av.type ORDER BY av.avg_profit DESC) AS rating
	FROM av)
	SELECT 
		res.name,
		res.type,
		res.avg_profit
	FROM res
	WHERE res.rating <= 3;
	
-- задание 2
DROP MATERIALIZED VIEW IF EXISTS cafe.check_diff;
CREATE VIEW cafe.check_diff AS
WITH y AS (
	SELECT 
		EXTRACT(YEAR FROM s.date) AS year,
		s.restaurant_uuid AS restaurant_uuid,
		ROUND(AVG(s.avg_check), 2) AS avg_check
	FROM cafe.sales s
	GROUP BY restaurant_uuid, year
	ORDER BY year
),
d AS (
	SELECT 
		y.year,
		y.restaurant_uuid,
		y.avg_check,
		LAG(y.avg_check) OVER(PARTITION BY y.restaurant_uuid ORDER BY y.year) AS prev_avg_check
	FROM y
	WHERE y.year < EXTRACT(year FROM CURRENT_DATE)
)
SELECT 
	d.year,
	r.name,
	r.type,
	d.avg_check,
	d.prev_avg_check,
	ROUND(((d.avg_check - d.prev_avg_check) / d.prev_avg_check * 100),2) AS diff
FROM d
JOIN cafe.restaurants r USING(restaurant_uuid)
ORDER BY r.name, d.year;

-- задание 3
SELECT
	r.name,
	COUNT(DISTINCT rm.manager_uuid) AS changed
FROM cafe.restaurant_manager_work_dates rm
JOIN cafe.restaurants r USING(restaurant_uuid)
GROUP BY r.name
ORDER BY changed DESC
LIMIT 3;

-- задание 4
WITH rests_pizza AS (
SELECT
	r.name,
	jsonb_each(r.menu -> 'Пицца') AS pizza
FROM cafe.restaurants r
WHERE menu -> 'Пицца' IS NOT NULL	
),
rest_count AS (
	SELECT 
	rests_pizza.name,
	COUNT(rests_pizza.pizza) AS count
	FROM rests_pizza
	GROUP BY rests_pizza.name
	ORDER BY count DESC
),
rank AS (SELECT 
	rc.name,
	rc.count,
	DENSE_RANK() OVER (ORDER BY rc.count DESC) rank
FROM rest_count rc)
SELECT
	r.name,
	r.count
FROM rank r
WHERE r.rank = 1;

-- задание 5
WITH rests AS (
SELECT
	r.name,
	r.menu -> 'Пицца' AS menu
FROM cafe.restaurants r
WHERE menu -> 'Пицца' IS NOT NULL	
),
prices as (SELECT
	rests.name,
	pizza.key::text AS pizza,
	pizza.value::decimal(6,2) AS price
FROM rests, jsonb_each(menu) AS pizza),
rank as (SELECT 
	p.name,
	p.pizza,
	p.price,
	ROW_NUMBER() OVER(PARTITION BY p.name ORDER BY p.price DESC) AS rank
FROM prices p)
SELECT
	r.name,
	'Пицца',
	r.price
FROM rank r
WHERE r.rank = 1
ORDER BY r.price DESC;

-- задание 6
WITH r_join AS (SELECT 
	r1.name AS name1,
	r2.name AS name2,
	r1.type AS type,
	ST_Distance(r1.location, r2.location) AS distance
FROM cafe.restaurants r1
JOIN cafe.restaurants r2 ON r2."type" = r1."type"
WHERE r1.name != r2.name
)
SELECT 
	rj.name1,
	rj.name2,
	rj.type,
	MIN(rj.distance) AS distance
FROM r_join rj
GROUP BY rj.name1,
	rj.name2,
	rj.type
ORDER BY distance ASC
LIMIT 1

-- задание 7
WITH c AS(SELECT
	d.district_name AS name,
	COUNT(*) AS count
FROM cafe.districts d
JOIN cafe.restaurants r ON ST_Within(r."location"::geometry, d.district_geom)
GROUP BY d.district_name),
min_max as (SELECT
	c.name,
	MAX(c.count) OVER(),
	MIN(c.count) OVER()
FROM c)
SELECT
	mm.name,
	c.count
FROM min_max mm
JOIN c USING(name)
WHERE c.count = mm.max OR c.count = mm.min
ORDER BY c.count DESC;
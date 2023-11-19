INSERT INTO cafe.managers (name, phone)
SELECT DISTINCT manager, manager_phone FROM raw_data.sales;

INSERT INTO cafe.restaurants(name, location, type, menu)
SELECT 
	DISTINCT s.cafe_name, 
	ST_GeomFromText('POINT(' || s.longitude::text || ' ' || s.latitude::text || ')')::geography,
	s.type::cafe.restaurant_type,
	m.menu
FROM raw_data.sales s
JOIN raw_data.menu m ON m.cafe_name = s.cafe_name;

INSERT INTO cafe.sales(restaurant_uuid, date, avg_check)
SELECT 
	restaurant_uuid,
	s.report_date,
	avg_check
FROM 
	cafe.restaurants r
JOIN raw_data.sales s ON s.cafe_name = r."name";

WITH report AS (
	SELECT 
		r.restaurant_uuid,
		m.manager_uuid,
		rs.report_date AS begin_date,
		LEAD(rs.report_date) OVER (PARTITION BY rs.manager_phone ORDER BY rs.report_date) AS end_date
	FROM raw_data.sales rs
	JOIN cafe.managers m ON m.phone = rs.manager_phone
	JOIN cafe.restaurants r ON r."name" = rs.cafe_name
)
INSERT INTO cafe.restaurant_manager_work_dates
SELECT
	report.restaurant_uuid,
	report.manager_uuid,
	MIN(report.begin_date) AS begin_date,
	MAX(report.end_date) AS end_date
FROM report
GROUP BY (report.restaurant_uuid, report.manager_uuid);
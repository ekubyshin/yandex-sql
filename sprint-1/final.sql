-- IMPORT RAW DATA
CREATE SCHEMA raw_data;

create table raw_data.sales (
	id SMALLINT NOT NULL, -- этого будет достаточно, автоинкримент не нужен тк импорт данных
	auto VARCHAR(100) NOT NULL,
	gasoline_consumption FLOAT NULL, -- если делать NUMERIC то падал на импорте целочисленных значений
	price FLOAT NOT NULL, -- тк тут цена и дабл не нужен
	date DATE, -- просто дата
	person VARCHAR(100), -- больше не нужно
	phone VARCHAR(50), -- число не подойдет судя по данны
	discount SMALLINT DEFAULT 0, -- достаточно для скидки
	brand_origin VARCHAR(50) -- достаточно
);

COPY raw_data(id, auto, gasoline_consumption, price, date, person, phone, discount, brand_origin)
FROM '/Path/To/File/cars.csv' WITH DELIMITER ',' CSV HEADER NULL 'null';

-- исправляем баги в бд
UPDATE raw_data.sales
SET brand_origin = 'Germany'
WHERE raw_data.sales.auto LIKE 'Porsche 911%';

-- чтобы каждый раз не сплитить данные и не городить монструзоные селекты
-- решил немного преобразовать данные и сформировать промежуточную таблицу
CREATE TABLE raw_data.splitted_sales (
	id SMALLINT NOT NULL,
	car_brand VARCHAR(50) NOT NULL,
	car_model VARCHAR(50) NOT NULL,
	car_color VARCHAR(10) NOT NULL,
	brand_origin VARCHAR(50),
	gasoline_consumption NUMERIC(3,1) DEFAULT 0.0,
	price NUMERIC(9,2) NOT NULL,
	discounted_price NUMERIC(9,2) NOT NULL,
	discount SMALLINT DEFAULT 0,
	sale_date DATE NOT NULL,
	first_name VARCHAR(50) NOT NULL,
	last_name VARCHAR(50) NOT NULL,
	phone VARCHAR(50) NOT NULL
);

-- заполняю промежуточную таблицу данными
INSERT INTO raw_data.splitted_sales(
	id,
	car_brand,
	car_model,
	car_color,
	brand_origin,
	gasoline_consumption,
	price,
	discounted_price,
	discount,
	sale_date,
	first_name,
	last_name,
	phone
) SELECT 
		sales.id AS id,
		SPLIT_PART(sales.auto, ' ', 1) AS car_brand,
		LTRIM(SPLIT_PART(sales.auto, ',', 1), SPLIT_PART(sales.auto, ' ', 1)) AS car_model,
		SPLIT_PART(sales.auto, ',', 2) AS car_color,
		sales.brand_origin AS brand_origin,
		sales.gasoline_consumption::NUMERIC(3,1) AS gasoline_consumption,
		CASE
			WHEN sales.discount < 1.0 THEN sales.price
			ELSE sales.price * 100 / (100 - sales.discount::FLOAT)
		END::NUMERIC(9,2) as price,
		sales.price::NUMERIC(9,2) AS discounted_price,
		sales.discount AS discount,
		sales.date AS sale_date,
		SPLIT_PART(sales.person, ' ', 1) AS first_name,
		SPLIT_PART(sales.person, ' ', 2) AS last_name,
		sales.phone as phone
	FROM raw_data.sales as sales
	ORDER BY sale_date;

-- create schema and tables
CREATE SCHEMA car_shop;

-- таблица клиентов. Судя по анализу сырых данных - скидка привязана не к клиенту, а к продаже конкретной машины
-- кроме того, нет четкой градации скидок, а значит не надо привязывать скидку к клиенту
-- скидка в автосалонах обычно зависит от различных акций
CREATE TABLE car_shop.clients (
	id SERIAL PRIMARY KEY, -- тут будет автоинкримент и связь по этому ключу
	first_name VARCHAR(30) NOT NULL, -- здесь выбрал, потому что для именя нет смысла брать текст
	last_name VARCHAR(30) NOT NULL, -- аналогично имени
	phone VARCHAR(50) NOT NULL -- должно уместиться
);

-- справочник стран
CREATE TABLE car_shop.countries (
	id SMALLSERIAL PRIMARY KEY, -- будем вбивать по id чтобы не было ошибок в названии стран
	name VARCHAR(30) NOT NULL UNIQUE
);

-- таблица брендов
CREATE TABLE car_shop.brands (
	id SMALLSERIAL PRIMARY KEY, -- связь по ключу для минимизации данных тк по названию бренда будет больше
	name VARCHAR(50) NOT NULL UNIQUE, -- не должно быть повторяющихся брендов с разными странами
	country_id SMALLINT REFERENCES car_shop.countries(id) ON DELETE SET NULL -- страна
);

-- решил создать справочник цветов, чтобы меньше жрало места
-- кроме того, чтобы при вбивании данных не было ошибок в цветах
CREATE TABLE car_shop.colors (
	id SMALLSERIAL PRIMARY KEY,
	name VARCHAR(50) NOT NULL UNIQUE
);

-- это больше как справочник машин и расход
-- цвет отдельно, тк одна и таже модель + название могут иметь разные цвета
-- потребление бензина среднее по модификациям, те нет привязки к модификации, поэтому можно засунуть в эту таблицу
CREATE TABLE car_shop.cars (
	id SMALLSERIAL PRIMARY KEY, -- связь по этому ключу
	name VARCHAR(50) NOT NULL, -- здесь уникально не важна, тк в рамках разных брендов названия могут быть одинаковые
	brand_id SMALLINT REFERENCES car_shop.brands(id) ON DELETE SET NULL,
	gasoline_consumption NUMERIC(3, 1) DEFAULT 0.0 CHECK (gasoline_consumption >= 0.0) -- в исходной таблице это флоат, но такая точность избыточная. NULL меняю на 0
);

-- таблица продаж
-- один клиент может покупать в разные дни
-- либо купить несколько машин в один день
CREATE TABLE car_shop.sales (
	id SERIAL PRIMARY KEY,--мб много
	client_id SMALLINT REFERENCES car_shop.clients(id) ON DELETE CASCADE,
	car_id SMALLINT REFERENCES car_shop.cars(id) ON DELETE CASCADE, -- связь с машинами
	color_id SMALLINT REFERENCES car_shop.colors(id) ON DELETE CASCADE,
	date DATE DEFAULT CURRENT_DATE,
	car_price NUMERIC(9,2) NOT NULL CHECK (car_price > 0.0), -- цена за единицу
	discount SMALLINT DEFAULT 0 CHECK (discount >= 0),-- скидка на машину
	discounted_price DECIMAL(9,2) DEFAULT 0 CHECK (discounted_price <= car_price) -- цена со скидкой
);


-- заполняем клиентов
-- тут решил для надежности сделать селект из селекта
-- но вроде просто запрос с DISTINCT SPLIT_PART давал такой же результат
INSERT INTO car_shop.clients (
	first_name,
	last_name,
	phone
) SELECT SPLIT_PART(persons.person, ' ', 1) AS first_name,
		SPLIT_PART(persons.person, ' ', 2) AS last_name,
		persons.phone
	FROM 
	(
	SELECT 
		DISTINCT raw.person,
		raw.phone
	FROM raw_data.sales AS raw
) AS persons;

-- заполняю справочник стран
INSERT INTO car_shop.countries(
	name
) SELECT DISTINCT raw.brand_origin FROM raw_data.splitted_sales AS raw;

-- заполняю бренды
INSERT INTO car_shop.brands(
	name,
	country_id
) SELECT brands.name as name, c.id as country_id
	FROM car_shop.countries c
	JOIN
		(SELECT DISTINCT raw.car_brand AS name,
			raw.brand_origin AS country
			FROM raw_data.splitted_sales as raw) AS brands ON brands.country = c.name;
			
-- заполняю справочник цветов
INSERT INTO car_shop.colors (
	name
) SELECT DISTINCT raw.car_color AS name FROM raw_data.splitted_sales AS raw;

-- заполняю таблицу машин
INSERT INTO car_shop.cars (
	name,
	brand_id,
	gasoline_consumption
) SELECT 
	cars.model AS name, 
	b.id as brand_id,
	cars.gasoline_consumption AS gasoline_consumption
	FROM car_shop.brands b
	JOIN
		(SELECT DISTINCT raw.car_model AS model,
					raw.car_brand AS brand,
					raw.gasoline_consumption AS gasoline_consumption
					FROM raw_data.splitted_sales AS raw) AS cars
	ON b.name = cars.brand;

-- заполняю продажи
INSERT INTO car_shop.sales (
	client_id,
	car_id,
	color_id,
	date,
	car_price,
	discount,
	discounted_price
) SELECT
		clients.id As client_id,
		cars.id AS car_id,
		colors.id AS color_id,
		raw_stocks.date AS date,
		raw_stocks.price AS car_price,
		raw_stocks.discount AS discount,
		raw_stocks.discounted_price AS discounted_price
	FROM
		(SELECT 
				rs.car_brand AS brand,
				rs.car_model AS model,
				rs.car_color AS color,
				rs.price AS price,
		 		rs.discount AS discount,
		 		rs.discounted_price AS discounted_price,
		 		rs.phone AS phone,
		 		rs.sale_date AS date
			FROM raw_data.splitted_sales AS rs) AS raw_stocks
	JOIN car_shop.clients AS clients ON clients.phone = raw_stocks.phone
	JOIN car_shop.colors AS colors ON colors.name = raw_stocks.color
	JOIN car_shop.brands AS brands ON brands.name = raw_stocks.brand
	JOIN car_shop.cars AS cars ON cars.name = raw_stocks.model AND cars.brand_id = brands.id;
	
-- задача 1
SELECT COUNT(*) AS nulls_percentage_gasoline_consumption FROM raw_data.sales AS raw
WHERE raw.gasoline_consumption IS NULL;

-- задача 2
SELECT 
	brands.name AS brand_name,
	years AS year,
	AVG(sales.car_price)::NUMERIC(9,2) AS price_avg -- не знаю тут надо было со скидкой или без скидки
FROM generate_series(2015, 2022, 1) AS years
JOIN car_shop.sales AS sales ON EXTRACT(YEAR FROM sales.date) = years
JOIN car_shop.cars AS cars ON cars.id = sales.car_id
JOIN car_shop.brands AS brands ON brands.id = cars.brand_id
GROUP BY years, brands.name
ORDER BY brand_name ASC, year ASC;

-- задача 3
SELECT
	EXTRACT(MONTH FROM dates.date) AS month,
	EXTRACT(YEAR FROM dates.date) AS year,
	CASE
		WHEN AVG(sales.car_price) IS NULL THEN 0.0
		ELSE AVG(sales.car_price)::NUMERIC(9,2)
	END AS price_avg
FROM (select GENERATE_SERIES('2022-01-01'::date, '2022-12-01'::date, '1 month')::date AS date) AS dates
LEFT JOIN car_shop.sales AS sales ON DATE_TRUNC('month', sales.date)::DATE = dates.date -- тут была ошибка надо было month. Думал, что если day то обнуляет дни ))
GROUP BY month, year
ORDER BY month ASC;

-- задача 4
SELECT 
	(clients.first_name || ' ' || clients.last_name) AS person,
	STRING_AGG(CONCAT_WS(', ', cars.name, brands.name), ', ') AS cars
FROM car_shop.sales AS sales
JOIN car_shop.clients AS clients ON clients.id = sales.client_id
JOIN car_shop.cars AS cars ON cars.id = sales.car_id
JOIN car_shop.brands AS brands ON brands.id = cars.brand_id
GROUP BY person
ORDER BY person ASC;

-- задача 5
SELECT
	countries.name AS brand_origin,
	MAX(sales.car_price) AS price_max,
	MIN(sales.car_price) AS price_min
FROM
	car_shop.sales AS sales
JOIN car_shop.cars AS cars ON cars.id = sales.car_id
JOIN car_shop.brands AS brands ON brands.id = cars.brand_id
JOIN car_shop.countries AS countries ON countries.id = brands.country_id
GROUP BY brand_origin;

-- задача 6
SELECT 
	COUNT(DISTINCT clients.phone) AS persons_from_usa_count
FROM 
	car_shop.clients AS clients
WHERE 
	clients.phone LIKE '+1%';
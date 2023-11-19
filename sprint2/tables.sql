CREATE TYPE cafe.restaurant_type AS ENUM ('coffee_shop', 'restaurant', 'bar', 'pizzeria');

CREATE TABLE IF NOT EXISTS cafe.restaurants(
	restaurant_uuid UUID NOT NULL PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
	name VARCHAR(50) NOT NULL,
	location geography(point),
	type cafe.restaurant_type NOT NULL,
	menu JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS cafe.managers (
	manager_uuid UUID PRIMARY KEY NOT NULL DEFAULT GEN_RANDOM_UUID(),
	name VARCHAR(50) NOT NULL,
	phone VARCHAR(30) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS cafe.restaurant_manager_work_dates (
	restaurant_uuid UUID NOT NULL REFERENCES cafe.restaurants ON DELETE CASCADE,
	manager_uuid UUID NOT NULL REFERENCES cafe.managers ON DELETE CASCADE,
	begin_date DATE NOT NULL DEFAULT CURRENT_DATE,
	end_date DATE CHECK (begin_date < end_date),
	PRIMARY KEY (restaurant_uuid, manager_uuid)
);

CREATE TABLE IF NOT EXISTS cafe.sales (
	restaurant_uuid UUID NOT NULL REFERENCES cafe.restaurants ON DELETE CASCADE,
	date DATE NOT NULL DEFAULT CURRENT_DATE,
	avg_check DECIMAL(9,2) NOT NULL DEFAULT 0.0,
	PRIMARY KEY (restaurant_uuid, date)
);
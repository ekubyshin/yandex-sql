SELECT
			stocks.sku_id as sku_id,
			sales.id AS sale_id,
			old_sales.price as car_price,
			old_sales.discount as discount,
			old_sales.discounted_price as discounted_price
		FROM raw_data.splitted_sales AS old_sales
		JOIN car_shop.brands AS brands ON brands.name = old_sales.car_brand
		JOIN car_shop.cars AS cars ON cars.name = old_sales.car_model and cars.brand_id = brands.id
		JOIN car_shop.colors AS colors ON colors.name = old_sales.car_color
		JOIN car_shop.clients AS clients ON clients.phone = old_sales.phone
		JOIN car_shop.sales AS sales ON sales.client_id = clients.id and old_sales.sale_date = sales.date
		JOIN car_shop.stocks AS stocks ON stocks.color_id = colors.id and stocks.car_id = cars.id and stocks.price = old_sales.price
		ORDER BY old_sales.sale_date;

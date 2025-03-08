-- запрос для проверки отсутствия NULL в новых данных

with cte AS (
SELECT  order_id,
        order_created_date,
        order_completion_date,
        order_status,
        craftsman_id,
        craftsman_name,
        craftsman_address,
        craftsman_birthday,
        craftsman_email,
        product_id,
        product_name,
        product_description,
        product_type,
        product_price,
        customer_id, 
        customer_name,
        customer_address,
        customer_birthday,
        customer_email 
FROM source1.craft_market_wide
UNION
SELECT  t2.order_id,
        t2.order_created_date,
        t2.order_completion_date,
        t2.order_status,
        t1.craftsman_id,
        t1.craftsman_name,
        t1.craftsman_address,
        t1.craftsman_birthday,
        t1.craftsman_email,
        t1.product_id,
        t1.product_name,
        t1.product_description,
        t1.product_type,
        t1.product_price,
        t2.customer_id,
        t2.customer_name,
        t2.customer_address,
        t2.customer_birthday,
        t2.customer_email 
FROM source2.craft_market_masters_products t1
	INNER JOIN source2.craft_market_orders_customers t2 
	ON t2.product_id = t1.product_id AND t1.craftsman_id = t2.craftsman_id 
UNION
SELECT  t1.order_id,
        t1.order_created_date,
        t1.order_completion_date,
        t1.order_status,
        t2.craftsman_id,
        t2.craftsman_name,
        t2.craftsman_address,
        t2.craftsman_birthday,
        t2.craftsman_email,
        t1.product_id,
        t1.product_name,
        t1.product_description,
        t1.product_type,
        t1.product_price,
        t3.customer_id,
        t3.customer_name,
        t3.customer_address,
        t3.customer_birthday,
        t3.customer_email
FROM source3.craft_market_orders t1
	INNER JOIN source3.craft_market_craftsmans t2 ON t1.craftsman_id = t2.craftsman_id 
    INNER JOIN source3.craft_market_customers t3 ON t1.customer_id = t3.customer_id
UNION
SELECT  t1.order_id,
		t1.order_created_date,
		t1.order_completion_date,
		t1.order_status,
		t1.craftsman_id,
		t1.craftsman_name,
		t1.craftsman_address,
		t1.craftsman_birthday,
		t1.craftsman_email,
		t1.product_id,
		t1.product_name,
		t1.product_description,
		t1.product_type,
		t1.product_price,
		t2.customer_id,
		t2.customer_name,
		t2.customer_address,
		t2.customer_birthday,
		t2.customer_email 
FROM external_source.craft_products_orders t1
	INNER JOIN external_source.customers t2 ON t1.customer_id = t2.customer_id
)
select * from cte
WHERE
order_id IS NULL OR 
order_created_date IS NULL OR 
order_status IS NULL OR 
craftsman_id IS NULL OR
craftsman_name IS NULL OR 
craftsman_address IS NULL OR 
craftsman_birthday IS NULL OR
craftsman_email IS NULL OR
product_id IS NULL OR 
product_name IS NULL OR 
product_description IS NULL OR
product_type IS NULL OR 
product_price IS NULL OR 
customer_id IS NULL OR
customer_name IS NULL OR
customer_address IS NULL OR
customer_birthday IS NULL OR
customer_email IS NULL
limit 5;
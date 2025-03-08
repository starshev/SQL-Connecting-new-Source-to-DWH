-- запрос для инкрементального обновления витрины dwh.customer_report_datamart

-- дельта (все новые данные в dwh, которых нет в последней версии витрины)
WITH dwh_delta AS (
SELECT 
	dc.customer_id AS customer_id,
	dc.customer_name AS customer_name,
	dc.customer_address AS customer_address,
	dc.customer_birthday AS customer_birthday,
	dc.customer_email AS customer_email,
	dp.product_id AS product_id,
	dp.product_price AS product_price,
	dp.product_type AS product_type,
	fo.order_id AS order_id,
	fo.order_completion_date - fo.order_created_date AS diff_order_date,
	fo.order_status AS order_status,
	dc2.craftsman_id,
	TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
	crd.customer_id AS exists_customer_id,
	dc.load_dttm as customer_load_dttm,
	fo.load_dttm as order_load_dttm,
	dp.load_dttm as product_load_dttm,
	dc2.load_dttm craftsman_load_dttm
FROM dwh.d_customer dc
INNER JOIN dwh.f_order fo ON dc.customer_id = fo.customer_id 
INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id 
INNER JOIN dwh.d_craftsman dc2 ON fo.craftsman_id = dc2.craftsman_id 
LEFT JOIN dwh.customer_report_datamart crd ON dc.customer_id = crd.customer_id
WHERE dc.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)
OR fo.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)
OR dp.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)
OR dc2.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)
),
-- список заказчиков из дельты, которые присутствуют в последней версии витрины
existing_customers AS (
SELECT customer_id FROM dwh_delta WHERE exists_customer_id IS NOT NULL
),
-- часть дельты для вставки в витрину (данные только по новым заказчикам)
delta_for_insert AS (
SELECT DISTINCT ON (T4.customer_id)
	T4.customer_id,
	T4.customer_name,
	T4.customer_address,
	T4.customer_birthday,
	T4.customer_email,
	T4.customer_money,
	T4.platform_money,
	T4.count_order,
	T4.avg_price_order,
	T4.median_time_order_completed,
	T4.product_type AS top_product_type,
	T4.craftsman_id AS top_craftsman_id,
	T4.count_order_created,
	T4.count_order_in_progress,
	T4.count_order_delivery,
	T4.count_order_done,
	T4.count_order_not_done,
	T4.report_period
FROM (
(
SELECT
	customer_id,
	/* персональные данные подтягиваем из самого свежего заказа (на случай если в дельте их несколько от одного заказчика),
    чтобы в витрине они были самые актуальные + чтобы не было дубликатов customer_id (в случае разных реквизитов) */
	(SELECT customer_name FROM dwh_delta AS sub WHERE sub.customer_id = dwh_delta.customer_id
	ORDER BY order_load_dttm DESC LIMIT 1) AS customer_name,
	(SELECT customer_address FROM dwh_delta AS sub WHERE sub.customer_id = dwh_delta.customer_id
	ORDER BY order_load_dttm DESC LIMIT 1) AS customer_address,
	(SELECT customer_birthday FROM dwh_delta AS sub WHERE sub.customer_id = dwh_delta.customer_id
	ORDER BY order_load_dttm DESC LIMIT 1) AS customer_birthday,
	(SELECT customer_email FROM dwh_delta AS sub WHERE sub.customer_id = dwh_delta.customer_id
	ORDER BY order_load_dttm DESC LIMIT 1) AS customer_email,
	SUM(product_price) AS customer_money,
	(SUM(product_price) * 0.1) AS platform_money,
	COUNT(order_id) AS count_order,
	AVG(product_price) AS avg_price_order,
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY diff_order_date) AS median_time_order_completed,
	SUM(CASE order_status WHEN 'created' THEN 1 ELSE 0 END) AS count_order_created,
	SUM(CASE order_status WHEN 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
	SUM(CASE order_status WHEN 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
	SUM(CASE order_status WHEN 'done' THEN 1 ELSE 0 END) AS count_order_done,
	SUM(CASE order_status WHEN 'not done' THEN 1 ELSE 0 END) AS count_order_not_done,
	report_period AS report_period
FROM dwh_delta
WHERE exists_customer_id IS NULL
GROUP BY customer_id, report_period
) AS T1
INNER JOIN (
SELECT     -- здесь считаем кол-во заказов каждого заказчика по категориям товаров
	dd.customer_id AS customer_id_for_product_type, 
	dd.product_type, 
	COUNT(dd.product_id) AS product_type_count
	FROM dwh_delta AS dd
	GROUP BY dd.customer_id, dd.product_type
) AS T2 ON T1.customer_id = T2.customer_id_for_product_type
INNER JOIN (
SELECT     -- здесь считаем кол-во заказов каждого заказчика по мастерам
	dd.customer_id AS customer_id_for_craftsman, 
	dd.craftsman_id, 
	COUNT(dd.product_id) AS craftsman_count
	FROM dwh_delta AS dd
	GROUP BY dd.customer_id, dd.craftsman_id
) AS T3 ON T1.customer_id = T3.customer_id_for_craftsman
) AS T4
ORDER BY T4.customer_id, T4.product_type_count DESC, T4.craftsman_count DESC
),
-- часть дельты для обновления витрины (данные только по уже существующим в витрине заказчикам)
delta_for_update AS (
SELECT DISTINCT ON (T5.customer_id)
	T5.customer_id,
	T5.customer_name,
	T5.customer_address,
	T5.customer_birthday,
	T5.customer_email,
	T5.customer_money,
	T5.platform_money,
	T5.count_order,
	T5.avg_price_order,
	T5.median_time_order_completed,
	T5.product_type AS top_product_type,
	T5.craftsman_id AS top_craftsman_id,
	T5.count_order_created,
	T5.count_order_in_progress,
	T5.count_order_delivery,
	T5.count_order_done,
	T5.count_order_not_done,
	T5.report_period
FROM (
(
SELECT
	T1.customer_id,
	/* персональные данные подтягиваем из самого свежего заказа (на случай если в дельте их несколько от одного заказчика),
    чтобы в витрине они были самые актуальные + чтобы не было дубликатов customer_id (в случае разных реквизитов) */
	(SELECT customer_name FROM dwh_delta AS sub WHERE sub.customer_id = T1.customer_id
	ORDER BY order_load_dttm DESC LIMIT 1) AS customer_name,
	(SELECT customer_address FROM dwh_delta AS sub WHERE sub.customer_id = T1.customer_id
	ORDER BY order_load_dttm DESC LIMIT 1) AS customer_address,
	(SELECT customer_birthday FROM dwh_delta AS sub WHERE sub.customer_id = T1.customer_id
	ORDER BY order_load_dttm DESC LIMIT 1) AS customer_birthday,
	(SELECT customer_email FROM dwh_delta AS sub WHERE sub.customer_id = T1.customer_id
	ORDER BY order_load_dttm DESC LIMIT 1) AS customer_email,
	SUM(T1.product_price) AS customer_money,
	(SUM(T1.product_price) * 0.1) AS platform_money,
	COUNT(T1.order_id) AS count_order,
	AVG(T1.product_price) AS avg_price_order,
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY T1.diff_order_date) AS median_time_order_completed,
	SUM(CASE T1.order_status WHEN 'created' THEN 1 ELSE 0 END) AS count_order_created,
	SUM(CASE T1.order_status WHEN 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
	SUM(CASE T1.order_status WHEN 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
	SUM(CASE T1.order_status WHEN 'done' THEN 1 ELSE 0 END)AS count_order_done,
	SUM(CASE T1.order_status WHEN 'not done' THEN 1 ELSE 0 END) AS count_order_not_done,
	T1.report_period
FROM (
SELECT
	dc.customer_id AS customer_id,
	dc.customer_name AS customer_name,
	dc.customer_address AS customer_address,
	dc.customer_birthday AS customer_birthday,
	dc.customer_email AS customer_email,
	dp.product_id AS product_id,
	dp.product_price AS product_price,
	dp.product_type AS product_type,
	fo.order_id AS order_id,
	fo.order_completion_date - fo.order_created_date AS diff_order_date,
	fo.order_status AS order_status,
	dc2.craftsman_id,
	TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
	dc.load_dttm as customer_load_dttm,
	fo.load_dttm as order_load_dttm,
	dp.load_dttm as product_load_dttm,
	dc2.load_dttm craftsman_load_dttm
FROM dwh.d_customer dc
INNER JOIN dwh.f_order fo ON dc.customer_id = fo.customer_id 
INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id 
INNER JOIN dwh.d_craftsman dc2 ON fo.craftsman_id = dc2.craftsman_id 
INNER JOIN existing_customers ec ON dc.customer_id = ec.customer_id
) AS T1
GROUP BY customer_id, customer_name, customer_address, customer_birthday, customer_email, report_period
) AS T2
INNER JOIN (
SELECT     -- здесь считаем кол-во заказов каждого заказчика по категориям товаров
	dd.customer_id AS customer_id_for_product_type, 
	dd.product_type, 
	COUNT(dd.product_id) AS product_type_count
	FROM dwh_delta AS dd
	GROUP BY dd.customer_id, dd.product_type
) AS T3 ON T2.customer_id = T3.customer_id_for_product_type
INNER JOIN (
SELECT     -- здесь считаем кол-во заказов каждого заказчика по мастерам
	dd.customer_id AS customer_id_for_craftsman, 
	dd.craftsman_id, 
	COUNT(dd.product_id) AS craftsman_count
	FROM dwh_delta AS dd
	GROUP BY dd.customer_id, dd.craftsman_id
) AS T4 ON T2.customer_id = T4.customer_id_for_craftsman
) AS T5
),
-- добавляем данные из дельты по новым заказчикам
insert_delta AS (
INSERT INTO dwh.customer_report_datamart (
	customer_id,
	customer_name,
	customer_address,
	customer_birthday,
	customer_email,
	customer_money,
	platform_money,
	count_order,
	avg_price_order,
	median_time_order_completed,
	top_product_type,
	top_craftsman_id,
	count_order_created,
	count_order_in_progress,
	count_order_delivery,
	count_order_done,
	count_order_not_done,
	report_period
)
SELECT
	customer_id,
	customer_name,
	customer_address,
	customer_birthday,
	customer_email,
	customer_money,
	platform_money,
	count_order,
	avg_price_order,
	median_time_order_completed,
	top_product_type,
	top_craftsman_id,
	count_order_created,
	count_order_in_progress,
	count_order_delivery,
	count_order_done,
	count_order_not_done,
	report_period
FROM delta_for_insert
),
-- обновляем строки по заказчикам из дельты, уже существующим в витрине
update_delta AS (
UPDATE dwh.customer_report_datamart AS crd
SET
	customer_name = dfu.customer_name,
	customer_address = dfu.customer_address,
	customer_birthday = dfu.customer_birthday,
	customer_email = dfu.customer_email,
	customer_money = dfu.customer_money,
	platform_money = dfu.platform_money,
	count_order = dfu.count_order,
	avg_price_order = dfu.avg_price_order,
	median_time_order_completed = dfu.median_time_order_completed,
	top_product_type = dfu.top_product_type,
	top_craftsman_id = dfu.top_craftsman_id,
	count_order_created = dfu.count_order_created,
	count_order_in_progress = dfu.count_order_in_progress,
	count_order_delivery = dfu.count_order_delivery,
	count_order_done = dfu.count_order_done,
	count_order_not_done = dfu.count_order_not_done,
	report_period = dfu.report_period
FROM delta_for_update AS dfu
WHERE crd.customer_id = dfu.customer_id
),
-- добавляем дату текущего инкрементального обновления витрины в табличку учета обновлений
insert_load_date AS (
	INSERT INTO dwh.load_dates_customer_report_datamart (
	load_dttm
	)
	SELECT GREATEST(
		COALESCE(MAX(customer_load_dttm), NOW()),
		COALESCE(MAX(order_load_dttm), NOW()),
		COALESCE(MAX(product_load_dttm), NOW()),
		COALESCE(MAX(craftsman_load_dttm), NOW())
	)
	FROM dwh_delta
)
SELECT 'Data Mart updated!';
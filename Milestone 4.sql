-- TASK 1
SELECT  country_code AS country,
        COUNT(country_code) AS total_no_of_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_of_stores DESC;

-- TASK 2
SELECT  locality,
        COUNT(country_code) AS total_no_of_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_of_stores DESC
LIMIT 7;

-- TASK 3
SELECT  SUM(dp.product_price * ot.product_quantity) AS total_sales,
        ddt.month 
FROM orders_table ot
JOIN dim_products dp ON ot.product_code = dp.product_code
JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
GROUP BY month
ORDER BY total_sales DESC
LIMIT 6;

-- TASK 4
SELECT store_type FROM dim_store_details GROUP BY store_type

SELECT COUNT(ot.product_quantity) AS number_of_sales,
        SUM(ot.product_quantity) AS product_quantity_count,
        CASE 
            WHEN dsd.store_type = 'Web Portal' THEN 'Web'
            ELSE 'Offline' -- this should really be more specific
        END AS location
FROM orders_table ot
JOIN dim_store_details dsd ON ot.store_code = dsd.store_code
GROUP BY location;

-- Task 5
SELECT dsd.store_type AS store_type,
        SUM(dp.product_price * ot.product_quantity) AS total_sales,
        ROUND(
            (100 * COUNT(ot.product_quantity))::NUMERIC / (SELECT COUNT(*) FROM orders_table), 2 
        ) AS "sales_made(%)"
FROM orders_table ot
JOIN dim_products dp ON ot.product_code = dp.product_code
JOIN dim_store_details dsd ON ot.store_code = dsd.store_code
GROUP BY store_type
ORDER BY "sales_made(%)" DESC;

-- TASK 6
SELECT  SUM(dp.product_price * ot.product_quantity) AS total_sales,
        ddt.year,
        ddt.month 
FROM orders_table ot
JOIN dim_products dp ON ot.product_code = dp.product_code
JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
GROUP BY year, month
ORDER BY total_sales DESC
LIMIT 10;

-- TASK 6
SELECT * from dim_store_details

SELECT  SUM(staff_numbers) AS total_staff_numbers,
        country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC

-- TASK 7
SELECT  SUM(dp.product_price * ot.product_quantity) AS total_sales,
        dsd.store_type AS store_type,
        dsd.country_code AS country_code
FROM orders_table ot
JOIN dim_products dp ON ot.product_code = dp.product_code
JOIN dim_store_details dsd ON ot.store_code = dsd.store_code
WHERE country_code = 'DE'
GROUP BY store_type, country_code
ORDER BY total_sales ASC;

-- TASK 8
SELECT * FROM dim_date_times LIMIT 100
    -- Method 1: Max - Min in a year
SELECT  year,
        (MAX(event_datetime)-MIN(event_datetime)) / (COUNT(*)-1) AS actual_time_taken
FROM (
SELECT 
    year,
    month,
    day,
    timestamp,
    TO_TIMESTAMP(CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0'), ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS') AS event_datetime
FROM dim_date_times
ORDER BY YEAR
 ) 
GROUP BY year
ORDER BY actual_time_taken DESC
LIMIT 5

    -- Method 2: Min - Min between years
SELECT  year,
        (LEAD(min_datetime, 1, max_datetime) OVER (ORDER BY year ASC) - min_datetime) / (datetime_count::NUMERIC) as actual_time_taken
FROM (
SELECT year, MIN(event_datetime) AS min_datetime, MAX(event_datetime) AS max_datetime, COUNT(*) AS datetime_count FROM (
SELECT 
    year,
    month,
    day,
    timestamp,
    TO_TIMESTAMP(CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0'), ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS') AS event_datetime
FROM dim_date_times
 ) 
 GROUP BY year
 
)
ORDER BY actual_time_taken DESC
LIMIT 5

-- Method 3: datetime - datetime for each datetime
SELECT  year,
        AVG(time_between) as actual_time_taken
FROM (
SELECT year, LEAD(event_datetime) OVER (ORDER BY event_datetime) - event_datetime AS time_between FROM (
SELECT 
    year,
    month,
    day,
    timestamp,
    TO_TIMESTAMP(CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0'), ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS') AS event_datetime
FROM dim_date_times
 ) 
 
)
GROUP BY year
ORDER BY actual_time_taken DESC
LIMIT 5

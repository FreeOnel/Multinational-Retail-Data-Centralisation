
-- Task 1
SELECT MAX(LENGTH(card_number)) AS max_length_card FROM orders_table;
SELECT MAX(LENGTH(store_code)) AS max_length_store FROM orders_table;
SELECT MAX(LENGTH(product_code)) AS max_length_product FROM orders_table;

-- Alter the data types
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN card_number TYPE VARCHAR(19), 
    ALTER COLUMN store_code TYPE VARCHAR(12),  
    ALTER COLUMN product_code TYPE VARCHAR(11), 
    ALTER COLUMN product_quantity TYPE SMALLINT;

-- Task 2
SELECT MAX(LENGTH(country_code)) AS max_length_product FROM dim_users;

ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
    ALTER COLUMN country_code TYPE VARCHAR(3), 
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN join_date TYPE DATE USING join_date::DATE;


-- Task 3
SELECT * FROM dim_store_details

UPDATE dim_store_details
SET latitude = COALESCE(latitude, lat);

-- Drop the `lat` column after merging
ALTER TABLE dim_store_details DROP COLUMN lat;

ALTER TABLE dim_store_details
    ALTER COLUMN latitude TYPE NUMERIC USING latitude::NUMERIC,
    ALTER COLUMN longitude TYPE NUMERIC USING longitude::NUMERIC;

SELECT MAX(LENGTH(store_code)) AS max_length_store_code,
       MAX(LENGTH(country_code)) AS max_length_country_code
FROM dim_store_details;

ALTER TABLE dim_store_details
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(12), 
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN country_code TYPE VARCHAR(2), 
    ALTER COLUMN continent TYPE VARCHAR(255);


-- Task 4
SELECT * FROM dim_products

UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE NUMERIC USING product_price::NUMERIC;

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(20);

UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
    ELSE NULL
END;

ALTER TABLE dim_products
ALTER COLUMN weight TYPE NUMERIC USING weight::NUMERIC;

-- Task 5
SELECT * FROM dim_products

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

UPDATE dim_products
SET still_available = CASE
    WHEN still_available = 'Still_avaliable' THEN TRUE
    WHEN still_available = 'Removed' THEN FALSE
    ELSE NULL
END;

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN;

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE NUMERIC USING product_price::NUMERIC;

ALTER TABLE dim_products
ALTER COLUMN weight TYPE NUMERIC USING weight::NUMERIC;

SELECT MAX(LENGTH("EAN")) AS max_ean_length, MAX(LENGTH(product_code)) AS max_product_code_length
FROM dim_products;

ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(17);

ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE USING date_added::DATE;

ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING uuid::UUID;

ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(20);

-- Task 6

SELECT  MAX(LENGTH(month)) AS max_month_length,
        MAX(LENGTH(year)) AS max_year_length,
        MAX(LENGTH(day)) AS max_day_length,
        MAX(LENGTH(time_period)) AS max_time_period_length
FROM dim_date_times;

ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(2),
    ALTER COLUMN year TYPE VARCHAR(4),
    ALTER COLUMN day TYPE VARCHAR(2),
    ALTER COLUMN time_period TYPE VARCHAR(10),
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

-- Task 7
SELECT  MAX(LENGTH(card_number)) AS max_card_number_length,
        MAX(LENGTH(expiry_date)) AS max_expiry_date_length
FROM dim_card_details;

-- Update the column data types
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN expiry_date TYPE VARCHAR(5),  -- Assuming format 'MM/YY'
    ALTER COLUMN date_payment_confirmed TYPE DATE;

-- Task 8
ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

-- Task 9
-- Add foreign key constraint for user_uuid
ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuid
FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);

-- Add foreign key constraint for card_number
ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number
FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);

-- Add foreign key constraint for store_code
ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code
FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);

-- Add foreign key constraint for product_code
ALTER TABLE orders_table
ADD CONSTRAINT fk_product_code
FOREIGN KEY (product_code) REFERENCES dim_products(product_code);

-- Add foreign key constraint for date_uuid
ALTER TABLE orders_table
ADD CONSTRAINT fk_date_uuid
FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);

SELECT * FROM orders_table
SELECT * FROM dim_store_details



SELECT DISTINCT store_code
FROM orders_table
WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);

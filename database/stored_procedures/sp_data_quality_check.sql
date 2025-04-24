-- Data Quality Check Procedure (simulated as a view)
CREATE VIEW IF NOT EXISTS sp_data_quality_check AS
WITH sales_quality AS (
    SELECT 
        'Sales Table' as table_name,
        COUNT(*) as total_records,
        SUM(CASE WHEN sale_id IS NULL THEN 1 ELSE 0 END) as null_id_count,
        SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_product_count,
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_count,
        SUM(CASE WHEN amount <= 0 THEN 1 ELSE 0 END) as invalid_amount_count,
        SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) as invalid_quantity_count,
        SUM(CASE WHEN sale_date > date('now') THEN 1 ELSE 0 END) as future_date_count
    FROM sales
),
product_quality AS (
    SELECT 
        'Product Table' as table_name,
        COUNT(*) as total_records,
        SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_id_count,
        SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) as null_name_count,
        SUM(CASE WHEN price <= 0 THEN 1 ELSE 0 END) as invalid_price_count,
        COUNT(DISTINCT product_id) as unique_ids,
        COUNT(*) - COUNT(DISTINCT product_id) as duplicate_id_count
    FROM products
),
customer_quality AS (
    SELECT 
        'Customer Table' as table_name,
        COUNT(*) as total_records,
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_id_count,
        SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) as null_name_count,
        SUM(CASE WHEN email NOT LIKE '%@%' THEN 1 ELSE 0 END) as invalid_email_count,
        COUNT(DISTINCT customer_id) as unique_ids,
        COUNT(*) - COUNT(DISTINCT customer_id) as duplicate_id_count
    FROM customers
)
SELECT 
    table_name,
    total_records,
    CASE 
        WHEN table_name = 'Sales Table' THEN null_id_count + null_product_count + null_customer_count + invalid_amount_count + invalid_quantity_count + future_date_count
        WHEN table_name = 'Product Table' THEN null_id_count + null_name_count + invalid_price_count + duplicate_id_count
        WHEN table_name = 'Customer Table' THEN null_id_count + null_name_count + invalid_email_count + duplicate_id_count
    END as total_issues,
    CASE 
        WHEN table_name = 'Sales Table' THEN 
            'Null IDs: ' || null_id_count || 
            ', Null Products: ' || null_product_count || 
            ', Null Customers: ' || null_customer_count || 
            ', Invalid Amounts: ' || invalid_amount_count || 
            ', Invalid Quantities: ' || invalid_quantity_count || 
            ', Future Dates: ' || future_date_count
        WHEN table_name = 'Product Table' THEN 
            'Null IDs: ' || null_id_count || 
            ', Null Names: ' || null_name_count || 
            ', Invalid Prices: ' || invalid_price_count || 
            ', Duplicate IDs: ' || duplicate_id_count
        WHEN table_name = 'Customer Table' THEN 
            'Null IDs: ' || null_id_count || 
            ', Null Names: ' || null_name_count || 
            ', Invalid Emails: ' || invalid_email_count || 
            ', Duplicate IDs: ' || duplicate_id_count
    END as issue_details
FROM sales_quality
UNION ALL
SELECT * FROM product_quality
UNION ALL
SELECT * FROM customer_quality;
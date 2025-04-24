-- Dynamic Report Generation

CREATE VIEW IF NOT EXISTS sp_dynamic_report AS
SELECT 
    'sales_by_region' as report_type,
    c.region as dimension,
    strftime('%Y-%m', s.sale_date) as time_period,
    COUNT(DISTINCT s.sale_id) as metric_value,
    'total_orders' as metric_name
FROM sales s
JOIN customers c ON s.customer_id = c.customer_id
GROUP BY c.region, strftime('%Y-%m', s.sale_date)

UNION ALL

SELECT 
    'sales_by_region' as report_type,
    c.region as dimension,
    strftime('%Y-%m', s.sale_date) as time_period,
    SUM(s.amount) as metric_value,
    'total_revenue' as metric_name
FROM sales s
JOIN customers c ON s.customer_id = c.customer_id
GROUP BY c.region, strftime('%Y-%m', s.sale_date)

UNION ALL

SELECT 
    'sales_by_category' as report_type,
    p.category as dimension,
    strftime('%Y-%m', s.sale_date) as time_period,
    COUNT(DISTINCT s.sale_id) as metric_value,
    'total_orders' as metric_name
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.category, strftime('%Y-%m', s.sale_date)

UNION ALL

SELECT 
    'sales_by_category' as report_type,
    p.category as dimension,
    strftime('%Y-%m', s.sale_date) as time_period,
    SUM(s.amount) as metric_value,
    'total_revenue' as metric_name
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.category, strftime('%Y-%m', s.sale_date)

ORDER BY report_type, dimension, time_period, metric_name;
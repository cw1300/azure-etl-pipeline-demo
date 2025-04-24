-- Sales Summary View
CREATE VIEW IF NOT EXISTS vw_sales_summary AS
SELECT 
    strftime('%Y-%m', s.sale_date) as month_year,
    p.category,
    c.region,
    COUNT(DISTINCT s.sale_id) as total_transactions,
    COUNT(DISTINCT s.customer_id) as unique_customers,
    SUM(s.quantity) as total_units_sold,
    SUM(s.amount) as total_revenue,
    AVG(s.amount) as avg_transaction_value,
    MIN(s.amount) as min_transaction_value,
    MAX(s.amount) as max_transaction_value,
    SUM(CASE WHEN strftime('%w', s.sale_date) IN ('0', '6') THEN s.amount ELSE 0 END) as weekend_revenue,
    SUM(CASE WHEN strftime('%w', s.sale_date) NOT IN ('0', '6') THEN s.amount ELSE 0 END) as weekday_revenue
FROM sales s
JOIN products p ON s.product_id = p.product_id
JOIN customers c ON s.customer_id = c.customer_id
GROUP BY strftime('%Y-%m', s.sale_date), p.category, c.region
ORDER BY month_year DESC, total_revenue DESC;
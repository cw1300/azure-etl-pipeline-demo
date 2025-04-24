-- Stored Procedure: Calculate Sales Metrics

CREATE VIEW IF NOT EXISTS sp_calculate_sales_metrics AS
WITH sales_metrics AS (
    SELECT 
        p.category,
        strftime('%Y-%m', s.sale_date) as month_year,
        COUNT(DISTINCT s.sale_id) as total_orders,
        SUM(s.quantity) as total_quantity,
        SUM(s.amount) as total_revenue,
        AVG(s.amount) as avg_order_value,
        COUNT(DISTINCT s.customer_id) as unique_customers
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
    GROUP BY p.category, strftime('%Y-%m', s.sale_date)
),
category_rankings AS (
    SELECT 
        category,
        SUM(total_revenue) as category_revenue,
        RANK() OVER (ORDER BY SUM(total_revenue) DESC) as revenue_rank
    FROM sales_metrics
    GROUP BY category
)
SELECT 
    sm.*,
    cr.revenue_rank as category_rank,
    sm.total_revenue / SUM(sm.total_revenue) OVER (PARTITION BY sm.month_year) * 100 as revenue_share
FROM sales_metrics sm
JOIN category_rankings cr ON sm.category = cr.category
ORDER BY sm.month_year DESC, cr.revenue_rank;
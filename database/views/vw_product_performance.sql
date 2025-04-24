-- Product Performance View
CREATE VIEW IF NOT EXISTS vw_product_performance AS
WITH product_sales AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        p.price,
        COUNT(DISTINCT s.sale_id) as total_sales,
        SUM(s.quantity) as total_quantity,
        SUM(s.amount) as total_revenue,
        AVG(s.amount) as avg_sale_amount,
        COUNT(DISTINCT s.customer_id) as unique_customers,
        MIN(s.sale_date) as first_sale_date,
        MAX(s.sale_date) as last_sale_date
    FROM products p
    LEFT JOIN sales s ON p.product_id = s.product_id
    GROUP BY p.product_id, p.product_name, p.category, p.price
),
category_metrics AS (
    SELECT 
        category,
        AVG(total_revenue) as avg_category_revenue,
        SUM(total_revenue) as total_category_revenue
    FROM product_sales
    GROUP BY category
)
SELECT 
    ps.*,
    ROUND(ps.total_revenue * 100.0 / cm.total_category_revenue, 2) as category_revenue_share,
    CASE 
        WHEN ps.total_revenue > cm.avg_category_revenue THEN 'Above Average'
        WHEN ps.total_revenue < cm.avg_category_revenue THEN 'Below Average'
        ELSE 'Average'
    END as performance_category,
    RANK() OVER (PARTITION BY ps.category ORDER BY ps.total_revenue DESC) as category_rank,
    RANK() OVER (ORDER BY ps.total_revenue DESC) as overall_rank
FROM product_sales ps
JOIN category_metrics cm ON ps.category = cm.category
ORDER BY ps.total_revenue DESC;
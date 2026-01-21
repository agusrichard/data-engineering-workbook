SELECT
    user_id,
    COUNT(id) AS total_orders
FROM {{ ref('orders') }}
GROUP BY 1 
HAVING COUNT(id) < 0
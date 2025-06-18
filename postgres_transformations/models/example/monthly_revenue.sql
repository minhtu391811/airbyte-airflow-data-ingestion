SELECT
    DATE_TRUNC('month', o.order_time) AS revenue_month,
    SUM(o.total_price) AS total_revenue,
    SUM(o.number_of_tickets) AS total_tickets_sold
FROM ticket_orders o
WHERE o.payment_status = 'PAID'
GROUP BY DATE_TRUNC('month', o.order_time)
ORDER BY revenue_month
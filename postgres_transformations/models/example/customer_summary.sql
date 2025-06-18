SELECT
    cu.customer_id,
    cu.full_name,
    cu.email,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.number_of_tickets) AS total_tickets,
    SUM(o.total_price) AS total_spent,
    COUNT(DISTINCT s.film_id) AS distinct_films_watched
FROM customers cu
JOIN ticket_orders o ON cu.customer_id = o.customer_id AND o.payment_status = 'PAID'
JOIN showtimes s ON o.showtime_id = s.showtime_id
GROUP BY cu.customer_id, cu.full_name, cu.email
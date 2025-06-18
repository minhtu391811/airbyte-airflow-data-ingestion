SELECT
    a.actor_id,
    a.actor_name,
    SUM(o.total_price) AS total_revenue
FROM actors a
JOIN film_actors fa ON a.actor_id = fa.actor_id
JOIN films f ON fa.film_id = f.film_id
JOIN showtimes s ON f.film_id = s.film_id
JOIN ticket_orders o ON s.showtime_id = o.showtime_id AND o.payment_status = 'PAID'
GROUP BY a.actor_id, a.actor_name
ORDER BY total_revenue DESC
LIMIT 10
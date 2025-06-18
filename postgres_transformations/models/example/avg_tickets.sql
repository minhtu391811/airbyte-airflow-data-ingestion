SELECT
    f.film_id,
    f.title,
    COUNT(s.showtime_id) AS total_showtimes,
    SUM(o.number_of_tickets) AS total_tickets_sold,
    ROUND(SUM(o.number_of_tickets)::NUMERIC / NULLIF(COUNT(s.showtime_id), 0), 2) AS avg_tickets_per_showtime
FROM films f
LEFT JOIN showtimes s ON f.film_id = s.film_id
LEFT JOIN ticket_orders o ON s.showtime_id = o.showtime_id AND o.payment_status = 'PAID'
GROUP BY f.film_id, f.title

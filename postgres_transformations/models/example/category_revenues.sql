SELECT
    fc.category_name,
    SUM(o.total_price) AS total_revenue,
    SUM(o.number_of_tickets) AS total_tickets_sold
FROM film_category fc
JOIN films f ON fc.film_id = f.film_id
JOIN showtimes s ON f.film_id = s.film_id
JOIN ticket_orders o ON s.showtime_id = o.showtime_id AND o.payment_status = 'PAID'
GROUP BY fc.category_name
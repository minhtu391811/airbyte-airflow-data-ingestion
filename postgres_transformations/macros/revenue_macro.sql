{% macro revenue_by_dimension(dimension_table, dimension_id, dimension_name) %}
SELECT
    {{ dimension_table }}.{{ dimension_id }},
    {{ dimension_table }}.{{ dimension_name }} AS name,
    SUM(o.total_price) AS total_revenue,
    SUM(o.number_of_tickets) AS total_tickets_sold
FROM {{ dimension_table }}
JOIN showtimes s ON {{ dimension_table }}.{{ dimension_id }} = s.{{ dimension_id }}
JOIN ticket_orders o ON s.showtime_id = o.showtime_id AND o.payment_status = 'PAID'
GROUP BY {{ dimension_table }}.{{ dimension_id }}, {{ dimension_table }}.{{ dimension_name }}
{% endmacro %}
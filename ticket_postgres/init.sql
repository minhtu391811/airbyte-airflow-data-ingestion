-- Create tables
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    full_name VARCHAR(100),
    email VARCHAR(100),
    phone_number VARCHAR(15),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE cinemas (
    cinema_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    location VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE showtimes (
    showtime_id SERIAL PRIMARY KEY,
    film_id INTEGER,
    cinema_id INTEGER REFERENCES cinemas(cinema_id),
    show_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ticket_orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    showtime_id INTEGER REFERENCES showtimes(showtime_id),
    number_of_tickets INTEGER NOT NULL,
    total_price DECIMAL(7,2) NOT NULL,
    order_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payment_status VARCHAR(20) CHECK (payment_status IN ('PAID', 'PENDING', 'CANCELLED')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a user for Airbyte connector
CREATE USER airbyte_connector PASSWORD 'Mop-391811';

-- Grant necessary permissions to the Airbyte connector user
GRANT USAGE ON SCHEMA public TO airbyte_connector;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO airbyte_connector;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO airbyte_connector;

ALTER USER airbyte_connector REPLICATION;

SELECT pg_create_logical_replication_slot('airbyte_slot', 'pgoutput');
SELECT pg_create_logical_replication_slot('airbyte_log_slot', 'pgoutput');

-- Set up replication for Airbyte
ALTER TABLE customers REPLICA IDENTITY DEFAULT;
ALTER TABLE cinemas REPLICA IDENTITY DEFAULT;
ALTER TABLE showtimes REPLICA IDENTITY DEFAULT;
ALTER TABLE ticket_orders REPLICA IDENTITY DEFAULT;

-- Create publications for Airbyte to use
CREATE PUBLICATION airbyte_publication FOR TABLE customers, cinemas, showtimes, ticket_orders;
CREATE PUBLICATION airbyte_log_publication FOR TABLE customers, cinemas, showtimes, ticket_orders;

CREATE TABLE airbyte_heartbeat (
	id SERIAL PRIMARY KEY,
	timestamp TIMESTAMP NOT NULL DEFAULT current_timestamp,
	text TEXT
);

ALTER PUBLICATION airbyte_publication ADD TABLE airbyte_heartbeat;
ALTER PUBLICATION airbyte_log_publication ADD TABLE airbyte_heartbeat;

GRANT INSERT, SELECT, UPDATE ON TABLE airbyte_heartbeat TO airbyte_connector;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE airbyte_heartbeat_id_seq TO airbyte_connector;

INSERT INTO customers (full_name, email, phone_number) VALUES
('John Doe', 'john.doe@example.com', '0901234567'),
('Jane Smith', 'jane.smith@example.com', '0902345678'),
('Alice Johnson', 'alice.johnson@example.com', '0903456789'),
('Bob Williams', 'bob.williams@example.com', '0904567890'),
('Emily Clark', 'emily.clark@example.com', '0905678901'),
('Michael Robinson', 'michael.robinson@example.com', '0906789012'),
('Sarah Lewis', 'sarah.lewis@example.com', '0907890123'),
('David Walker', 'david.walker@example.com', '0908901234'),
('Sophia Hall', 'sophia.hall@example.com', '0909012345'),
('James Allen', 'james.allen@example.com', '0901123456'),
('Olivia Young', 'olivia.young@example.com', '0902234567'),
('Chris King', 'chris.king@example.com', '0903345678'),
('Grace Wright', 'grace.wright@example.com', '0904456789'),
('William Scott', 'william.scott@example.com', '0905567890'),
('John Doe', 'john.doe+2@example.com', '0906678901'),
('Jane Smith', 'jane.smith+2@example.com', '0907789012'),
('Alice Johnson', 'alice.johnson+2@example.com', '0908890123'),
('Bob Williams', 'bob.williams+2@example.com', '0909901234'),
('Emily Clark', 'emily.clark+2@example.com', '0910012345'),
('Michael Robinson', 'michael.robinson+2@example.com', '0911123456');

INSERT INTO cinemas (name, location) VALUES
('CGV Aeon Mall', 'Tan Phu, HCMC'),
('Lotte Cinema', 'District 7, HCMC'),
('Galaxy Cinema', 'Nguyen Du, HCMC'),
('BHD Star Cineplex', 'Vincom Thao Dien, HCMC'),
('Mega GS', 'Cong Hoa, HCMC');

INSERT INTO showtimes (film_id, cinema_id, show_time) VALUES
(1, 1, '2025-06-20 18:00:00'),
(2, 2, '2025-06-20 19:00:00'),
(3, 3, '2025-06-21 20:00:00'),
(4, 4, '2025-06-21 21:00:00'),
(5, 5, '2025-06-22 17:30:00'),
(6, 1, '2025-06-22 19:45:00'),
(7, 2, '2025-06-23 18:15:00'),
(8, 3, '2025-06-23 20:00:00'),
(9, 4, '2025-06-24 16:30:00'),
(10, 5, '2025-06-24 21:00:00'),
(11, 1, '2025-06-25 18:45:00'),
(12, 2, '2025-06-25 19:30:00'),
(13, 3, '2025-06-26 20:15:00'),
(14, 4, '2025-06-26 22:00:00'),
(15, 5, '2025-06-27 18:00:00'),
(16, 1, '2025-06-27 20:30:00'),
(17, 2, '2025-06-28 19:00:00'),
(18, 3, '2025-06-28 21:00:00'),
(19, 4, '2025-06-29 18:45:00'),
(20, 5, '2025-06-29 20:15:00');

INSERT INTO ticket_orders (customer_id, showtime_id, number_of_tickets, total_price, payment_status) VALUES
(1, 1, 2, 25.98, 'PAID'),
(2, 2, 3, 38.97, 'PAID'),
(3, 3, 1, 12.99, 'PENDING'),
(4, 4, 2, 21.98, 'CANCELLED'),
(5, 5, 4, 51.96, 'PAID'),
(6, 6, 2, 27.98, 'PAID'),
(7, 7, 3, 38.97, 'PENDING'),
(8, 8, 1, 11.99, 'PAID'),
(9, 9, 2, 23.98, 'PAID'),
(10, 10, 1, 12.99, 'PAID'),
(11, 11, 3, 35.97, 'CANCELLED'),
(12, 12, 1, 14.99, 'PENDING'),
(13, 13, 2, 29.98, 'PAID'),
(14, 14, 3, 41.97, 'PAID'),
(15, 15, 2, 25.98, 'PENDING'),
(16, 16, 1, 11.99, 'PAID'),
(17, 17, 2, 23.98, 'PAID'),
(18, 18, 4, 47.96, 'PAID'),
(19, 19, 2, 27.98, 'CANCELLED'),
(20, 20, 3, 32.97, 'PAID');
import psycopg2
import random
import schedule
import time
import re
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# Kết nối đến 2 database
conn_1 = psycopg2.connect(
    dbname="management_db",
    user="minhtus",
    password="Mop-391811",
    host="localhost",
    port="5432"
)

conn_2 = psycopg2.connect(
    dbname="ticket_db",
    user="minhtus",
    password="Mop-391811",
    host="localhost",
    port="5433"
)

conn_1.autocommit = True
conn_2.autocommit = True

cursor1 = conn_1.cursor()
cursor2 = conn_2.cursor()

def insert_data():
    print(f"\n[{datetime.now()}] 🔄 Inserting new data...")

    # === USERS (conn_1) ===
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.email()
    dob = fake.date_of_birth(minimum_age=18, maximum_age=70)
    cursor1.execute("""
        INSERT INTO users (first_name, last_name, email, date_of_birth)
        VALUES (%s, %s, %s, %s) RETURNING id
    """, (first_name, last_name, email, dob))
    user_row = cursor1.fetchone()
    if user_row is None:
        print("Failed to insert user or fetch user id.")
        return
    user_id = user_row[0]

    # === FILMS (conn_1) ===
    title = fake.sentence(nb_words=3).rstrip(".")
    release_date = fake.date_between(start_date='-5y', end_date='today')
    price = round(random.uniform(5.0, 15.0), 2)
    rating = random.choice(['G', 'PG', 'PG-13', 'R', 'NC-17'])
    user_rating = round(random.uniform(1.0, 5.0), 1)
    cursor1.execute("""
        INSERT INTO films (title, release_date, price, rating, user_rating)
        VALUES (%s, %s, %s, %s, %s) RETURNING film_id
    """, (title, release_date, price, rating, user_rating))
    film_row = cursor1.fetchone()
    if film_row is None:
        print("Failed to insert film or fetch film id.")
        return
    film_id = film_row[0]

    # === FILM_CATEGORY (conn_1) ===
    category = random.choice(['Action', 'Comedy', 'Drama', 'Sci-Fi', 'Thriller', 'Horror'])
    cursor1.execute("""
        INSERT INTO film_category (film_id, category_name)
        VALUES (%s, %s)
    """, (film_id, category))

    # === ACTORS (conn_1) ===
    actor_name = fake.name()
    cursor1.execute("""
        INSERT INTO actors (actor_name)
        VALUES (%s) RETURNING actor_id
    """, (actor_name,))
    actor_row = cursor1.fetchone()
    if actor_row is None:
        print("Failed to insert actor or fetch actor id.")
        return
    actor_id = actor_row[0]

    # === FILM_ACTORS (conn_1) ===
    cursor1.execute("""
        INSERT INTO film_actors (film_id, actor_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
    """, (film_id, actor_id))

    # === CUSTOMERS (conn_2) ===
    full_name = fake.name()
    cust_email = fake.email()
    phone = re.sub(r'\D', '', fake.phone_number())[:15]
    cursor2.execute("""
        INSERT INTO customers (full_name, email, phone_number)
        VALUES (%s, %s, %s) RETURNING customer_id
    """, (full_name, cust_email, phone))
    customer_row = cursor2.fetchone()
    if customer_row is None:
        print("Failed to insert customer or fetch customer id.")
        return
    customer_id = customer_row[0]

    # === CINEMAS (conn_2) ===
    cinema_name = fake.company()
    location = fake.address()
    cursor2.execute("""
        INSERT INTO cinemas (name, location)
        VALUES (%s, %s) RETURNING cinema_id
    """, (cinema_name, location))
    cinema_row = cursor2.fetchone()
    if cinema_row is None:
        print("Failed to insert cinema or fetch cinema id.")
        return
    cinema_id = cinema_row[0]

    # === SHOWTIMES (conn_2) ===
    show_time = datetime.now() + timedelta(days=random.randint(0, 5), hours=random.randint(0, 12))
    cursor2.execute("""
        INSERT INTO showtimes (film_id, cinema_id, show_time)
        VALUES (%s, %s, %s) RETURNING showtime_id
    """, (film_id, cinema_id, show_time))
    showtime_row = cursor2.fetchone()
    if showtime_row is None:
        print("Failed to insert showtime or fetch showtime id.")
        return
    showtime_id = showtime_row[0]

    # === TICKET_ORDERS (conn_2) ===
    number_of_tickets = random.randint(1, 5)
    total_price = number_of_tickets * price
    status = random.choice(['PAID', 'PENDING', 'CANCELLED'])
    cursor2.execute("""
        INSERT INTO ticket_orders (customer_id, showtime_id, number_of_tickets, total_price, payment_status)
        VALUES (%s, %s, %s, %s, %s)
    """, (customer_id, showtime_id, number_of_tickets, total_price, status))

    print(f"[{datetime.now()}] ✅ Inserted: user={user_id}, film={film_id}, actor={actor_id}, customer={customer_id}")

schedule.every(1).minutes.do(insert_data)

print("🚀 Data generator running... Ctrl+C to stop")

try:
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    print("🛑 Stopped.")
finally:
    cursor1.close()
    cursor2.close()
    conn_1.close()
    conn_2.close()
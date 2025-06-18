import streamlit as st
import psycopg2
import pandas as pd

# ---- CONFIG ----

SOURCE_CONFIG = {
    'host': 'host.docker.internal',
    'port': 5433,
    'database': 'source_db',
    'user': 'minhtus',
    'password': 'Mop-391811'
}

DEST_CONFIG = {
    'host': 'host.docker.internal',
    'port': 5434,
    'database': 'destination_db',
    'user': 'minhtus',
    'password': 'Mop-391811'
}

TABLES = ['users', 'films', 'film_category', 'actors', 'film_actors']

# ---- COMMON DB FUNCTION ----

def query_postgres(conn_config, query):
    try:
        conn = psycopg2.connect(**conn_config)
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchone()[0]
        cur.close()
        conn.close()
        return result
    except Exception as e:
        return None

# ---- DATA QUALITY CHECK ----

def dq_check(table):
    result = {
        'Table': table,
        'Row Count Check': '✅',
        'Null Check': 'N/A',
        'Domain Check': 'N/A',
        'Duplicate Check': '✅'
    }

    # Row count check
    src_count = query_postgres(SOURCE_CONFIG, f"SELECT COUNT(*) FROM {table}")
    dst_count = query_postgres(DEST_CONFIG, f"SELECT COUNT(*) FROM {table}")
    if src_count is None or dst_count is None or src_count != dst_count:
        result['Row Count Check'] = '❌'

    # Null Check
    if table == 'users':
        nulls = query_postgres(DEST_CONFIG, f"SELECT COUNT(*) FROM {table} WHERE email IS NULL OR first_name IS NULL OR last_name IS NULL")
        result['Null Check'] = '✅' if nulls == 0 else '❌'

    if table == 'films':
        nulls = query_postgres(DEST_CONFIG, f"SELECT COUNT(*) FROM {table} WHERE title IS NULL OR user_rating IS NULL")
        result['Null Check'] = '✅' if nulls == 0 else '❌'

        # Domain Check
        invalid_rating = query_postgres(DEST_CONFIG, f"SELECT COUNT(*) FROM {table} WHERE user_rating < 1 OR user_rating > 5")
        result['Domain Check'] = '✅' if invalid_rating == 0 else '❌'

    if table == 'film_category':
        nulls = query_postgres(DEST_CONFIG, f"SELECT COUNT(*) FROM {table} WHERE category_name IS NULL")
        result['Null Check'] = '✅' if nulls == 0 else '❌'

    if table == 'actors':
        nulls = query_postgres(DEST_CONFIG, f"SELECT COUNT(*) FROM {table} WHERE actor_name IS NULL")
        result['Null Check'] = '✅' if nulls == 0 else '❌'

    # Duplicate Check
    if table != 'film_actors':
        pk_column = {
            'users': 'id',
            'films': 'film_id',
            'film_category': 'category_id',
            'actors': 'actor_id'
        }[table]

        dup = query_postgres(DEST_CONFIG, f"""
            SELECT COUNT(*) FROM (
                SELECT {pk_column}, COUNT(*) 
                FROM {table} 
                GROUP BY {pk_column} 
                HAVING COUNT(*) > 1
            ) sub
        """)
        result['Duplicate Check'] = '✅' if dup == 0 else '❌'
    else:
        dup = query_postgres(DEST_CONFIG, f"""
            SELECT COUNT(*) FROM (
                SELECT film_id, actor_id, COUNT(*) 
                FROM {table} 
                GROUP BY film_id, actor_id 
                HAVING COUNT(*) > 1
            ) sub
        """)
        result['Duplicate Check'] = '✅' if dup == 0 else '❌'

    return result

# ---- STREAMLIT UI ----

st.set_page_config(page_title="Data Quality Dashboard", page_icon="🧪", layout="wide")
st.title("🧪 Data Quality Monitoring Dashboard")

# Run checks
dq_results = []
for table in TABLES:
    dq_results.append(dq_check(table))

df = pd.DataFrame(dq_results)

# Display result
st.dataframe(df, use_container_width=True)

# Summary
st.header("📊 Summary")
for col in ['Row Count Check', 'Null Check', 'Domain Check', 'Duplicate Check']:
    pass_count = (df[col] == '✅').sum()
    fail_count = (df[col] == '❌').sum()
    st.write(f"**{col}** — ✅ Passed: {pass_count}, ❌ Failed: {fail_count}")

st.caption("🚀 Fully automated DQ monitoring based on your airflow DAG logic.")

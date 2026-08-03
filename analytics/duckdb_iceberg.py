import duckdb
import os
import time
import duckdb
import os
import time

WAREHOUSE_PATH = os.getenv("ICEBERG_WAREHOUSE_PATH", "/tmp/iceberg_warehouse")
TABLE_PATH = f"{WAREHOUSE_PATH}/uba_db/user_events_lakehouse"
def run_duckdb_analysis():
    print("🦆 Initializing DuckDB Engine for True Iceberg Lakehouse Scan...")
    
    con = duckdb.connect(database=':memory:')

    try:
        con.execute("LOAD iceberg;")
    except Exception:
        print(" Installing Iceberg extension for the first time...")
        con.execute("INSTALL iceberg; LOAD iceberg;")

    print(f" Reading Iceberg Table Metadata from: {TABLE_PATH}")

    try:
        print("\n=== [DuckDB Query 1] Event Summary via True Iceberg Scan ===")
        con.execute("SET unsafe_enable_version_guessing = true;")
        query_1 = f"""
            SELECT 
                event_type,
                COUNT(*) as event_count,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(AVG(price), 2) as avg_price
            FROM iceberg_scan('{TABLE_PATH}')
            GROUP BY event_type
            ORDER BY event_count DESC;
        """
        result_df = con.execute(query_1).df()
        print(result_df.to_string(index=False))

    except Exception as e:
        print(f" Iceberg Scan Note/Error: {e}")

if __name__ == "__main__":
    time.sleep(2)
    run_duckdb_analysis()
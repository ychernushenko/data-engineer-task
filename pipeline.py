from datetime import datetime, timezone
import json
from seed import get_connection
import clickhouse_connect
import os

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")

TARGET_TABLE_NAMES = [
    "advertiser",           # Base tables
    "campaign",
    "impressions",
    "clicks",
    "advertiser_stats",     # Aggregated target tables
    "campaign_stats",
    "daily_stats",
]

ANALYTICS_TABLES = ["advertiser_stats", "campaign_stats", "daily_stats"]


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )


def truncate_clickhouse_tables(client, tables):
    """
    Truncates both base tables and materialized view target tables in ClickHouse.
    Should be used before a full sync to avoid duplicate data and double-counting.
    """

    print("\n🧹 Truncating ClickHouse tables for full sync...")
    for table_name in tables:
        try:
            client.command(f"TRUNCATE TABLE IF EXISTS {table_name}")
            print(f"✅ Truncated {table_name}")
        except Exception as e:
            print(f"❌ Could not truncate {table_name}: {e}")


def read_sql(path, name):
    with open(os.path.join(path, name), "r") as f:
        return f.read()


def create_clickhouse_tables(conn):

    for table_name in TARGET_TABLE_NAMES:
        conn.query(read_sql("sql/init", table_name + ".sql"))


def copy_table(pg_conn, ch_client, table, mode="full", last_id=None):
    with pg_conn.cursor() as cur:

        if mode == "incremental" and last_id is not None:
            print(f"Starting from id {last_id}")
            cur.execute(
                f"SELECT * FROM {table} WHERE id > %s ORDER BY id", (last_id,))
        else:
            print("Loading full table.")
            cur.execute(f"SELECT * FROM {table} ORDER BY id")

        rows = cur.fetchall()
        if not rows:
            print(f"⚠️ No new rows for table '{table}'")
            return

        columns = [desc[0] for desc in cur.description]
        ch_client.insert(table, rows, column_names=columns)
        print(f"✅ Synced {len(rows)} rows into ClickHouse table '{table}'")

        # Return max ID synced
        return max(row[columns.index("id")] for row in rows)


def update_analytics(conn, tables):
    truncate_clickhouse_tables(conn, tables)

    for table_name in tables:
        conn.query(read_sql("sql/init", table_name + "_init.sql"))


def run_pipeline(pg_conn, ch_conn, mode="full"):
    # Truncate tables if full load
    if mode == "full":
        truncate_clickhouse_tables(ch_conn, TARGET_TABLE_NAMES)

    create_clickhouse_tables(ch_conn)

    # Track last synced IDs per table
    last_synced_path = "last_synced_ids.json"
    last_synced = {}
    if mode == "incremental":
        if os.path.exists(last_synced_path):
            try:
                with open(last_synced_path, "r") as f:
                    content = f.read()
                    last_synced = json.loads(content)
            except json.JSONDecodeError as e:
                last_synced = {}
        else:
            print(f"[WARN] {last_synced_path} not found.")

    updated_synced = {}

    for table in ['advertiser', 'campaign', 'impressions', 'clicks']:
        print(f"\n🔄 Copying table: {table}")
        last_id = int(last_synced.get(table, 0)
                      ) if mode == "incremental" else None

        try:
            max_id = copy_table(pg_conn, ch_conn, table,
                                mode=mode, last_id=last_id)
            if max_id:
                updated_synced[table] = max_id
            else:
                print(f"No updates for table: {table}")

        except Exception as e:
            print(f"❌ Error syncing table '{table}': {e}")

    update_analytics(ch_conn, ANALYTICS_TABLES)

    last_synced.update(updated_synced)
    with open("last_synced_ids.json", "w") as f:
        json.dump(last_synced, f, indent=2)
    print("\n📝 Updated last_synced_ids.json")

    print("\n🎉 Sync completed.")

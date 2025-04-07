import os
import json
import clickhouse_connect

# Constants
BASE_TABLES = ["advertiser", "campaign", "impressions", "clicks"]
ANALYTICS_TABLES = ["advertiser_stats", "campaign_stats", "daily_stats"]
TARGET_TABLE_NAMES = BASE_TABLES + ANALYTICS_TABLES

LAST_SYNC_FILE = "last_synced_ids.json"
SQL_PATH = "sql/init"


def read_sql(path, name):
    with open(os.path.join(path, name), "r") as f:
        return f.read()


class ClickHouseClient:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
        )

    def truncate_tables(self, tables):
        print("\n🧹 Truncating ClickHouse tables for full sync...")
        for table_name in tables:
            try:
                self.client.command(f"TRUNCATE TABLE IF EXISTS {table_name}")
                print(f"✅ Truncated {table_name}")
            except Exception as e:
                print(f"❌ Could not truncate {table_name}: {e}")

    def create_tables(self):
        for table_name in TARGET_TABLE_NAMES:
            sql = read_sql(SQL_PATH, table_name + ".sql")
            self.client.query(sql)

    def update_analytics(self):
        self.truncate_tables(ANALYTICS_TABLES)
        for table_name in ANALYTICS_TABLES:
            sql = read_sql(SQL_PATH, table_name + "_init.sql")
            self.client.query(sql)

    def insert(self, table, rows, column_names):
        self.client.insert(table, rows, column_names=column_names)

    def query(self, query_str):
        return self.client.query(query_str)

    def close(self):
        return self.client.close()


class Pipeline:
    def __init__(self, pg_conn, ch_client: ClickHouseClient, mode="full"):
        self.pg_conn = pg_conn
        self.ch_client = ch_client
        self.mode = mode
        self.last_synced = {}
        self.updated_synced = {}

    def load_last_synced_ids(self):
        if not os.path.exists(LAST_SYNC_FILE):
            print(f"[WARN] {LAST_SYNC_FILE} not found.")
            return
        try:
            with open(LAST_SYNC_FILE, "r") as f:
                self.last_synced = json.load(f)
        except json.JSONDecodeError:
            self.last_synced = {}

    def save_last_synced_ids(self):
        self.last_synced.update(self.updated_synced)
        with open(LAST_SYNC_FILE, "w") as f:
            json.dump(self.last_synced, f, indent=2)
        print("\n📝 Updated last_synced_ids.json")

    def copy_table(self, table, last_id=None):
        with self.pg_conn.cursor() as cur:
            if self.mode == "incremental" and last_id is not None:
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
            self.ch_client.insert(table, rows, column_names=columns)
            print(f"✅ Synced {len(rows)} rows into ClickHouse table '{table}'")

            return max(row[columns.index("id")] for row in rows)

    def run(self):
        if self.mode == "full":
            self.ch_client.truncate_tables(TARGET_TABLE_NAMES)

        self.ch_client.create_tables()

        if self.mode == "incremental":
            self.load_last_synced_ids()

        for table in BASE_TABLES:
            print(f"\n🔄 Copying table: {table}")
            last_id = int(self.last_synced.get(table, 0)
                          ) if self.mode == "incremental" else None
            try:
                max_id = self.copy_table(table, last_id)
                if max_id:
                    self.updated_synced[table] = max_id
                else:
                    print(f"No updates for table: {table}")
            except Exception as e:
                print(f"❌ Error syncing table '{table}': {e}")

        self.ch_client.update_analytics()

        if self.mode == "incremental":
            self.save_last_synced_ids()

        print("\n🎉 Sync completed.")

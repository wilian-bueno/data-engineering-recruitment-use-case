"""
Delta + PostgreSQL write utilities.

Usage in any notebook (after %run setup/config.py):
    import sys; sys.path.insert(0, "/home/jovyan/work")
    from utils.delta_utils import save_layer, create_pg_view
"""
import re
import psycopg2


def _pg_connect(pg_props: dict):
    """Opens a psycopg2 connection from JDBC-style props dict."""
    m = re.match(r"jdbc:postgresql://([^:]+):(\d+)/([^?]+)", pg_props["url"])
    host, port, db = m.group(1), int(m.group(2)), m.group(3)
    return psycopg2.connect(host=host, port=port, dbname=db,
                             user=pg_props["user"], password=pg_props["password"])


def save_layer(df, table_name: str, delta_path: str, pg_props: dict,
               pg_schema: str = "gold",
               delta_mode: str = "overwrite",
               pg_mode: str = "overwrite") -> None:
    """
    Writes a DataFrame to:
      - Delta Lake  → delta_path/table_name  (delta_mode: overwrite | append)
      - PostgreSQL  → pg_schema.table_name   (pg_mode:    overwrite | append)
    """
    schema_opt = "overwriteSchema" if delta_mode == "overwrite" else "mergeSchema"
    df.write.format("delta").mode(delta_mode).option(schema_opt, "true") \
       .save(f"{delta_path}/{table_name}")

    (df.repartition(1).write.format("jdbc")
       .option("url",      pg_props["url"])
       .option("dbtable",  f"{pg_schema}.{table_name}")
       .option("user",     pg_props["user"])
       .option("password", pg_props["password"])
       .option("driver",   pg_props["driver"])
       .option("batchsize", "1000")
       .mode(pg_mode)
       .save())

    print(f"  {table_name}: {df.count()} rows "
          f"-> Delta [{delta_mode}] + PostgreSQL {pg_schema} [{pg_mode}]")


def create_pg_view(view_name: str, sql: str, pg_props: dict,
                   schema: str = "gold") -> None:
    """Creates or replaces a PostgreSQL view using psycopg2.
    Handles existing objects of any type (table or view)."""
    conn = _pg_connect(pg_props)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"""
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = '{schema}' AND table_name = '{view_name}'
                  AND table_type = 'BASE TABLE'
            ) THEN
                EXECUTE 'DROP TABLE {schema}.{view_name} CASCADE';
            ELSIF EXISTS (
                SELECT 1 FROM information_schema.views
                WHERE table_schema = '{schema}' AND table_name = '{view_name}'
            ) THEN
                EXECUTE 'DROP VIEW {schema}.{view_name} CASCADE';
            END IF;
        END $$;
    """)
    cur.execute(f"CREATE VIEW {schema}.{view_name} AS\n{sql}")
    cur.close()
    conn.close()
    print(f"  View: {schema}.{view_name} created")

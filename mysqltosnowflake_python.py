import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData, Table, Column
from sqlalchemy import Integer, String, Boolean, DateTime, Float, Numeric, Text
from sqlalchemy.dialects.postgresql import BYTEA
import urllib.parse
import psycopg2
from io import StringIO

# ---------- MySQL Connection ----------
mysql_user = "root"
mysql_pass = urllib.parse.quote_plus("mysql@15")
mysql_host = "localhost"
mysql_port = 3306
mysql_db   = "adf"

mysql_conn_str = f"mysql+mysqlconnector://{mysql_user}:{mysql_pass}@{mysql_host}:{mysql_port}/{mysql_db}"
mysql_engine = create_engine(mysql_conn_str)
mysql_inspector = inspect(mysql_engine)

# ---------- PostgreSQL Connection ----------
pg_user = "etl"
pg_pass = urllib.parse.quote_plus("demopass")
pg_host = "localhost"
pg_port = 5432
pg_db   = "ETL"

pg_conn_str = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
pg_engine = create_engine(pg_conn_str)
pg_conn = pg_engine.raw_connection()
pg_cursor = pg_conn.cursor()

# ---------- Helper: Type Mapper ----------
def map_type(mysql_type):
    mysql_type = mysql_type.lower()
    if "tinyint(1)" in mysql_type:
        return Boolean
    if "int" in mysql_type:
        return Integer
    if "bigint" in mysql_type:
        return Integer
    if "varchar" in mysql_type or "char" in mysql_type:
        return String
    if "text" in mysql_type:
        return Text
    if "datetime" in mysql_type or "timestamp" in mysql_type:
        return DateTime
    if "float" in mysql_type:
        return Float
    if "double" in mysql_type or "decimal" in mysql_type or "numeric" in mysql_type:
        return Numeric
    if "blob" in mysql_type or "binary" in mysql_type:
        return BYTEA
    return String

# ---------- Step 1: Create Tables (without constraints) ----------
def create_tables():
    with pg_engine.connect() as conn:
        tables = mysql_inspector.get_table_names()
        for table in tables:
            print(f"🛠 Creating table: {table}")
            #conn.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE;')

            columns = []
            for col in mysql_inspector.get_columns(table):
                colname = col["name"]
                coltype = str(col["type"])
                colnullable = col["nullable"]
                mapped_type = map_type(coltype)
                columns.append(Column(colname, mapped_type, nullable=colnullable))

            pg_table = Table(table, MetaData(), *columns)
            pg_table.create(pg_engine, checkfirst=True)
            print(f" Created table: {table}")

# ---------- Step 2: Bulk Load Data ----------
def load_data():
    tables = mysql_inspector.get_table_names()
    for table in tables:
        print(f"\n🔹 Loading data for table: {table}")
        try:
            for chunk in pd.read_sql(f"SELECT * FROM {table}", mysql_engine, chunksize=20000):
                buffer = StringIO()
                chunk.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                cols = ",".join(chunk.columns)
                copy_sql = f'COPY "{table}" ({cols}) FROM STDIN WITH CSV'
                pg_cursor.copy_expert(copy_sql, buffer)
                pg_conn.commit()

            pg_cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
            print(f" {pg_cursor.fetchone()[0]} rows copied into {table}")
        except Exception as e:
            print(f" Error copying {table}: {e}")

# ---------- Step 3: Add Constraints & Indexes ----------
def add_constraints():
    tables = mysql_inspector.get_table_names()
    with pg_engine.connect() as conn:
        for table in tables:
            # Primary Keys
            pk = mysql_inspector.get_pk_constraint(table)
            if pk and pk.get("constrained_columns"):
                cols = ",".join(pk["constrained_columns"])
                try:
                    conn.execute(f'ALTER TABLE "{table}" ADD PRIMARY KEY ({cols});')
                    print(f" Added PK on {table} ({cols})")
                except Exception as e:
                    print(f" Could not add PK on {table}: {e}")

            # Foreign Keys
            fks = mysql_inspector.get_foreign_keys(table)
            for fk in fks:
                cols = ",".join(fk["constrained_columns"])
                ref_table = fk["referred_table"]
                ref_cols = ",".join(fk["referred_columns"])
                fk_name = fk.get("name", f"fk_{table}_{ref_table}")
                try:
                    conn.execute(
                        f'ALTER TABLE "{table}" ADD CONSTRAINT "{fk_name}" FOREIGN KEY ({cols}) REFERENCES "{ref_table}" ({ref_cols});'
                    )
                    print(f" Added FK {fk_name} on {table} ({cols} -> {ref_table}.{ref_cols})")
                except Exception as e:
                    print(f" Could not add FK on {table}: {e}")

            # Indexes
            indexes = mysql_inspector.get_indexes(table)
            for idx in indexes:
                idx_name = idx["name"]
                cols = ",".join(idx["column_names"])
                unique = "UNIQUE" if idx.get("unique") else ""
                try:
                    conn.execute(
                        f'CREATE {unique} INDEX "{idx_name}" ON "{table}" ({cols});'
                    )
                    print(f" Added {unique} index {idx_name} on {table} ({cols})")
                except Exception as e:
                    print(f" Could not add index {idx_name} on {table}: {e}")

# ---------- Main ----------
if __name__ == "__main__":
    create_tables()
    load_data()
    add_constraints()
    pg_cursor.close()
    pg_conn.close()
    print("\n Migration complete: Schema + Data (COPY) + PKs + FKs + Indexes")

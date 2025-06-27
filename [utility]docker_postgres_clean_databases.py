import os
import psycopg2

def load_db_config(dbname_env, default_dbname):
    return {
        "host": os.getenv("PGHOST", "localhost"),
        "port": os.getenv("PGPORT", "5432"),
        "user": os.getenv("PGUSER", "mailmule"),
        "password": os.getenv("PGPASSWORD", "159753"),
        "dbname": os.getenv(dbname_env, default_dbname),
    }

def drop_all_tables(db_config):
    # connect to the database
    conn = psycopg2.connect(**db_config)
    conn.autocommit = True
    with conn.cursor() as cur:
        # fetch all user tables in public schema
        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public';
        """)
        tables = cur.fetchall()
        if not tables:
            print(f"No tables found in database '{db_config['dbname']}'.")
        for (table_name,) in tables:
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
            print(f"Dropped table: {table_name}")
    conn.close()

if __name__ == "__main__":
    # Load configurations
    mail_config = load_db_config("PGDATABASE", "mailmule_db")
    conv_config = load_db_config("PGCONV_DB", "mailmule_conv_db")

    print(f"Cleaning database: {mail_config['dbname']}")
    drop_all_tables(mail_config)

    print(f"Cleaning database: {conv_config['dbname']}")
    drop_all_tables(conv_config)

    print("All tables dropped. Databases are now clean.")

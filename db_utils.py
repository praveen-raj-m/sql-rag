# db_utils.py
import pandas as pd
from sqlalchemy import create_engine, text, MetaData

# Create/connect SQLite database
engine = create_engine("sqlite:///rag.db", echo=False)


def run_query(query):
    with engine.connect() as conn:
        try:
            result = conn.execute(text(query))
            try:
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
            except:
                return "Executed successfully."
        except Exception as e:
            return f"Error: {e}"


def get_schema():
    meta = MetaData()
    meta.reflect(bind=engine)
    schema_desc = ""
    for table in meta.tables.values():
        schema_desc += f"Table {table.name} with columns: {[col.name for col in table.columns]}\n"
    return schema_desc


def create_table(table_name, columns: dict):
    col_str = ", ".join([f"{k} {v}" for k, v in columns.items()])
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({col_str});"
    return run_query(query)


def create_sample_table_if_not_exists():
    query = """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT,
        age INTEGER
    );
    """
    run_query(query)

    check = run_query("SELECT COUNT(*) FROM users;")
    if isinstance(check, pd.DataFrame) and check.iloc[0, 0] == 0:
        insert_query = """
        INSERT INTO users (name, age) VALUES 
        ('Alice', 28),
        ('Bob', 34),
        ('Charlie', 22);
        """
        run_query(insert_query)


def list_tables():
    meta = MetaData()
    meta.reflect(bind=engine)
    return list(meta.tables.keys())


def get_table_columns(table_name):
    meta = MetaData()
    meta.reflect(bind=engine)
    if table_name in meta.tables:
        table = meta.tables[table_name]
        return ", ".join([col.name for col in table.columns])
    return "No such table."


def insert_row(table_name, row_string):
    try:
        meta = MetaData()
        meta.reflect(bind=engine)
        table = meta.tables[table_name]
        col_names = [col.name for col in table.columns]
        placeholders = ", ".join(["?"] * len(col_names))
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        values = eval(f"[{row_string}]")
        with engine.connect() as conn:
            conn.execute(text(query), values)
        return "Row inserted successfully."
    except Exception as e:
        return f"Error: {e}"


def bulk_insert_csv(file_obj, table_name):
    try:
        df = pd.read_csv(file_obj.name)
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        return f"{len(df)} rows inserted into '{table_name}'"
    except Exception as e:
        return f"CSV Insert Error: {e}"


def create_table_from_csv(file_obj, table_name):
    try:
        df = pd.read_csv(file_obj.name)
        df.to_sql(table_name, con=engine, if_exists='fail', index=False)
        return f"New table '{table_name}' created with {len(df)} rows."
    except Exception as e:
        return f"Error creating table from CSV: {e}"


def delete_row_by_id(table_name, row_id):
    try:
        query = f"DELETE FROM {table_name} WHERE id = {row_id};"
        return run_query(query)
    except Exception as e:
        return f"Error: {e}"


def create_foreign_key_relation(from_table, from_col, to_table, to_col):
    try:
        fk_query = f"""
        ALTER TABLE {from_table}
        ADD CONSTRAINT fk_{from_table}_{from_col}
        FOREIGN KEY ({from_col}) REFERENCES {to_table}({to_col});
        """
        return run_query(fk_query)
    except Exception as e:
        return f"Error: {e}"

import os
import json
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, text
from sqlalchemy.schema import CreateTable
from typing import Dict, List

engine = create_engine("sqlite:///rag.db", echo=False)


def list_tables():
    meta = MetaData()
    meta.reflect(bind=engine)
    return list(meta.tables.keys())


def get_schema():
    meta = MetaData()
    meta.reflect(bind=engine)
    output = []
    for table in meta.tables.values():
        output.append(f"Table: {table.name}")
        for col in table.columns:
            output.append(f"  - {col.name} ({col.type})")
        output.append("")
    return "\n".join(output)


def get_table_columns(table_name):
    meta = MetaData()
    meta.reflect(bind=engine)
    if table_name in meta.tables:
        return [col.name for col in meta.tables[table_name].columns]
    return []


def run_query(query):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            if result.returns_rows:
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df
            return "✅ Query executed successfully."
    except Exception as e:
        return f"❌ Error: {str(e)}"


def create_sample_table_if_not_exists():
    if "users" not in list_tables():
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    age INTEGER
                )
            """))
            conn.execute(text("""
                INSERT INTO users (name, age) VALUES
                ('Alice', 28),
                ('Bob', 34),
                ('Charlie', 22)
            """))
        generate_metadata_for_table("users")


def create_table(table_name, columns):
    meta = MetaData()
    cols = [Column("id", Integer, primary_key=True)]
    for name, dtype in columns.items():
        if name != "id":
            cols.append(Column(name, text(dtype)))
    table = Table(table_name, meta, *cols)
    meta.create_all(engine)
    return f"✅ Table '{table_name}' created."


def insert_row(table_name, row_values):
    try:
        values = [x.strip() for x in row_values.split(",")]
        columns = get_table_columns(table_name)
        if "id" in columns:
            columns.remove("id")
        if len(values) != len(columns):
            return "❌ Column count mismatch."
        placeholders = ", ".join(["?"] * len(values))
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        with sqlite3.connect("rag.db") as conn:
            conn.execute(sql, values)
            conn.commit()
        return "✅ Row inserted."
    except Exception as e:
        return f"❌ Error: {str(e)}"


def bulk_insert_csv(file, table_name):
    try:
        df = pd.read_csv(file.name)
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        return f"✅ Inserted {len(df)} rows into '{table_name}'"
    except Exception as e:
        return f"❌ CSV Upload Error: {str(e)}"


def create_table_from_csv(file, table_name):
    try:
        df = pd.read_csv(file.name)
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        generate_metadata_for_table(table_name)
        return f"✅ New table '{table_name}' created from CSV."
    except Exception as e:
        return f"❌ CSV Table Creation Error: {str(e)}"


def create_foreign_key_relation(from_table, from_col, to_table, to_col):
    return f"🔒 Foreign key constraints not supported in SQLite dynamically."


# ✅ METADATA MANAGEMENT

def generate_metadata_for_table(table_name):
    os.makedirs("metadata", exist_ok=True)
    meta = MetaData()
    meta.reflect(bind=engine)
    table = meta.tables.get(table_name)
    if table is None:
        return
    metadata = {
        "table": table_name,
        "description": f"Auto-generated metadata for table '{table_name}'",
        "columns": {col.name: f"{col.name} ({col.type})" for col in table.columns},
        "primary_key": [col.name for col in table.primary_key][0] if table.primary_key else None,
        "foreign_keys": []
    }
    with open(f"metadata/{table_name}.json", "w") as f:
        json.dump(metadata, f, indent=2)


def remove_metadata_for_table(table_name):
    path = f"metadata/{table_name}.json"
    if os.path.exists(path):
        os.remove(path)


def sync_metadata_with_existing_tables():
    os.makedirs("metadata", exist_ok=True)
    for table in list_tables():
        path = f"metadata/{table}.json"
        if not os.path.exists(path):
            generate_metadata_for_table(table)


def generate_schema_from_db(db_path: str = "rag.db") -> Dict:
    """Generate schema information from the database"""
    schema = {}
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            # Get table info
            cursor.execute(f"PRAGMA table_info({table});")
            columns = {}
            for col in cursor.fetchall():
                col_name = col[1]
                col_type = col[2]
                col_pk = col[5]
                
                # Build column description with full details
                desc = {
                    "type": col_type,
                    "primary_key": bool(col_pk),
                    "not_null": bool(col[3]),
                    "default": col[4],
                    "example_query": f'"{col_name}"' if ' ' in col_name or ',' in col_name else col_name
                }
                
                columns[col_name] = desc
                
            schema[table] = columns
            
    except Exception as e:
        print(f"Error generating schema: {str(e)}")
    finally:
        conn.close()
        
    return schema


def update_schema_file(schema: Dict, schema_file: str = "metadata/schema.json") -> None:
    """Update the schema file with new schema information"""
    os.makedirs(os.path.dirname(schema_file), exist_ok=True)
    with open(schema_file, 'w') as f:
        json.dump(schema, f, indent=2)


def refresh_schema(db_path: str = "rag.db", schema_file: str = "metadata/schema.json") -> str:
    """Refresh the schema file with current database structure"""
    try:
        schema = generate_schema_from_db(db_path)
        update_schema_file(schema, schema_file)
        return "✅ Schema refreshed successfully"
    except Exception as e:
        return f"❌ Error refreshing schema: {str(e)}"

from sqlalchemy import create_engine, text, MetaData
import pandas as pd

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


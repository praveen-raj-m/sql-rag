# app.py
import gradio as gr
from sqlalchemy import MetaData, text, create_engine
from db_utils import (
    run_query, get_schema, create_table, create_sample_table_if_not_exists,
    insert_row, bulk_insert_csv, list_tables, get_table_columns,
    create_foreign_key_relation, create_table_from_csv, generate_metadata_for_table,
    remove_metadata_for_table, refresh_schema, sync_metadata_with_existing_tables,
    get_db_connection, DB_PATH
)
from llm_utils import LLMHandler
import pandas as pd
import os
import streamlit as st
import sqlite3
from mcp_utils import MCPValidator
from typing import Tuple, Optional

# Initialize components
llm = LLMHandler()
mcp = MCPValidator()

# Initialize DB and LLM
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
create_sample_table_if_not_exists()
sync_metadata_with_existing_tables()

# Shared dropdowns across all tabs
shared_tables = list_tables()
table_dropdown = gr.Dropdown(label="Select Table", choices=shared_tables)
existing_table = gr.Dropdown(label="Choose Existing Table", choices=shared_tables)
from_table = gr.Dropdown(label="From Table", choices=shared_tables)
to_table = gr.Dropdown(label="To Table", choices=shared_tables)
from_col = gr.Dropdown(label="Column in From Table", choices=[])
to_col = gr.Dropdown(label="Primary Key Column in To Table", choices=[])

# Helpers

def get_pretty_schema():
    meta = MetaData()
    meta.reflect(bind=engine)
    output = []
    for table in meta.tables.values():
        row_count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table.name}", engine)['count'][0]
        output.append(f"Table: {table.name} (Rows: {row_count})")
        for col in table.columns:
            output.append(f"  - {col.name} ({col.type})")
        output.append("")
    return "\n".join(output)

def get_column_names(table_name):
    meta = MetaData()
    meta.reflect(bind=engine)
    if table_name in meta.tables:
        return [col.name for col in meta.tables[table_name].columns]
    return []

def update_dropdown_choices(table):
    cols = get_column_names(table)
    tables = list_tables()
    return gr.update(choices=cols, value=None), gr.update(choices=tables, value=table)

def preview_table_rows(table_name):
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5", engine)
        return df.to_markdown(index=False)
    except:
        return "Error fetching rows."

def init_db():
    """Initialize the database connection"""
    conn = sqlite3.connect('rag.db')
    return conn

def get_table_schema(conn, table_name):
    """Get schema information for a table"""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    schema = {}
    for col in columns:
        schema[col[1]] = col[2]  # column name and type
    return schema

def refresh_schema():
    """Refresh the schema information"""
    conn = init_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            schema = get_table_schema(conn, table_name)
            mcp.update_schema(table_name, schema)
            
        st.success("Schema refreshed successfully!")
    except Exception as e:
        st.error(f"Error refreshing schema: {str(e)}")
    finally:
        conn.close()

def delete_table_wrapper(table_name):
    try:
        run_query(f"DROP TABLE IF EXISTS {table_name}")
        remove_metadata_for_table(table_name)
        refresh_schema()
        tables = list_tables()
        return (
            f"✅ Deleted {table_name}",
            get_pretty_schema(),
            gr.update(choices=tables, value=None),
            gr.update(choices=tables, value=None),
            gr.update(choices=tables, value=None),
            gr.update(choices=tables, value=None),
            gr.update(choices=tables, value=None),
            gr.update(choices=[], value=None),
            gr.update(choices=[], value=None)
        )
    except Exception as e:
        return f"❌ {str(e)}", get_pretty_schema(), *[gr.update(choices=list_tables())]*7

def is_direct_response(response: str) -> bool:
    """Check if the response is a direct answer rather than a SQL query."""
    return response.startswith(("Tables in the database:", "Columns in"))

def handle_nl_query(question: str) -> Tuple[str, str, str]:
    """Handle natural language query and return SQL, result, and error."""
    sql = llm.nl_to_sql(question)
    
    # If it's a direct response (like table listing), return it directly
    if is_direct_response(sql):
        return sql, sql, ""
    
    # Check if we're counting columns
    is_column_count = "pragma_table_info" in sql.lower()
    
    # Otherwise execute as SQL query
    try:
        conn = get_db_connection()
        result = pd.read_sql_query(sql, conn)
        conn.close()
        
        # Format the result 
        formatted_result = format_sql_result(sql, result)
        return sql, formatted_result, ""
    except Exception as e:
        return sql, "", str(e)

def format_sql_result(sql: str, result: pd.DataFrame) -> str:
    """Format the SQL result based on query type."""
    # For COUNT(*) queries, extract and format the count
    if "COUNT(*)" in sql:
        if not result.empty:
            count = result.iloc[0, 0]
            # For null value counts
            if "IS NULL" in sql:
                col_name = sql.split("WHERE")[1].split("IS NULL")[0].strip().replace('"', '')
                table_name = sql.split("FROM")[1].split("WHERE")[0].strip()
                return f"There are {count:,} null values in the {col_name} column of the {table_name} table."
            # For column counts
            elif "FROM pragma_table_info" in sql:
                table_name = sql.split("'")[1] if "'" in sql else "the table"
                return f"The {table_name} table has {count} columns."
            # For regular row counts
            else:
                table_name = sql.split("FROM")[1].strip().rstrip(';') if "FROM" in sql else "the table"
                return f"There are {count:,} rows in {table_name}."
                
    # For MAX/MIN queries
    if "MAX(" in sql or "MIN(" in sql:
        if not result.empty:
            value = result.iloc[0, 0]
            operation = "maximum" if "MAX(" in sql else "minimum"
            col_name = sql.split("(")[1].split(")")[0].replace('"', '')
            return f"The {operation} value of {col_name} is {value}."
            
    # For AVG queries
    if "AVG(" in sql:
        if not result.empty:
            value = result.iloc[0, 0]
            col_name = sql.split("(")[1].split(")")[0].replace('"', '')
            return f"The average value of {col_name} is {value}."
    
    # Default formatting for other queries
    if len(result) == 0:
        return "No results found."
    elif len(result) == 1:
        return "1 row found:\n" + result.to_string()
    else:
        return f"{len(result)} rows found:\n" + result.to_string()

def handle_sql_query(sql: str) -> Tuple[str, str]:
    """Handle direct SQL query execution."""
    try:
        conn = get_db_connection()
        result = pd.read_sql_query(sql, conn)
        conn.close()
        formatted_result = format_sql_result(sql, result)
        return formatted_result, ""
    except Exception as e:
        return "", str(e)

with gr.Blocks() as demo:
    gr.Markdown("# SQL RAG Dashboard")
    
    with gr.Row():
        with gr.Column():
            question = gr.Textbox(label="Ask your question", placeholder="What is the highest AC current in the kettlepump table?")
            btn_ask = gr.Button("Ask")
            
        with gr.Column():
            sql_out = gr.Textbox(label="Generated SQL", interactive=True)
            btn_run_sql = gr.Button("Run SQL")
            
    with gr.Row():
        result = gr.Textbox(label="Result")
        error = gr.Textbox(label="Error")
    
    btn_ask.click(
        handle_nl_query,
        inputs=[question],
        outputs=[sql_out, result, error]
    )
    
    btn_run_sql.click(
        handle_sql_query,
        inputs=[sql_out],
        outputs=[result, error]
    )

    # Create tab
    with gr.Tab("Upload CSV"):
        csv_file = gr.File(label="Upload CSV File (.csv only)", file_types=[".csv"])
        csv_table_name = gr.Textbox(label="New Table Name")
        csv_output = gr.Textbox(label="Upload Status")

        def handle_csv_upload(file, table_name):
            result = create_table_from_csv(file, table_name)
            if result.startswith("✅"):
                refresh_schema()
            return result

        upload_btn = gr.Button("Upload CSV")
        upload_btn.click(handle_csv_upload, inputs=[csv_file, csv_table_name], outputs=[csv_output])

    # Create tab
    with gr.Tab("Create Table"):
        table_name = gr.Textbox(label="Table Name")
        col_name = gr.Textbox(label="Column Name")
        col_type = gr.Dropdown(label="Type", choices=["TEXT", "INTEGER", "REAL", "BOOLEAN"], value="TEXT")
        add_col = gr.Button("Add Column")
        col_preview = gr.Textbox(label="Preview")
        col_state = gr.State([])

        def add_column(n, t, state):
            state.append((n, t))
            return ", ".join([f"{x[0]} {x[1]}" for x in state]), state

        add_col.click(add_column, inputs=[col_name, col_type, col_state], outputs=[col_preview, col_state])

        btn_create = gr.Button("Create Table")
        status = gr.Textbox(label="Status")
        schema = gr.Textbox(label="Schema")

        def create_final_table(tname, columns):
            col_dict = {n: t for n, t in columns}
            result = create_table(tname, col_dict)
            if result.startswith("✅"):
                refresh_schema()
            return f"✅ {tname} created.", get_schema()

        btn_create.click(create_final_table, inputs=[table_name, col_state], outputs=[status, schema])

    # Delete tab
    with gr.Tab("View/Delete Tables"):
        schema_view_btn = gr.Button("Refresh Details")
        schema_output = gr.Textbox(label="Schema", lines=30)
        refresh_schema_btn = gr.Button("Refresh Schema")
        schema_status = gr.Textbox(label="Schema Refresh Status")

        delete_table_dropdown = gr.Dropdown(label="Select Table to Delete", choices=list_tables())
        delete_btn = gr.Button("Delete Selected Table")
        delete_status = gr.Textbox(label="Delete Status")
        preview_output = gr.Textbox(label="Preview", lines=8)

        delete_table_dropdown.change(preview_table_rows, inputs=delete_table_dropdown, outputs=preview_output)
        schema_view_btn.click(get_pretty_schema, outputs=[schema_output])
        refresh_schema_btn.click(refresh_schema, outputs=[schema_status])
        delete_btn.click(
            delete_table_wrapper,
            inputs=[delete_table_dropdown],
            outputs=[delete_status, schema_output, delete_table_dropdown, table_dropdown, existing_table, from_table, to_table, from_col, to_col]
        )

    demo.launch(share=True)

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
import json

# Initialize components
llm = LLMHandler()
mcp = MCPValidator()

# Initialize DB and LLM
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
create_sample_table_if_not_exists()
sync_metadata_with_existing_tables()

# Shared dropdowns across all tabs - Modified to fetch values on demand
def get_all_tables():
    return list_tables()

# Call the function to get tables instead of passing the function itself
table_dropdown = gr.Dropdown(label="Select Table", choices=get_all_tables())
existing_table = gr.Dropdown(label="Choose Existing Table", choices=get_all_tables())
from_table = gr.Dropdown(label="From Table", choices=get_all_tables())
to_table = gr.Dropdown(label="To Table", choices=get_all_tables())
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
    conn = sqlite3.connect(DB_PATH)
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
    try:
        conn = init_db()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            for table in tables:
                table_name = table[0]
                try:
                    schema = get_table_schema(conn, table_name)
                    mcp.update_schema(table_name, schema)
                except Exception as e:
                    print(f"Error updating schema for table {table_name}: {str(e)}")
            
            print("Schema refreshed successfully!")
            return " Schema refreshed successfully!"
        except Exception as e:
            error_msg = f"Error refreshing schema: {str(e)}"
            print(error_msg)
            return f" {error_msg}"
        finally:
            conn.close()
    except Exception as e:
        error_msg = f"Database connection error: {str(e)}"
        print(error_msg)
        return f" {error_msg}"

def delete_table(table_name):
    """
    Delete a table and return a status message.
    This function only deletes the table without updating any UI components.
    """
    try:
        print(f"Starting deletion of table: {table_name}")
        
        # First, verify the table exists
        tables = list_tables()
        if table_name not in tables:
            return f" Table '{table_name}' does not exist"
        
        # Try to drop the table
        print(f"Dropping table {table_name} from database")
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        conn.close()
        
        # Try to remove metadata
        print(f"Removing metadata for table {table_name}")
        metadata_path = f"metadata/{table_name}.json"
        alt_metadata_path = f"metadata/{table_name}_metadata.json"
        
        # Try both possible metadata file formats
        for path in [metadata_path, alt_metadata_path]:
            if os.path.exists(path):
                try:
                    os.remove(path)
                    print(f"Successfully removed metadata file: {path}")
                except Exception as e:
                    print(f"Error removing metadata file {path}: {e}")
        
        # Update schema.json
        schema_path = "schema.json"
        if os.path.exists(schema_path):
            try:
                with open(schema_path, 'r') as f:
                    schema = json.load(f)
                
                if table_name in schema:
                    del schema[table_name]
                    
                    with open(schema_path, 'w') as f:
                        json.dump(schema, f, indent=2)
                    print(f"Updated schema.json to remove '{table_name}'")
            except Exception as e:
                print(f"Error updating schema.json: {e}")
        
        # Return success message
        return f" Table '{table_name}' successfully deleted"
    
    except Exception as e:
        print(f"Error in delete_table: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"Error deleting table: {str(e)}"

def get_updated_dropdown():
    """Get updated dropdown choices based on current tables"""
    tables = list_tables()
    return gr.update(choices=tables, value=tables[0] if tables else None)

def get_updated_schema():
    """Get updated schema text"""
    return get_pretty_schema()

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
    # Get column names from the result DataFrame
    column_names = list(result.columns) if not result.empty else []
    
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
    
    # For queries showing rows with NULL values
    if "IS NULL" in sql and "COUNT(*)" not in sql:
        col_name = sql.split("WHERE")[1].split("IS NULL")[0].strip().replace('"', '')
        table_name = sql.split("FROM")[1].split("WHERE")[0].strip()
        
        if len(result) == 0:
            return f"No rows found with NULL values in the {col_name} column of the {table_name} table."
        else:
            return f"Found {len(result)} rows with NULL values in the {col_name} column of the {table_name} table:\n{result.to_string()}"
    
    # For COUNT(DISTINCT) queries
    if "COUNT(DISTINCT" in sql:
        if not result.empty:
            count = result.iloc[0, 0]
            col_name = sql.split("(DISTINCT")[1].split(")")[0].strip().replace('"', '')
            table_name = sql.split("FROM")[1].strip().rstrip(';')
            return f"There are {count:,} unique values in the {col_name} column of the {table_name} table."
                
    # For MAX/MIN queries
    if "MAX(" in sql or "MIN(" in sql:
        if not result.empty:
            value = result.iloc[0, 0]
            operation = "maximum" if "MAX(" in sql else "minimum"
            col_name = sql.split("(")[1].split(")")[0].replace('"', '')
            table_name = sql.split("FROM")[1].strip().rstrip(';')
            return f"The {operation} value of {col_name} in the {table_name} table is {value}."
            
    # For AVG queries
    if "AVG(" in sql:
        if not result.empty:
            value = result.iloc[0, 0]
            col_name = sql.split("(")[1].split(")")[0].replace('"', '')
            table_name = sql.split("FROM")[1].strip().rstrip(';')
            return f"The average value of {col_name} in the {table_name} table is {value}."
            
    # For SUM queries
    if "SUM(" in sql:
        if not result.empty:
            value = result.iloc[0, 0]
            col_name = sql.split("(")[1].split(")")[0].replace('"', '')
            table_name = sql.split("FROM")[1].strip().rstrip(';')
            return f"The sum of {col_name} in the {table_name} table is {value}."
            
    # For LENGTH queries (character count)
    if "LENGTH(" in sql and "REPLACE" not in sql and "ORDER BY" not in sql:
        if not result.empty:
            value = result.iloc[0, 0]
            return f"The character count is {value}."
            
    # For word count queries
    if "LENGTH(" in sql and "REPLACE" in sql:
        if not result.empty:
            value = result.iloc[0, 0]
            return f"The word count is {value}."
            
    # For text length ordering (longest/shortest)
    if "text_length" in sql and ("ORDER BY text_length DESC" in sql or "ORDER BY text_length ASC" in sql):
        if not result.empty:
            # Get the column name and value
            direction = "longest" if "DESC" in sql else "shortest"
            
            # Find the text column - it should be any column that's not 'text_length' or numeric
            text_column = None
            text_value = None
            length_value = None
            
            for col in column_names:
                if col != 'text_length' and isinstance(result.iloc[0][col], str):
                    text_column = col
                    text_value = result.iloc[0][col]
                    break
            
            if 'text_length' in column_names:
                length_value = result.iloc[0]['text_length']
                
            if text_column and text_value and length_value:
                return f"The {direction} text in the {text_column} column is '{text_value}' with a length of {length_value} characters."
            
            # Generic fallback if we couldn't identify the text column
            return f"Found a record with text length {result.iloc[0]['text_length'] if 'text_length' in column_names else 'unknown'}."
    
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

with gr.Blocks(title="SQL RAG") as demo:
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
            refresh_schema()
            return f"{tname} created.", get_schema()

        btn_create.click(create_final_table, inputs=[table_name, col_state], outputs=[status, schema])

    # Delete tab
    with gr.Tab("View/Delete Tables"):
        schema_view_btn = gr.Button("Refresh Schema View")
        schema_output = gr.Textbox(label="Schema", lines=30)
        
        gr.Markdown("### Delete Tables")
        gr.Markdown("1. Select a table to delete")
        gr.Markdown("2. Click Delete")
        gr.Markdown("3. After deletion, you can refresh the schema and dropdown")
        
        # Simplified deletion interface
        delete_table_dropdown = gr.Dropdown(label="Select Table to Delete", choices=get_all_tables())
        
        with gr.Row():
            delete_btn = gr.Button("Delete Selected Table", variant="stop")
            refresh_dropdown_btn = gr.Button("Refresh Table List")
            
        delete_status = gr.Textbox(label="Status")
        
        # Schema operations
        schema_view_btn.click(get_pretty_schema, outputs=[schema_output])
        
        # Simple deletion without trying to update other components
        delete_btn.click(
            delete_table,
            inputs=[delete_table_dropdown],
            outputs=[delete_status]
        )
        
        # Add a refresh schema button after deletion
        refresh_after_delete_btn = gr.Button("Refresh Schema After Deletion")
        refresh_after_delete_btn.click(get_pretty_schema, outputs=[schema_output])
        
        # Refresh only the delete dropdown
        refresh_dropdown_btn.click(
            lambda: gr.update(choices=get_all_tables()),
            inputs=None,
            outputs=[delete_table_dropdown]
        )
        
        # Add warning message
        gr.Markdown("**Important:** After deleting a table, you need to:")
        gr.Markdown("1. Click 'Refresh Table List' to update this dropdown")
        gr.Markdown("2. Click 'Refresh Schema After Deletion' to update the schema")
        gr.Markdown("3. To update other dropdowns in other tabs, refresh the entire page")

    demo.launch(share=True)

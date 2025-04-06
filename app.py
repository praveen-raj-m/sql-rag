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
from llm_utils import LLMHandler, extract_table_name
import pandas as pd
import os
import streamlit as st
import sqlite3
from mcp_utils import MCPValidator
from typing import Tuple, Optional, List, Dict, Any

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

def get_tables() -> List[str]:
    """Get list of tables from the database."""
    try:
        conn = sqlite3.connect("sqlite.db")
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    except Exception as e:
        print(f"Error getting tables: {e}")
        return []

def execute_sql(sql: str) -> Tuple[pd.DataFrame, str]:
    """Execute SQL query and return the results as a DataFrame."""
    try:
        conn = sqlite3.connect("sqlite.db")
        
        # Handle PRAGMA queries separately
        if "PRAGMA" in sql:
            cursor = conn.cursor()
            result = cursor.execute(sql).fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=columns)
            conn.close()
            return df, None
            
        df = pd.read_sql_query(sql, conn)
        conn.close()
        return df, None
    except Exception as e:
        return pd.DataFrame(), f"Execution failed on sql '{sql}': {str(e)}"

def create_interface():
    """Create and launch the Gradio interface."""
    # Customize CSS for a cleaner look
    css = """
    .gradio-container {
        max-width: 1200px;
        margin: auto;
    }
    h1 {
        text-align: center;
        margin-bottom: 1rem;
        font-size: 2.5rem;
        color: #2C3E50;
    }
    h3 {
        margin-top: 1rem;
    }
    .description {
        text-align: center;
        margin-bottom: 2rem;
        font-size: 1.2rem;
        color: #34495E;
    }
    .example-title {
        font-weight: bold;
        margin-top: 1.5rem;
        color: #2980B9;
    }
    .examples {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
    }
    .example-box {
        background-color: #F7F9FA;
        padding: 0.5rem;
        border-radius: 0.3rem;
        border: 1px solid #E0E5E9;
        cursor: pointer;
        transition: all 0.2s;
    }
    .example-box:hover {
        background-color: #E0E5E9;
    }
    .footer {
        text-align: center;
        margin-top: 2rem;
        font-size: 0.9rem;
        color: #7F8C8D;
    }
    """
    
    with gr.Blocks(css=css) as demo:
        gr.HTML(
            """
            <h1>SQL RAG Dashboard</h1>
            <div class="description">
                Ask questions about your database in plain English.<br>
                No SQL knowledge required - natural language to SQL conversion happens automatically.
            </div>
            """
        )
        
        with gr.Row():
            with gr.Column():
                query_input = gr.Textbox(
                    label="Ask your question",
                    placeholder="Example: What tables are in this database?",
                    lines=2
                )
                query_button = gr.Button("Ask", variant="primary")
            
        with gr.Row():
            with gr.Column():
                sql_output = gr.Textbox(label="Generated SQL", interactive=False)
                execute_button = gr.Button("Run SQL")
            
        with gr.Row():
            result_output = gr.Textbox(label="Result", lines=10, interactive=False)
            
        with gr.Accordion("Example Questions", open=False):
            gr.HTML('<div class="example-title">Basic Information:</div>')
            with gr.Row(elem_classes=["examples"]):
                gr.HTML('<div class="example-box">What tables are in this database?</div>')
                gr.HTML('<div class="example-box">How many columns are in the kettlepump table?</div>')
                gr.HTML('<div class="example-box">Count rows in the users table</div>')
                
            gr.HTML('<div class="example-title">Data Analysis:</div>')
            with gr.Row(elem_classes=["examples"]):
                gr.HTML('<div class="example-box">What is the highest AC current in kettlepump?</div>')
                gr.HTML('<div class="example-box">Find the average age of users</div>')
                gr.HTML('<div class="example-box">Who has the longest name in the users table?</div>')
                
            gr.HTML('<div class="example-title">Null Value Analysis:</div>')
            with gr.Row(elem_classes=["examples"]):
                gr.HTML('<div class="example-box">Count null values in the Button Down column</div>')
                gr.HTML('<div class="example-box">Show me rows where Stopped column is null</div>')
                gr.HTML('<div class="example-box">How many missing values in the survey table?</div>')
                
            gr.HTML('<div class="example-title">Unique Values:</div>')
            with gr.Row(elem_classes=["examples"]):
                gr.HTML('<div class="example-box">How many unique ages in the users table?</div>')
                gr.HTML('<div class="example-box">Count distinct values in AC Current column</div>')
                
        gr.HTML(
            """
            <div class="footer">
                SQL RAG Dashboard - Query databases with natural language
            </div>
            """
        )
        
        # Set up event handlers
        query_button.click(handle_nl_query, inputs=query_input, outputs=[query_input, sql_output, result_output])
        execute_button.click(lambda sql: (sql, *execute_sql(sql)), inputs=sql_output, outputs=[sql_output, result_output])
        
        # Setup example click handlers
        for el in demo.blocks.values():
            if isinstance(el, gr.HTML) and "example-box" in el.value:
                example_text = el.value.split('>')[1].split('<')[0]
                el.click(lambda q: q, inputs=[gr.Textbox(value=example_text)], outputs=query_input)
                el.click(handle_nl_query, inputs=[gr.Textbox(value=example_text)], outputs=[query_input, sql_output, result_output])
    
    return demo

if __name__ == "__main__":
    # Create and launch the interface
    demo = create_interface()
    demo.launch(share=False)

# app.py
import gradio as gr
from sqlalchemy import MetaData, text, create_engine
from db_utils import (
    run_query, get_schema, create_table, create_sample_table_if_not_exists,
    insert_row, bulk_insert_csv, list_tables, get_table_columns,
    create_foreign_key_relation, create_table_from_csv, generate_metadata_for_table,
    remove_metadata_for_table
)
from llm_utils import nl_to_sql
import pandas as pd
import os

# Initialize DB
engine = create_engine("sqlite:///rag.db", echo=False)
create_sample_table_if_not_exists()
from db_utils import sync_metadata_with_existing_tables
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

def build_prompt_from_metadata(user_question):
    import json
    prompt = (
                "You are an expert SQLite query generator."
            "Use only valid SQLite syntax. Do not use information_schema or SHOW TABLES."
            "Always reference the provided metadata to understand available tables and columns."
            "Only return the SQL query. Do not explain your answer."
            "Here is the database metadata:"
        )

    meta_dir = "metadata"
    for file in os.listdir(meta_dir):
        if file.endswith(".json"):
            with open(os.path.join(meta_dir, file)) as f:
                data = json.load(f)
                prompt += f"Table: {data['table']} — {data.get('description', 'No description')}"
                for col, desc in data['columns'].items():
                    prompt += f"- {col}: {desc}"
                prompt += " "

    prompt += "Example 1:"
    prompt += "Question: How many users are older than 30?"
    prompt += "SQL: SELECT COUNT(*) FROM users WHERE age > 30;"

    prompt += "Example 2:"
    prompt += "Question: How many columns are in the 'survey' table?"
    prompt += "SQL: SELECT COUNT(*) FROM survey;"

    prompt += "Example 3:"
    prompt += "Question: How many NULL values are in each column of the 'survey2' table?"
    prompt += (
        "SQL: SELECT 'column1' AS column_name, COUNT(*) FROM survey2 WHERE column1 IS NULL"
        "UNION ALL"
        "SELECT 'column2', COUNT(*) FROM survey2 WHERE column2 IS NULL;"
    )

    prompt += "Now answer the following:"
    prompt += f"Question: {user_question} SQL:"
    

    return prompt

with gr.Blocks() as demo:
    gr.Markdown("SQL RAG Dashboard (Talk to Your Database)")

    # Ask tab
    with gr.Tab("Ask with Natural Language"):
        nl_input = gr.Textbox(label="Ask your question")
        sql_out = gr.Textbox(label="Generated SQL")
        result_out = gr.Textbox(label="Result")
        btn_run = gr.Button("Ask")

        def handle_nl_query(user_question):
            prompt = build_prompt_from_metadata(user_question)
            sql = nl_to_sql(prompt)
            result = run_query(sql)
            return sql, result if isinstance(result, str) else result.to_markdown(index=False)

        btn_run.click(handle_nl_query, inputs=nl_input, outputs=[sql_out, result_out])

    # Create tab
    with gr.Tab("Upload CSV"):
        csv_file = gr.File(label="Upload CSV File (.csv only)", file_types=[".csv"])
        csv_table_name = gr.Textbox(label="New Table Name")
        csv_output = gr.Textbox(label="Upload Status")

        def handle_csv_upload(file, table_name):
            return create_table_from_csv(file, table_name)

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
            generate_metadata_for_table(tname)
            return f"✅ {tname} created.", get_schema()

        btn_create.click(create_final_table, inputs=[table_name, col_state], outputs=[status, schema])

    # Delete tab
    with gr.Tab("View/Delete Tables"):
        schema_view_btn = gr.Button("Refresh Details")
        schema_output = gr.Textbox(label="Schema", lines=30)

        def delete_table(table_name):
            try:
                run_query(f"DROP TABLE IF EXISTS {table_name}")
                remove_metadata_for_table(table_name)
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

        delete_table_dropdown = gr.Dropdown(label="Select Table to Delete", choices=shared_tables)
        refresh_meta_btn = gr.Button("Refresh Metadata")
        meta_status = gr.Textbox(label="Metadata Refresh Status", interactive=False)

        def refresh_metadata():
            sync_metadata_with_existing_tables()
            return "✅ Metadata synced with current database tables."

        refresh_meta_btn.click(refresh_metadata, outputs=[meta_status])
        delete_btn = gr.Button("Delete Selected Table")
        delete_status = gr.Textbox(label="Delete Status")
        preview_output = gr.Textbox(label="Preview", lines=8)

        delete_table_dropdown.change(preview_table_rows, inputs=delete_table_dropdown, outputs=preview_output)
        schema_view_btn.click(get_pretty_schema, outputs=[schema_output])
        delete_btn.click(
            delete_table,
            inputs=[delete_table_dropdown],
            outputs=[delete_status, schema_output, delete_table_dropdown, table_dropdown, existing_table, from_table, to_table, from_col, to_col]
        )

    demo.launch(share=True)

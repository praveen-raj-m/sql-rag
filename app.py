# app.py
import gradio as gr
from sqlalchemy import MetaData, text, create_engine
from db_utils import (
    run_query, get_schema, create_table, create_sample_table_if_not_exists,
    insert_row, bulk_insert_csv, list_tables, get_table_columns, delete_row_by_id,
    create_foreign_key_relation, create_table_from_csv
)
from llm_utils import nl_to_sql

# Initialize DB connection
engine = create_engine("sqlite:///rag.db", echo=False)

# Ensure base tables exist
create_sample_table_if_not_exists()

# Helper to format schema cleanly
def get_pretty_schema():
    meta = MetaData()
    meta.reflect(bind=engine)
    output = []
    for table in meta.tables.values():
        output.append(f"Table: {table.name}")
        for col in table.columns:
            output.append(f"  - {col.name} ({col.type})")
        output.append("")
    return "\n".join(output)

with gr.Blocks() as demo:
    gr.Markdown("# SQL RAG Dashboard (Local LLM + Ollama + SQLite)")

    # --- NATURAL LANGUAGE TO SQL ---
    with gr.Tab("Ask with Natural Language"):
        nl_input = gr.Textbox(label="Ask a question", placeholder="e.g., Show users older than 25")
        sql_out = gr.Textbox(label="Generated SQL")
        result_out = gr.Textbox(label="Result (Markdown table)")
        btn_run = gr.Button("Generate and Run")

        def handle_nl_query(user_query):
            schema = get_schema()
            sql = nl_to_sql(user_query, schema)
            result = run_query(sql)
            return sql, result if isinstance(result, str) else result.to_markdown(index=False)

        btn_run.click(handle_nl_query, inputs=[nl_input], outputs=[sql_out, result_out])

    # --- TABLE CREATION ---
    with gr.Tab("Create Table"):
        table_name = gr.Textbox(label="Table Name")
        with gr.Row():
            col_name = gr.Textbox(label="Column Name")
            col_type = gr.Dropdown(label="Column Type", choices=["TEXT", "INTEGER", "REAL", "BOOLEAN", "DATE", "DATETIME", "BLOB"], value="TEXT")
            add_col = gr.Button("Add Column")

        col_preview = gr.Textbox(label="Columns Preview")
        col_state = gr.State([])

        def add_column(n, t, state):
            state.append((n, t))
            return ", ".join([f"{x[0]} {x[1]}" for x in state]), state

        add_col.click(add_column, inputs=[col_name, col_type, col_state], outputs=[col_preview, col_state])

        btn_create = gr.Button("Create Table")
        status = gr.Textbox(label="Status")
        schema = gr.Textbox(label="Current Schema")

        def create_final_table(tname, columns):
            col_dict = {n: t for n, t in columns}
            result = create_table(tname, col_dict)
            return f"Table '{tname}' created.", get_schema()

        btn_create.click(create_final_table, inputs=[table_name, col_state], outputs=[status, schema])

    # --- ROW INSERTION ---
    with gr.Tab("Insert Data"):
        table_dropdown = gr.Dropdown(label="Select Table", choices=list_tables())
        col_info = gr.Textbox(label="Table Columns")
        row_input = gr.Textbox(label="Row Data (comma-separated)")
        insert_btn = gr.Button("Insert Row")
        insert_status = gr.Textbox(label="Insert Status")

        table_dropdown.change(lambda x: get_table_columns(x), inputs=table_dropdown, outputs=col_info)
        insert_btn.click(insert_row, inputs=[table_dropdown, row_input], outputs=[insert_status])

    # --- BULK CSV UPLOAD ---
    with gr.Tab("Bulk Upload CSV"):
        file_input = gr.File(label="CSV File", file_types=[".csv"])
        upload_mode = gr.Radio(label="Upload Mode", choices=["Append to Existing Table", "Create New Table"], value="Append to Existing Table")

        existing_table = gr.Dropdown(label="Choose Existing Table", choices=list_tables(), visible=True)
        new_table_name = gr.Textbox(label="New Table Name", visible=False)

        upload_btn = gr.Button("Upload CSV")
        upload_status = gr.Textbox(label="Status")

        def toggle_fields(mode):
            return gr.update(visible=(mode == "Append to Existing Table")), gr.update(visible=(mode == "Create New Table"))

        upload_mode.change(toggle_fields, inputs=[upload_mode], outputs=[existing_table, new_table_name])

        def handle_csv_upload(file, mode, existing, new_name):
            if not file:
                return "No file uploaded."
            if mode == "Append to Existing Table":
                return bulk_insert_csv(file, existing)
            else:
                return create_table_from_csv(file, new_name)

        upload_btn.click(handle_csv_upload, inputs=[file_input, upload_mode, existing_table, new_table_name], outputs=[upload_status])

    # --- DELETE ROW BY ID ---
    with gr.Tab("Delete Row by ID"):
        del_table = gr.Dropdown(label="Select Table", choices=list_tables())
        del_id = gr.Textbox(label="Row ID to delete")
        del_result = gr.Textbox(label="Delete Result")
        del_btn = gr.Button("Delete")

        del_btn.click(delete_row_by_id, inputs=[del_table, del_id], outputs=[del_result])

    # --- FOREIGN KEY CREATION ---
    with gr.Tab("Create Relationship (FK)"):
        from_table = gr.Dropdown(label="From Table", choices=list_tables())
        from_col = gr.Textbox(label="Column in From Table")
        to_table = gr.Dropdown(label="To Table", choices=list_tables())
        to_col = gr.Textbox(label="Primary Key Column in To Table")
        fk_status = gr.Textbox(label="Status")
        fk_btn = gr.Button("Add Foreign Key")

        fk_btn.click(create_foreign_key_relation, inputs=[from_table, from_col, to_table, to_col], outputs=[fk_status])

    # --- TABLE SCHEMA VIEW ---
    with gr.Tab("View Table Details"):
        schema_view_btn = gr.Button("Refresh Table Details")
        schema_output = gr.Textbox(label="Current Table Details", lines=30)

        schema_view_btn.click(get_pretty_schema, outputs=[schema_output])

demo.launch(share=True)

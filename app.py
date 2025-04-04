import gradio as gr
from db_utils import run_query, get_schema, create_table
from llm_utils import nl_to_sql

def handle_nl_query(nl_input):
    schema = get_schema()
    sql = nl_to_sql(nl_input, schema)
    result = run_query(sql)
    return sql, result

def handle_table_create(table_name, col_str):
    """
    col_str format: name TEXT, age INTEGER
    """
    try:
        col_dict = {pair.split()[0]: pair.split()[1] for pair in col_str.split(",")}
        res = create_table(table_name, col_dict)
        return f"Table '{table_name}' created.", get_schema()
    except Exception as e:
        return f"Error: {e}", get_schema()

with gr.Blocks() as demo:
    gr.Markdown("## 🧠 SQL Generator Dashboard with Ollama")

    with gr.Tab("Ask in Natural Language"):
        nl_input = gr.Textbox(label="Ask your question (e.g., show all users over 25)")
        sql_out = gr.Textbox(label="Generated SQL")
        table_out = gr.Dataframe(label="Query Result")

        run_btn = gr.Button("Generate and Run")
        run_btn.click(handle_nl_query, inputs=[nl_input], outputs=[sql_out, table_out])

    with gr.Tab("Create Table"):
        table_name = gr.Textbox(label="Table Name")
        columns = gr.Textbox(label="Columns (e.g., name TEXT, age INTEGER)")
        msg = gr.Textbox(label="Message")
        schema_out = gr.Textbox(label="Current Schema")

        create_btn = gr.Button("Create Table")
        create_btn.click(handle_table_create, inputs=[table_name, columns], outputs=[msg, schema_out])

demo.launch()


# SQL RAG Dashboard

A fully local dashboard to query, manage, and build SQL databases using natural language via a local LLM (LLaMA 3.2 with Ollama).

## 🔧 Features

- **Natural Language to SQL**: Translate plain English to executable SQL.
- **Advanced Queries**: Supports aggregates, null checks, text analysis, and more.
- **Model Context Protocol (MCP)**: 
  - Validates schemas
  - Enriches queries with context
  - Ensures metadata-aware, accurate SQL

  ## Why MCP?

  | Traditional LLMs           | MCP Approach                          |
  |---------------------------|----------------------------------------|
  | Relies on SQL knowledge   | Uses real-time schema + metadata       |
  | Generic output            | Accurate, production-ready SQL         |
  | No schema awareness       | Dynamic schema + error handling        |
  | Limited functionality     | Supports advanced ops (e.g., nulls, joins, text) |

- **Table & Data Management**:
  - Create/view/delete tables
  - Insert rows manually or via CSV
  - Manage relationships



## Quick Start

### Prerequisites
- Python 3.8+
- [Ollama](https://ollama.ai/)

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/sql-rag-dashboard.git
cd sql-rag-dashboard

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start Ollama with LLaMA 3.2
ollama pull llama3
ollama run llama3
```

### Running the Dashboard

```bash
python app.py
```

Visit `http://localhost:7860` to use the application.

### Setting up sample data

To create sample tables:

```bash
python create_sample_tables.py
```

## Database Management Tools

Several tools are included to help manage your database:

```bash
# List all tables
python db_manager.py list

# Delete a specific table
python db_manager.py delete <table_name>

# Create a sample users table
python db_manager.py create-users

# Refresh the schema
python db_manager.py refresh-schema
```

Visit `http://localhost:7860` in your browser.


More: [README_DB_TOOLS.md](README_DB_TOOLS.md)

## Example Queries

- "Show all products in the Electronics category"
- "Average salary in Engineering?"
- "Count nulls in total_amount column"
- "Sum of stock for Kitchen products"
- "Words in product descriptions"

## Architecture

- **DB**: SQLite (`sqlite.db`)
- **LLM**: LLaMA 3.2 via Ollama
- **UI**: Gradio
- **MCP**:
  - Pydantic schema validation
  - Real-time schema updates
  - Context-aware prompt generation
  - Advanced query handlers




## Acknowledgements

- [LLaMA 3.2](https://ai.meta.com/llama/) by Meta AI  
- [Ollama](https://ollama.ai/)  
- [Gradio](https://www.gradio.app/)

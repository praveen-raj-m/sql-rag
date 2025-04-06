# SQL RAG Dashboard

A fully local dashboard that lets you query, manage, and build SQL databases using natural language powered by a local LLM (LLaMA 3.2 via Ollama).

## Features

- **Natural Language to SQL**: Ask questions in plain English and get SQL queries and results
- **Advanced Query Capabilities**: Support for complex operations including:
  - Counting null values in columns
  - Finding averages, maximums, and minimums
  - Calculating sums and counting distinct values
  - Text analysis (word count, character count)
- **Table Management**: Create, view, and delete tables with an intuitive interface
- **Data Management**:
  - Step-by-step table creation with column type dropdowns
  - Manual row insertion
  - Bulk CSV upload (append or create new tables)
- **Relationship Management**: Create foreign key relationships between tables
- **Fully Local Execution**: No external APIs or cloud services required
- **Interactive Dashboard**: Built with Gradio for a user-friendly experience

## Sample Tables

The repository includes scripts to create these sample tables:

1. **Products**: Product inventory with name, category, price, stock, and description
2. **Employees**: Employee records with names, positions, departments, salaries
3. **Orders**: Customer order information with payment methods and status

## Getting Started

### Prerequisites

- Python 3.8 or higher
- [Ollama](https://ollama.ai/) installed locally

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

For a detailed guide, see [README_DB_TOOLS.md](README_DB_TOOLS.md).

## Example Natural Language Queries

- "Show all products in the Electronics category"
- "What is the average salary of employees in the Engineering department?"
- "How many orders have a status of Shipped?"
- "Count null values in the total_amount column of the orders table"
- "Find the maximum price in the products table"
- "What is the sum of stock for Kitchen products?"

## Technical Details

- SQLite database (stored as `sqlite.db`)
- LLaMA 3.2 integration via Ollama
- Gradio for the user interface
- Dynamic schema management with JSON metadata

## License

[MIT](LICENSE)

## Acknowledgements

- [LLaMA 3.2](https://ai.meta.com/llama/) by Meta AI
- [Ollama](https://ollama.ai/) for local LLM hosting
- [Gradio](https://www.gradio.app/) for the web interface

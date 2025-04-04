# SQL RAG Dashboard (Ollama + LLaMA 3.2 + SQLite)

A fully local dashboard that lets you query, manage, and build SQL databases using natural language powered by a local LLM (LLaMA 3.2 via Ollama).

---

## Features

- Natural language to SQL using local LLaMA 3.2
- Step-by-step table creation with column type dropdowns
- Manual row insertion
- Bulk CSV upload
  - Append to existing tables
  - Create a new table automatically from CSV headers
- Delete row by ID
- Create foreign key relationships
- Fully local execution (no external APIs or cloud)
- Shareable Gradio dashboard

---

## Setup Instructions

### 1. Clone the Repo

```bash
git clone https://github.com/YOUR_USERNAME/sql-rag.git
cd sql-rag
```

### 2. Create a Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Requirements

```bash
pip install -r requirements.txt
```

### 4. Start Ollama with LLaMA 3.2

```bash
ollama pull llama3
ollama run llama3
```

### 5. Run the Dashboard

```bash
python app.py
```

Visit `http://localhost:7860` to use the app.

---

## Example Natural Language Prompts

- Show all users older than 25
- List all products with price over 500
- Insert a new user named John, aged 40
- Delete the product with id 3

---

## CSV Upload Options

When uploading a CSV file:

- Choose "Append to Existing Table" and select a table
- Or choose "Create New Table", provide a name, and it will:
  - Automatically infer columns from CSV headers
  - Create the table and insert data

---

## Data Safety

- Uses SQLite, file stored as `rag.db`
- Use `.gitignore` to exclude the database and virtual environment

---

## Roadmap

- Export results as downloadable CSV
- Visual query builder
- Chart and data visualizations
- Multi-user login and access control
- Deploy with Docker or Hugging Face Spaces

---

## License

MIT

---

## Contributing

Pull requests and feedback welcome. Open an issue or submit a PR.
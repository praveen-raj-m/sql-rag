# SQL RAG Dashboard

A natural language interface for SQLite databases that lets users query databases using everyday language instead of SQL code.

## Overview

SQL RAG Dashboard is a Python application that combines:

- Natural Language Processing to understand user questions
- Dynamic SQL query generation
- Semantic search capabilities with RAG (Retrieval-Augmented Generation)
- An intuitive web interface built with Gradio

Users can ask questions like "How many null values are in the 'Button Down' column?" or "Show me the longest name in the users table" without writing a single line of SQL.

## Features

- **Natural Language Queries**: Ask questions about your data in plain English
- **Automatic SQL Generation**: Converts natural language into optimized SQL queries
- **Complex Column Handling**: Works with complex column names containing spaces and special characters
- **Descriptive Statistics**: Supports common analytical functions (max, min, avg, count, etc.)
- **Text Analysis**: Analyze text fields (word counts, character counts, etc.)
- **Null Value Analysis**: Easily identify and analyze missing data
- **Dynamic Schema Loading**: Automatically adapts to any SQLite database schema
- **Formatted Results**: Presents results in a clear, readable format

## Installation

### Requirements

- Python 3.8+
- SQLite database

### Setup

1. Clone the repository:

   ```
   git clone https://github.com/yourusername/sql-rag-dashboard.git
   cd sql-rag-dashboard
   ```

2. Install dependencies:

   ```
   pip install -r requirements.txt
   ```

3. Place your SQLite database file in the project directory as `sqlite.db`

4. Run the application:

   ```
   python app.py
   ```

5. Open the dashboard in your browser at the URL shown in the terminal

## Usage Examples

Ask questions like:

- "What tables are in this database?"
- "How many rows are in the users table?"
- "Show me the average age of users"
- "Find the highest AC current value in kettlepump"
- "Count null values in the Button Down column"
- "Who has the longest name in the users table?"
- "How many unique values are in this column?"

## Testing

Run the comprehensive test suite:

```
python final_test.py
```

The test suite covers a wide range of query types and edge cases to ensure the system works correctly with your database.

## Project Structure

- `app.py`: Main application with Gradio interface
- `llm_utils.py`: Natural language to SQL conversion logic
- `final_test.py`: Comprehensive test suite
- `requirements.txt`: Project dependencies

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

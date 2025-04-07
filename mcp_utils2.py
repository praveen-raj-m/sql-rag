import json
import os
import re
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# --- Schema & Validation Layer ---

class MCPValidator:
    def __init__(self, schema_file: str = "metadata/schema.json", db_path: str = "rag.db"):
        self.schema_file = schema_file
        self.db_path = db_path
        self.schema = self._load_schema()
        self._validate_schema_with_db()
        self.last_schema_update = self._get_schema_mtime()

    def _get_schema_mtime(self) -> float:
        try:
            return os.path.getmtime(self.schema_file)
        except:
            return 0

    def _check_schema_update(self) -> bool:
        current_mtime = self._get_schema_mtime()
        if current_mtime > self.last_schema_update:
            self.last_schema_update = current_mtime
            self.schema = self._load_schema()
            return True
        return False

    def _load_schema(self) -> Dict:
        try:
            with open(self.schema_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def _validate_schema_with_db(self) -> None:
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            db_tables = [row[0] for row in cursor.fetchall()]
            schema_changed = False
            for table in list(self.schema.keys()):
                if table not in db_tables:
                    del self.schema[table]
                    schema_changed = True
            if schema_changed:
                self.update_schema_file()
        except Exception as e:
            print(f"Warning: Could not validate schema with database: {str(e)}")
        finally:
            conn.close()

    def update_schema_file(self) -> None:
        os.makedirs(os.path.dirname(self.schema_file), exist_ok=True)
        with open(self.schema_file, 'w') as f:
            json.dump(self.schema, f, indent=2)

    def validate_sql_query(self, query: str) -> bool:
        self._check_schema_update()
        query_lower = query.lower()
        dangerous_keywords = ['drop', 'delete', 'alter', 'update', 'insert', 'create', 'truncate']
        if any(keyword in query_lower for keyword in dangerous_keywords):
            return False
        if not query_lower.strip().startswith('select'):
            return False
        table_pattern = r'from\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        tables = re.findall(table_pattern, query_lower)
        for table in tables:
            if table not in self.schema:
                return False
        allowed_aggregates = ['count', 'sum', 'avg', 'min', 'max']
        has_aggregate = any(agg in query_lower for agg in allowed_aggregates)
        column_pattern = r'select\s+(.*?)\s+from'
        columns_match = re.search(column_pattern, query_lower)
        if columns_match:
            columns = []
            current_col = ""
            in_quotes = False
            for char in columns_match.group(1):
                if char == '"':
                    in_quotes = not in_quotes
                    current_col += char
                elif char == ',' and not in_quotes:
                    columns.append(current_col.strip())
                    current_col = ""
                else:
                    current_col += char
            if current_col:
                columns.append(current_col.strip())
            for col in columns:
                if has_aggregate and any(agg in col.lower() for agg in allowed_aggregates):
                    continue
                if col == '*':
                    continue
                col = col.strip('"')
                if not any(
                    col in self.schema[table].keys()
                    for table in tables
                    if table in self.schema
                ):
                    return False
        return True

    def validate_response(self, response: str) -> bool:
        self._check_schema_update()
        if not any(marker in response.lower() for marker in ['select', 'from']):
            return False
        hallucination_patterns = [
            r"i don't know", r"i cannot", r"not sure", r"no information",
            r"no data", r"error", r"undefined", r"null", r"empty"
        ]
        for pattern in hallucination_patterns:
            if re.search(pattern, response.lower()):
                return False
        return True

    def format_response(self, sql_query: str, result: str) -> str:
        return f"Based on your query, here's what I found:\n\n{result}\n\nGenerated SQL: {sql_query}"

    def update_schema(self, table_name: str, columns: Dict[str, str]) -> None:
        self.schema[table_name] = columns
        self.update_schema_file()

    def get_table_info(self, table_name: str) -> Optional[Dict]:
        self._check_schema_update()
        return self.schema.get(table_name)

    def list_tables(self) -> List[str]:
        self._check_schema_update()
        return list(self.schema.keys())

    def remove_table(self, table_name: str) -> None:
        if table_name in self.schema:
            del self.schema[table_name]
            self.update_schema_file()

# --- Prompt Context Memory ---

class PromptContextManager:
    def __init__(self):
        self.memory: List[Dict[str, str]] = []

    def add(self, user_prompt: str, llm_response: str) -> None:
        self.memory.append({
            "timestamp": datetime.utcnow().isoformat(),
            "user": user_prompt,
            "llm": llm_response
        })

    def get_recent_context(self, n: int = 5) -> List[Dict[str, str]]:
        return self.memory[-n:]

    def clear_memory(self):
        self.memory = []

def run_query(query: str, db_path: str = "rag.db") -> str:
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        conn.close()
        return json.dumps([dict(zip(columns, row)) for row in result], indent=2)
    except Exception as e:
        return f"Error executing query: {str(e)}"

# --- LLM Placeholder (Mock) ---

def generate_sql_from_prompt(prompt: str, schema: Dict) -> str:
    """
    Placeholder for LLM generation.
    In production, hook this into your local LLM (e.g. via Ollama or API).
    """
    return f"SELECT * FROM sample_table WHERE column LIKE '%{prompt.lower()}%';"

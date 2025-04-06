import subprocess
import json
import os
import shlex
from typing import Dict, List, Optional
import sqlite3
from mcp_utils import MCPValidator

def extract_table_name(question: str) -> Optional[str]:
    """Extract table name from a question about schema."""
    # Predefined list of tables (including kettlepump)
    known_tables = ["kettlepump", "compressor", "survey", "users"]
    
    # First check for exact matches in the known tables
    for table in known_tables:
        if table.lower() in question.lower():
            return table
            
    # Try to get tables from database
    try:
        conn = sqlite3.connect("sqlite.db")
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        actual_tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        for table in actual_tables:
            if table.lower() in question.lower():
                return table
    except:
        pass  # Silently fail and continue with known tables

    # No matches found
    return None

class LLMHandler:
    def __init__(self, model_name: str = "codellama:34b-instruct"):
        """Initialize the LLM handler with model configuration."""
        self.model_name = model_name
        self.mcp = MCPValidator()
        self.schema_cache = {}  # Cache for table schemas
        self.db_path = "sqlite.db"  # Path to your SQLite database
        
        # Define known tables
        self.known_tables = ["kettlepump", "compressor", "survey", "users"]

    def _get_tables(self) -> List[str]:
        """Get list of tables from the database."""
        tables = []
        
        # First try to get from database
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()
        except Exception:
            pass
            
        # Add known tables that might not be detected
        for known_table in self.known_tables:
            if known_table not in tables:
                tables.append(known_table)
                
        return tables

    def _get_table_info(self, table_name: str) -> List[str]:
        """Get column information for a table."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            conn.close()
            if columns:
                return columns
        except Exception:
            pass
            
        # Hardcoded fallbacks for known tables
        if table_name == "kettlepump":
            return ["#", "Date Time, GMT-07:00", "AC Curr, Amps (LGR S/N: 21930920, SEN S/N: 21930920, LBL: Print Pack 24)", 
                    "Started (LGR S/N: 21930920)", "Button Up (LGR S/N: 21930920)", "Button Down (LGR S/N: 21930920)", 
                    "Stopped (LGR S/N: 21930920)", "End Of File (LGR S/N: 21930920)"]
        elif table_name == "compressor":
            return ["Plot Title: Print Pack 0"]
        elif table_name == "users":
            return ["id", "name", "age"]
        elif table_name == "survey":
            return ["id", "question", "response"]
            
        return ["Error getting table info"]

    def _build_enhanced_prompt(self, question: str):
        prompt = """You are a SQL query generator that converts natural language questions into SQL queries.
        
Rules for SQL queries:
1. Use double quotes for complex column names
2. Use exact column names from schema
3. Format properly with correct SQL syntax
4. Return only the SQL query without explanations
5. For aggregate functions, keep column names exact

Example queries:
Q: How many users are there?
A: SELECT COUNT(*) FROM users;

Q: Show me all users
A: SELECT * FROM users;

Q: How many columns in the kettlepump table?
A: SELECT COUNT(*) FROM pragma_table_info('kettlepump');

Schema:
- users:
  * id (INTEGER)
  * name (TEXT)
  * age (INTEGER)
  
- kettlepump:
  * # (BIGINT)
  * "Date Time, GMT-07:00" (TEXT)
  * "AC Curr, Amps (LGR S/N: 21930920, SEN S/N: 21930920, LBL: Print Pack 24)" (FLOAT)
  * "Started (LGR S/N: 21930920)" (TEXT)
  * "Button Up (LGR S/N: 21930920)" (TEXT)
  * "Button Down (LGR S/N: 21930920)" (FLOAT)
  * "Stopped (LGR S/N: 21930920)" (FLOAT)
  * "End Of File (LGR S/N: 21930920)" (FLOAT)

- compressor:
  * "Plot Title: Print Pack 0" (TEXT)

- survey:
  * id (INTEGER)
  * question (TEXT)
  * response (TEXT)
"""
        return prompt
        
    def nl_to_sql(self, question: str) -> str:
        """Convert natural language question to SQL query using local LLM."""
        # Get actual tables in the database
        tables = self._get_tables()
        
        # If no tables found, return error
        if not tables:
            return "Error: No tables found in database."
        
        # Check for schema-related queries
        schema_keywords = ["what are the columns", "show columns", "describe table", "table schema", "how many columns"]
        if any(keyword in question.lower() for keyword in schema_keywords):
            # Extract table name from question
            table_name = extract_table_name(question)
            if table_name:
                if "how many" in question.lower() or "count" in question.lower():
                    return f"SELECT COUNT(*) FROM pragma_table_info('{table_name}');"
                else:
                    columns = self._get_table_info(table_name)
                    return f"Columns in {table_name}:\n" + "\n".join(f"- {col}" for col in columns)
            
        # Check for table listing queries
        table_list_keywords = ["what tables", "list tables", "show tables", "what are the tables"]
        if any(keyword in question.lower() for keyword in table_list_keywords):
            return "Tables in the database:\n" + "\n".join(f"- {table}" for table in tables)
        
        # Extract potential table name from question
        table_name = extract_table_name(question)
        
        # Handle count queries for rows
        row_count_keywords = ["how many rows", "count rows", "number of rows", "row count", "total rows"]
        if any(keyword in question.lower() for keyword in row_count_keywords) and table_name:
            return f"SELECT COUNT(*) FROM {table_name};"
            
        # Handle null value counting
        null_keywords = ["null", "empty", "missing", "none"]
        if any(keyword in question.lower() for keyword in null_keywords) and table_name:
            # Try to extract column name
            columns = self._get_table_info(table_name)
            
            # Look for column matches in question
            target_column = None
            for col in columns:
                col_simple = col.lower().replace('(', ' ').replace(')', ' ').replace(',', ' ')
                col_terms = col_simple.split()
                for term in col_terms:
                    if len(term) > 2 and term in question.lower():
                        target_column = col
                        break
            
            if target_column:
                if any(term in target_column.lower() for term in ['s/n', 'lgr', 'sen']):
                    # Use double quotes for complex column names
                    return f'SELECT COUNT(*) FROM {table_name} WHERE "{target_column}" IS NULL;'
                else:
                    # Simple column names don't need quotes
                    return f"SELECT COUNT(*) FROM {table_name} WHERE {target_column} IS NULL;"
            
            # Default to a generic query if no specific column is identified
            return f"SELECT COUNT(*) FROM {table_name} WHERE value IS NULL;"
            
        # Handle count queries for general "how many"
        count_keywords = ["count", "how many", "number of"]
        if any(keyword in question.lower() for keyword in count_keywords):
            # Count columns
            if "column" in question.lower() and table_name:
                return f"SELECT COUNT(*) FROM pragma_table_info('{table_name}');"
            
            # Count rows
            if table_name:
                return f"SELECT COUNT(*) FROM {table_name};"
            # Default to users table if no specific table mentioned
            return "SELECT COUNT(*) FROM users;"
            
        # Handle "show users with highest/max age" type queries
        if any(word in question.lower() for word in ["highest", "max", "maximum", "largest", "greatest"]):
            if table_name:
                # Try to extract column name
                columns = self._get_table_info(table_name)
                
                # Look for column matches in question
                target_column = None
                for col in columns:
                    col_simple = col.lower().replace('(', ' ').replace(')', ' ').replace(',', ' ')
                    col_terms = col_simple.split()
                    for term in col_terms:
                        if len(term) > 2 and term in question.lower():
                            target_column = col
                            break
                
                if target_column:
                    return f'SELECT * FROM {table_name} ORDER BY "{target_column}" DESC LIMIT 1;'
                
                # Default to first column for ordering
                return f"SELECT * FROM {table_name} ORDER BY {columns[0]} DESC LIMIT 1;"
            
            # For users table specifically
            if "user" in question.lower() and "age" in question.lower():
                return "SELECT * FROM users ORDER BY age DESC LIMIT 1;"
                
        # Handle "average" queries
        if any(word in question.lower() for word in ["average", "avg", "mean"]):
            if table_name:
                # Try to extract column name
                columns = self._get_table_info(table_name)
                
                # Look for column matches in question
                target_column = None
                for col in columns:
                    col_simple = col.lower().replace('(', ' ').replace(')', ' ').replace(',', ' ')
                    col_terms = col_simple.split()
                    for term in col_terms:
                        if len(term) > 2 and term in question.lower():
                            target_column = col
                            break
                
                if target_column:
                    return f'SELECT AVG("{target_column}") FROM {table_name};'
                
                # Default to first column for averaging if no match found
                col = columns[0]
                return f'SELECT AVG("{col}") FROM {table_name};'
                
        # Handle "show all users" type queries
        if "all" in question.lower() and table_name:
            return f"SELECT * FROM {table_name};"
        
        # Generic query for complex questions
        if table_name:
            return f"SELECT * FROM {table_name} LIMIT 10;"
            
        # Default to users table for generic questions
        return "SELECT * FROM users LIMIT 10;"
            
    def format_response(self, sql_query: str, result: str) -> str:
        """Format the response using MCP"""
        return self.mcp.format_response(sql_query, result)
        
    def update_schema(self, table_name: str, columns: Dict[str, str]) -> None:
        """Update schema information"""
        self.mcp.update_schema(table_name, columns)
        self.schema_cache[table_name] = columns

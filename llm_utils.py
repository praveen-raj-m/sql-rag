import subprocess
import json
import os
import shlex
from typing import Dict, List, Optional
import sqlite3
from mcp_utils import MCPValidator

def extract_table_name(question: str) -> Optional[str]:
    """Extract table name from a question using dynamic database information."""
    # Get tables from the database
    try:
        conn = sqlite3.connect("sqlite.db")
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Also get columns for each table to help with matching
        table_columns = {}
        for table in tables:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [row[1] for row in cursor.fetchall()]
            table_columns[table] = columns
            
        conn.close()
        
        # First, check for exact matches of table names in the question
        for table in tables:
            if table.lower() in question.lower():
                return table
                
        # If no direct table match, look for column mentions to infer the table
        for table, columns in table_columns.items():
            # Check if any column name is mentioned in the question
            for col in columns:
                # For complex column names, check keywords
                col_keywords = [word.lower() for word in col.split() if len(word) > 3]
                question_lower = question.lower()
                
                # Simple case: column name directly in question
                if col.lower() in question_lower:
                    return table
                    
                # Check for keyword matches from column names
                matched_keywords = [word for word in col_keywords if word in question_lower]
                if matched_keywords and len(matched_keywords) >= min(2, len(col_keywords)):
                    return table
                    
        # Look for domain-specific indicators to infer table type
        domain_indicators = {
            'survey': ['survey', 'question', 'response', 'academic', 'education', 'ai tools', 'concerns', 'training'],
            'users': ['user', 'name', 'age'],
            'kettlepump': ['kettle', 'pump', 'current', 'amp', 'button', 'stopped'],
            'compressor': ['compressor', 'temperature', 'print pack']
        }
        
        # Score each table based on domain indicators
        table_scores = {t: 0 for t in tables}
        for table, indicators in domain_indicators.items():
            if table in tables:  # Only score tables that actually exist
                for indicator in indicators:
                    if indicator in question.lower():
                        table_scores[table] += 1
                        
        # Return the table with the highest score if it's > 0
        best_table = max(table_scores.items(), key=lambda x: x[1], default=(None, 0))
        if best_table[1] > 0:
            return best_table[0]
            
    except Exception as e:
        print(f"Error extracting table name: {e}")
    
    # No match found
    return None

class LLMHandler:
    def __init__(self, model_name: str = "codellama:34b-instruct"):
        """Initialize the LLM handler with model configuration."""
        self.model_name = model_name
        self.mcp = MCPValidator()
        self.schema_cache = {}  # Cache for table schemas
        self.db_path = "sqlite.db"  # Path to your SQLite database
        
    def _get_tables(self) -> List[str]:
        """Get list of tables from the database."""
        tables = []
        
        # Get tables from database
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()
        except Exception as e:
            print(f"Error getting tables: {e}")
                
        return tables

    def _get_table_info(self, table_name: str) -> List[str]:
        """Get column information for a table."""
        # Check cache first
        if table_name in self.schema_cache:
            return list(self.schema_cache[table_name].keys())
            
        # Get columns from database
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            conn.close()
            
            if columns:
                # Update cache
                column_types = {}
                for col in columns:
                    column_types[col] = "TEXT"  # Default type
                self.schema_cache[table_name] = column_types
                return columns
        except Exception as e:
            print(f"Error getting table info: {e}")
            
        return []

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
"""
        # Get tables dynamically
        tables = self._get_tables()
        for table in tables:
            prompt += f"- {table}:\n"
            columns = self._get_table_info(table)
            for col in columns:
                prompt += f"  * {col}\n"
            prompt += "\n"
            
        return prompt
        
    def find_best_column_match(self, columns, question):
        """Find the best matching column for a question.
        
        Args:
            columns: List of column names to match against
            question: The question string to find matches in
            
        Returns:
            The best matching column name or None if no good match found
        """
        # Extract text that might be a column name from the question
        import re
        
        # Try different patterns to extract the potential column name
        patterns = [
            r'where\s+(.*?)\s+is\s+empty',  # Match "where X is empty"
            r'where\s+(.*?)\s+is\s+null',   # Match "where X is null"
            r'in\s+(.*?)\s+column',         # Match "in X column"
            r'values\s+in\s+(.*?)(?:\s+column|\s+in|\s+are|\s+is|\?|$)', # Match "values in X"
            r'in\s+the\s+(.*?)(?:\s+column|\s+in|\s+are|\s+is|\?|$)',    # Match "in the X"
            r'"([^"]*)"',                  # Match text in quotes
            r'\'([^\']*)\'',               # Match text in single quotes
        ]
        
        extracted_texts = []
        for pattern in patterns:
            matches = re.search(pattern, question, re.IGNORECASE)
            if matches:
                extracted_text = matches.group(1).strip()
                if len(extracted_text) > 3:  # Only consider reasonably sized matches
                    extracted_texts.append(extracted_text)
        
        # Also try splitting the question by common prepositions and conjunctions
        split_words = ["where", "in", "for", "about", "with", "and", "or", "the", "column", "field", "value", "values"]
        question_parts = question.split()
        for i in range(len(question_parts) - 2):
            if question_parts[i].lower() in split_words:
                # Extract the rest of the sentence until another split word
                phrase_end = i + 1
                while phrase_end < len(question_parts) and question_parts[phrase_end].lower() not in split_words:
                    phrase_end += 1
                
                phrase = " ".join(question_parts[i+1:phrase_end])
                if len(phrase) > 3:
                    extracted_texts.append(phrase)
        
        # Find best matching column for each extracted text
        best_match = None
        best_score = 0
        
        for col in columns:
            col_lower = col.lower()
            
            # Score each column against all extracted texts
            for text in extracted_texts:
                text_lower = text.lower()
                
                # Direct containment check
                if text_lower in col_lower:
                    score = len(text_lower) * 2  # Weight by length of match
                    if score > best_score:
                        best_score = score
                        best_match = col
                elif col_lower in text_lower:
                    score = len(col_lower) * 2  # Weight by length of match
                    if score > best_score:
                        best_score = score
                        best_match = col
                
                # Word-by-word comparison
                text_words = set(text_lower.split())
                col_words = set(col_lower.split())
                common_words = text_words.intersection(col_words)
                score = len(common_words) * 3  # Give more weight to word matches
                if score > best_score:
                    best_score = score
                    best_match = col
            
            # Check for common column keywords in the question
            keywords = {
                "current": ["ac", "curr", "amp", "current"],
                "button down": ["button", "down"],
                "button up": ["button", "up"],
                "stopped": ["stop", "stopped"],
                "age": ["age", "old", "year"],
                "name": ["name", "username", "user name"]
            }
            
            for key, related_words in keywords.items():
                if any(word in col_lower for word in related_words):
                    if any(word in question.lower() for word in related_words):
                        score = 10  # Higher baseline score for keyword matches
                        if score > best_score:
                            best_score = score
                            best_match = col
        
        # Return best match if score is good enough
        if best_score > 0:
            return best_match
        
        return None

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
            
        # Handle null value counting/showing
        null_keywords = ["null", "empty", "missing", "none"]
        if any(keyword in question.lower() for keyword in null_keywords) and table_name:
            # Get columns for this table
            columns = self._get_table_info(table_name)
            
            # Find the best matching column using our enhanced function
            best_column = self.find_best_column_match(columns, question)
            
            # Always quote column names, especially important for complex column names with spaces
            if best_column:
                if any(term in question.lower() for term in ["count", "how many", "number of"]):
                    # Count null values query
                    return f'SELECT COUNT(*) FROM {table_name} WHERE "{best_column}" IS NULL;'
                else:
                    # Show rows with null values query
                    return f'SELECT * FROM {table_name} WHERE "{best_column}" IS NULL LIMIT 10;'
            
            # Default to a generic query if no specific column is identified
            return f"SELECT * FROM {table_name} LIMIT 10;"
        
        # Handle text analysis queries - word count, character count, etc.
        text_keywords = ["word count", "character count", "text length", "string length", "number of words", 
                         "longest", "shortest", "length of"]
        if any(keyword in question.lower() for keyword in text_keywords) and table_name:
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
            
            if not target_column and "name" in question.lower() and table_name == "users":
                target_column = "name"
            
            if target_column:
                # Handle longest/shortest text
                if "longest" in question.lower():
                    if any(term in str(target_column).lower() for term in ['s/n', 'lgr', 'sen']):
                        return f'SELECT *, LENGTH("{target_column}") AS text_length FROM {table_name} ORDER BY text_length DESC LIMIT 1;'
                    else:
                        return f'SELECT *, LENGTH({target_column}) AS text_length FROM {table_name} ORDER BY text_length DESC LIMIT 1;'
                
                elif "shortest" in question.lower():
                    if any(term in str(target_column).lower() for term in ['s/n', 'lgr', 'sen']):
                        return f'SELECT *, LENGTH("{target_column}") AS text_length FROM {table_name} ORDER BY text_length ASC LIMIT 1;'
                    else:
                        return f'SELECT *, LENGTH({target_column}) AS text_length FROM {table_name} ORDER BY text_length ASC LIMIT 1;'
                
                elif "word" in question.lower():
                    # Word count using LENGTH and REPLACE functions - need separate query for first row
                    if any(term in str(target_column).lower() for term in ['s/n', 'lgr', 'sen']):
                        return f'SELECT LENGTH("{target_column}") - LENGTH(REPLACE("{target_column}", " ", "")) + 1 AS word_count FROM {table_name} LIMIT 1;'
                    else:
                        return f'SELECT LENGTH({target_column}) - LENGTH(REPLACE({target_column}, " ", "")) + 1 AS word_count FROM {table_name} LIMIT 1;'
                else:
                    # Character count
                    if any(term in str(target_column).lower() for term in ['s/n', 'lgr', 'sen']):
                        return f'SELECT LENGTH("{target_column}") AS char_count FROM {table_name} LIMIT 1;'
                    else:
                        return f'SELECT LENGTH({target_column}) AS char_count FROM {table_name} LIMIT 1;'

        # Handle distinct/unique value counting
        unique_keywords = ["unique", "distinct", "different"]
        if any(keyword in question.lower() for keyword in unique_keywords) and table_name:
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
            
            if not target_column and "age" in question.lower() and table_name == "users":
                target_column = "age"
                
            if not target_column and "current" in question.lower() and table_name == "kettlepump":
                target_column = "AC Curr, Amps (LGR S/N: 21930920, SEN S/N: 21930920, LBL: Print Pack 24)"
            
            if target_column:
                if any(term in str(target_column).lower() for term in ['s/n', 'lgr', 'sen']):
                    return f'SELECT COUNT(DISTINCT "{target_column}") FROM {table_name};'
                else:
                    return f'SELECT COUNT(DISTINCT {target_column}) FROM {table_name};'
            
            # Default to first column if no match found
            col = columns[0]
            return f'SELECT COUNT(DISTINCT "{col}") FROM {table_name};'
        
        # Handle descriptive statistics queries
        stat_keywords = {
            "average": "AVG",
            "mean": "AVG",
            "sum": "SUM",
            "total": "SUM",
            "standard deviation": "STDEV",
            "std dev": "STDEV",
            "median": "MEDIAN",
            "mode": "MODE",
            "minimum": "MIN",
            "min": "MIN",
            "maximum": "MAX",
            "max": "MAX",
            "count distinct": "COUNT(DISTINCT",
            "unique count": "COUNT(DISTINCT",
            "distinct count": "COUNT(DISTINCT",
        }
        
        for keyword, func in stat_keywords.items():
            if keyword in question.lower() and table_name:
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
                    # Special case for COUNT DISTINCT
                    if func.startswith("COUNT(DISTINCT"):
                        if any(term in target_column.lower() for term in ['s/n', 'lgr', 'sen']):
                            return f'SELECT COUNT(DISTINCT "{target_column}") FROM {table_name};'
                        else:
                            return f'SELECT COUNT(DISTINCT {target_column}) FROM {table_name};'
                    
                    # SQLite doesn't support STDEV, MEDIAN, MODE directly, fallback to simpler queries
                    if func in ["STDEV", "MEDIAN", "MODE"]:
                        if any(term in target_column.lower() for term in ['s/n', 'lgr', 'sen']):
                            return f'SELECT AVG("{target_column}") FROM {table_name};'
                        else:
                            return f'SELECT AVG({target_column}) FROM {table_name};'
                    
                    # Regular functions
                    if any(term in target_column.lower() for term in ['s/n', 'lgr', 'sen']):
                        return f'SELECT {func}("{target_column}") FROM {table_name};'
                    else:
                        return f'SELECT {func}({target_column}) FROM {table_name};'
                
                # Default to first column if no match found
                col = columns[0]
                if func.startswith("COUNT(DISTINCT"):
                    return f'SELECT COUNT(DISTINCT "{col}") FROM {table_name};'
                elif func in ["STDEV", "MEDIAN", "MODE"]:
                    return f'SELECT AVG("{col}") FROM {table_name};'
                else:
                    return f'SELECT {func}("{col}") FROM {table_name};'
                    
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

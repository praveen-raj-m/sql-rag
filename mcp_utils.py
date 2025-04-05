import json
from typing import Dict, List, Optional
import re
import sqlite3
import os
from datetime import datetime

class MCPValidator:
    def __init__(self, schema_file: str = "metadata/schema.json", db_path: str = "rag.db"):
        self.schema_file = schema_file
        self.db_path = db_path
        self.schema = self._load_schema()
        self._validate_schema_with_db()
        self.last_schema_update = self._get_schema_mtime()
        
    def _get_schema_mtime(self) -> float:
        """Get the last modification time of the schema file"""
        try:
            return os.path.getmtime(self.schema_file)
        except:
            return 0
            
    def _check_schema_update(self) -> bool:
        """Check if schema file has been updated"""
        current_mtime = self._get_schema_mtime()
        if current_mtime > self.last_schema_update:
            self.last_schema_update = current_mtime
            self.schema = self._load_schema()
            return True
        return False
            
    def _load_schema(self) -> Dict:
        """Load schema from JSON file"""
        try:
            with open(self.schema_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
            
    def _validate_schema_with_db(self) -> None:
        """Validate schema against actual database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all tables from database
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            db_tables = [row[0] for row in cursor.fetchall()]
            
            # Remove tables from schema that don't exist in DB
            schema_changed = False
            for table in list(self.schema.keys()):
                if table not in db_tables:
                    del self.schema[table]
                    schema_changed = True
                    
            # Update schema file if changed
            if schema_changed:
                self.update_schema_file()
                
        except Exception as e:
            print(f"Warning: Could not validate schema with database: {str(e)}")
        finally:
            conn.close()
            
    def update_schema_file(self) -> None:
        """Update the schema file with current schema"""
        os.makedirs(os.path.dirname(self.schema_file), exist_ok=True)
        with open(self.schema_file, 'w') as f:
            json.dump(self.schema, f, indent=2)
            
    def validate_sql_query(self, query: str) -> bool:
        """Validate SQL query against schema and basic syntax rules"""
        # Check for schema updates
        self._check_schema_update()
        
        # Convert to lowercase for case-insensitive checks
        query_lower = query.lower()
        
        # Basic SQL injection prevention
        dangerous_keywords = ['drop', 'delete', 'alter', 'update', 'insert', 'create', 'truncate']
        if any(keyword in query_lower for keyword in dangerous_keywords):
            return False
            
        # Must be a SELECT query
        if not query_lower.strip().startswith('select'):
            return False
            
        # Extract table names from query
        table_pattern = r'from\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        tables = re.findall(table_pattern, query_lower)
        
        # Check if tables exist in schema
        for table in tables:
            if table not in self.schema:
                return False
                
        # Allow aggregate functions
        allowed_aggregates = ['count', 'sum', 'avg', 'min', 'max']
        has_aggregate = any(agg in query_lower for agg in allowed_aggregates)
        
        # Extract column names from query
        column_pattern = r'select\s+(.*?)\s+from'
        columns_match = re.search(column_pattern, query_lower)
        if columns_match:
            columns = [col.strip() for col in columns_match.group(1).split(',')]
            for col in columns:
                # Skip validation for aggregate functions
                if has_aggregate and any(agg in col.lower() for agg in allowed_aggregates):
                    continue
                # Skip validation for *
                if col == '*':
                    continue
                # Validate column exists in any of the referenced tables
                if not any(
                    col in self.schema[table].keys() 
                    for table in tables 
                    if table in self.schema
                ):
                    return False
                    
        return True
        
    def validate_response(self, response: str) -> bool:
        """Validate LLM response format and content"""
        # Check for schema updates
        self._check_schema_update()
        
        # Check if response contains SQL query
        if not any(marker in response.lower() for marker in ['select', 'from']):
            return False
            
        # Check for common hallucination patterns
        hallucination_patterns = [
            r'i don\'t know',
            r'i cannot',
            r'not sure',
            r'no information',
            r'no data',
            r'error',
            r'undefined',
            r'null',
            r'empty'
        ]
        
        for pattern in hallucination_patterns:
            if re.search(pattern, response.lower()):
                return False
                
        return True
        
    def format_response(self, sql_query: str, result: str) -> str:
        """Format the response in a natural language format"""
        return f"Based on your query, here's what I found:\n\n{result}\n\nGenerated SQL: {sql_query}"
        
    def update_schema(self, table_name: str, columns: Dict[str, str]) -> None:
        """Update schema with new table information"""
        self.schema[table_name] = columns
        self.update_schema_file()
            
    def get_table_info(self, table_name: str) -> Optional[Dict]:
        """Get information about a specific table"""
        # Check for schema updates
        self._check_schema_update()
        return self.schema.get(table_name)
        
    def list_tables(self) -> List[str]:
        """List all tables in the schema"""
        # Check for schema updates
        self._check_schema_update()
        return list(self.schema.keys())
        
    def remove_table(self, table_name: str) -> None:
        """Remove a table from the schema"""
        if table_name in self.schema:
            del self.schema[table_name]
            self.update_schema_file() 
import unittest
from mcp_utils import MCPValidator
from db_utils import refresh_schema, create_table, run_query
from llm_utils import LLMHandler
import sqlite3
import os
import json
import time

class TestMCP(unittest.TestCase):
    def setUp(self):
        """Set up test environment"""
        self.db_path = "test_rag.db"
        self.schema_file = "metadata/test_schema.json"
        
        # Create metadata directory if it doesn't exist
        os.makedirs("metadata", exist_ok=True)
        
        # Create test database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                age INTEGER
            )
        """)
        cursor.execute("""
            INSERT INTO users (name, age) VALUES
            ('John Doe', 30),
            ('Jane Smith', 25),
            ('Bob Johnson', 35)
        """)
        conn.commit()
        conn.close()
        
        # Initialize MCP and refresh schema
        self.mcp = MCPValidator(self.schema_file, self.db_path)
        refresh_schema(self.db_path, self.schema_file)
        
        # Wait for schema file to be created
        time.sleep(0.1)
        
    def tearDown(self):
        """Clean up test environment"""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        if os.path.exists(self.schema_file):
            os.remove(self.schema_file)
            
    def test_table_deletion(self):
        """Test table deletion and schema update"""
        # First verify table exists
        self.assertIn('users', self.mcp.schema)
        
        # Delete table
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE users")
        conn.commit()
        conn.close()
        
        # Refresh schema
        refresh_schema(self.db_path, self.schema_file)
        
        # Force schema reload
        self.mcp._check_schema_update()
        
        # Verify table is removed from schema
        self.assertNotIn('users', self.mcp.schema)
        
        # Verify schema file is updated
        with open(self.schema_file, 'r') as f:
            schema_data = json.load(f)
            self.assertNotIn('users', schema_data)
            
    def test_schema_loading(self):
        """Test if schema is loaded correctly"""
        # Force schema reload
        self.mcp._check_schema_update()
        schema = self.mcp.schema
        
        self.assertIn('users', schema, "Users table should be in schema")
        self.assertIn('id', schema['users'], "id column should be in users table")
        self.assertIn('name', schema['users'], "name column should be in users table")
        self.assertIn('age', schema['users'], "age column should be in users table")
        
    def test_valid_queries(self):
        """Test valid SQL queries"""
        valid_queries = [
            "SELECT * FROM users",
            "SELECT name FROM users WHERE age > 30",
            "SELECT COUNT(*) FROM users",
            "SELECT AVG(age) FROM users",
            "SELECT name, age FROM users ORDER BY age"
        ]
        
        for query in valid_queries:
            self.assertTrue(self.mcp.validate_sql_query(query), f"Query should be valid: {query}")
            
    def test_invalid_queries(self):
        """Test invalid SQL queries"""
        invalid_queries = [
            "DROP TABLE users",
            "DELETE FROM users",
            "UPDATE users SET age = 30",
            "SELECT * FROM non_existent_table",
            "SELECT invalid_column FROM users"
        ]
        
        for query in invalid_queries:
            self.assertFalse(self.mcp.validate_sql_query(query), f"Query should be invalid: {query}")
            
    def test_schema_update(self):
        """Test schema update functionality"""
        # Create new table
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                price REAL
            )
        """)
        conn.commit()
        conn.close()
        
        # Refresh schema
        refresh_schema(self.db_path, self.schema_file)
        
        # Check if new table is in schema
        self.mcp._check_schema_update()
        self.assertIn('products', self.mcp.schema)
        
    def test_response_validation(self):
        """Test response validation"""
        valid_responses = [
            "SELECT * FROM users",
            "Here are the results: SELECT name FROM users"
        ]
        
        invalid_responses = [
            "I don't know",
            "Error: Table not found",
            "No data available",
            "undefined"
        ]
        
        for response in valid_responses:
            self.assertTrue(self.mcp.validate_response(response), f"Response should be valid: {response}")
            
        for response in invalid_responses:
            self.assertFalse(self.mcp.validate_response(response), f"Response should be invalid: {response}")
            
    def test_schema_file_update(self):
        """Test schema file update"""
        # Modify schema file
        with open(self.schema_file, 'w') as f:
            json.dump({"test_table": {"id": "INTEGER"}}, f)
            
        # Check if schema is updated
        self.mcp._check_schema_update()
        self.assertIn('test_table', self.mcp.schema)

class TestLLMHandler(unittest.TestCase):
    def setUp(self):
        """Set up test environment"""
        self.db_path = "test_rag.db"
        self.schema_file = "metadata/test_schema.json"
        
        # Create metadata directory if it doesn't exist
        os.makedirs("metadata", exist_ok=True)
        
        # Create test database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                age INTEGER
            )
        """)
        cursor.execute("""
            INSERT INTO users (name, age) VALUES
            ('John Doe', 30),
            ('Jane Smith', 25),
            ('Bob Johnson', 35)
        """)
        conn.commit()
        conn.close()
        
        # Initialize LLM handler
        self.llm = LLMHandler()
        
    def tearDown(self):
        """Clean up test environment"""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        if os.path.exists(self.schema_file):
            os.remove(self.schema_file)
            
    def test_nl_to_sql(self):
        """Test natural language to SQL conversion"""
        test_cases = [
            ("Show me all users", "SELECT * FROM users"),
            ("Find users older than 30", "SELECT * FROM users WHERE age > 30"),
            ("Count all users", "SELECT COUNT(*) FROM users"),
            ("Get average age", "SELECT AVG(age) FROM users")
        ]
        
        for question, expected_sql in test_cases:
            sql = self.llm.nl_to_sql(question)
            self.assertTrue(sql.startswith("SELECT"), f"Query should start with SELECT: {sql}")
            self.assertIn("FROM", sql, f"Query should contain FROM: {sql}")
            
    def test_format_response(self):
        """Test response formatting"""
        sql = "SELECT * FROM users"
        result = "John Doe, 30\nJane Smith, 25"
        
        formatted = self.llm.format_response(sql, result)
        self.assertIn(sql, formatted)
        self.assertIn(result, formatted)
        
if __name__ == '__main__':
    unittest.main() 
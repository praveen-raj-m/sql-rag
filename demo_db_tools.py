#!/usr/bin/env python3
"""
Demo script for SQL RAG Dashboard database management tools.
This script demonstrates the usage of the various database management functions.
"""

import os
import time
import sqlite3
import pandas as pd
import json

# Import functions from our tools
from db_manager import list_tables, delete_table, create_users_table, refresh_schema

# Configuration
DB_PATH = "sqlite.db"
METADATA_DIR = "metadata"
SCHEMA_PATH = "schema.json"

def ensure_dirs_exist():
    """Ensure necessary directories exist"""
    os.makedirs(METADATA_DIR, exist_ok=True)

def print_header(text):
    """Print a formatted header"""
    print("\n" + "="*60)
    print(f" {text} ".center(60, "="))
    print("="*60 + "\n")

def print_section(text):
    """Print a section header"""
    print("\n" + "-"*60)
    print(f" {text} ".center(60, "-"))
    print("-"*60)

def display_table_contents(table_name):
    """Display the contents of a table"""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 10", conn)
        conn.close()
        
        print(f"\nContents of table '{table_name}' (first 10 rows):")
        print(df.to_string(index=False))
        
        # Get row count
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        conn.close()
        
        print(f"\nTotal rows: {count}")
        
    except Exception as e:
        print(f"Error displaying table: {e}")

def create_survey_table():
    """Create a sample survey table"""
    ensure_dirs_exist()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if table already exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='survey'")
    if cursor.fetchone():
        print("The survey table already exists. No changes made.")
        conn.close()
        return False
    
    # Create survey table
    try:
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS survey (
            id INTEGER PRIMARY KEY,
            question TEXT,
            response TEXT,
            rating INTEGER,
            submitted_date TEXT
        )
        ''')
        
        # Insert sample data
        sample_data = [
            (1, 'How satisfied are you?', 'Very satisfied', 5, '2023-05-01'),
            (2, 'Would you recommend us?', 'Yes definitely', 5, '2023-05-02'),
            (3, 'Areas for improvement?', 'Better UI', 4, '2023-05-03'),
            (4, 'How easy was to use?', 'Somewhat difficult', 3, '2023-05-04'),
            (5, 'Will you use again?', 'Maybe', 3, '2023-05-05')
        ]
        
        cursor.executemany('INSERT INTO survey VALUES (?, ?, ?, ?, ?)', sample_data)
        conn.commit()
        
        print("Successfully created survey table with sample data")
        
        # Create metadata
        metadata = {
            "name": "survey",
            "description": "Customer satisfaction survey responses",
            "columns": [
                {"name": "id", "type": "INTEGER", "description": "Unique survey response ID"},
                {"name": "question", "type": "TEXT", "description": "Survey question text"},
                {"name": "response", "type": "TEXT", "description": "Customer's response"},
                {"name": "rating", "type": "INTEGER", "description": "Numerical rating (1-5)"},
                {"name": "submitted_date", "type": "TEXT", "description": "Date response was submitted"}
            ]
        }
        
        metadata_path = os.path.join(METADATA_DIR, "survey_metadata.json")
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        print(f"Created metadata file at {metadata_path}")
        
        return True
    
    except sqlite3.Error as e:
        print(f"Error creating survey table: {e}")
        conn.close()
        return False
    finally:
        conn.close()

def display_schema():
    """Display the contents of the schema.json file"""
    try:
        if os.path.exists(SCHEMA_PATH):
            with open(SCHEMA_PATH, 'r') as f:
                schema = json.load(f)
            
            print("\nSchema.json contents:")
            print(json.dumps(schema, indent=2)[:500] + "...\n(truncated)")
        else:
            print("Schema.json file not found")
    except Exception as e:
        print(f"Error reading schema: {e}")

def run_demo():
    """Run the demo of database management tools"""
    print_header("SQL RAG DASHBOARD - DATABASE MANAGEMENT TOOLS DEMO")
    
    # Step 1: List tables
    print_section("1. Listing Tables")
    print("Checking for existing tables...")
    tables = list_tables()
    
    # Step 2: Create Users Table
    print_section("2. Creating Users Table")
    create_users_table()
    
    # Step 3: Create Survey Table
    print_section("3. Creating Survey Table")
    create_survey_table()
    
    # Step 4: List tables again
    print_section("4. Updated Table List")
    tables = list_tables()
    
    # Step 5: Display table contents
    print_section("5. Displaying Table Contents")
    for table in tables:
        display_table_contents(table)
    
    # Step 6: Refresh schema
    print_section("6. Refreshing Schema")
    refresh_schema()
    
    # Step 7: Display schema
    print_section("7. Schema Contents")
    display_schema()
    
    # Step 8: Delete one table
    print_section("8. Deleting a Table")
    
    if len(tables) > 0:
        table_to_delete = tables[0]
        print(f"Deleting table: {table_to_delete}")
        delete_table(table_to_delete)
    else:
        print("No tables to delete")
    
    # Step 9: Final table list
    print_section("9. Final Table List")
    list_tables()
    
    print_header("DEMO COMPLETED")
    print("For more information on database management tools, see README_DB_TOOLS.md")
    print("Available tools:")
    print("- db_manager.py: Command line database management")
    print("- delete_table.py: Simple table deletion script")
    print("- table_manager_app.py: Graphical table management interface")
    print("- manage_db.sh: Interactive menu for database operations")

if __name__ == "__main__":
    run_demo() 
#!/usr/bin/env python3
"""
Comprehensive Database Management Script for SQL RAG Dashboard

This script provides a command-line interface for managing SQLite database operations:
- List all tables
- Delete a specific table (including metadata)
- Create sample users table
- Refresh the schema.json file

Usage:
  python db_manager.py list
  python db_manager.py delete <table_name>
  python db_manager.py create-users
  python db_manager.py refresh-schema
  python db_manager.py help
"""

import sqlite3
import sys
import os
import json
import argparse
import pandas as pd
from pathlib import Path

# Configuration
DB_PATH = "sqlite.db"
METADATA_DIR = "metadata"
SCHEMA_PATH = "schema.json"

def ensure_dirs_exist():
    """Ensure necessary directories exist"""
    os.makedirs(METADATA_DIR, exist_ok=True)

def list_tables():
    """List all tables in the database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    if not tables:
        print("No tables found in the database")
    else:
        print("Tables in the database:")
        for i, table in enumerate(tables, 1):
            print(f"{i}. {table}")
            
    return tables

def delete_table(table_name):
    """Delete a table and its associated metadata"""
    ensure_dirs_exist()
    
    # Validate table exists
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if the table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    if not cursor.fetchone():
        print(f"Error: Table '{table_name}' does not exist in the database")
        conn.close()
        return False
    
    # Delete the table
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        print(f"Table '{table_name}' successfully deleted from database")
    except sqlite3.Error as e:
        print(f"Database error when deleting table: {e}")
        conn.close()
        return False
    finally:
        conn.close()
    
    # Delete associated metadata files
    table_metadata_path = os.path.join(METADATA_DIR, f"{table_name}_metadata.json")
    if os.path.exists(table_metadata_path):
        try:
            os.remove(table_metadata_path)
            print(f"Metadata file '{table_metadata_path}' deleted")
        except OSError as e:
            print(f"Error deleting metadata file: {e}")
    
    # Check for alternative metadata filename format
    alt_metadata_path = os.path.join(METADATA_DIR, f"{table_name}.json")
    if os.path.exists(alt_metadata_path):
        try:
            os.remove(alt_metadata_path)
            print(f"Metadata file '{alt_metadata_path}' deleted")
        except OSError as e:
            print(f"Error deleting metadata file: {e}")
    
    # Update schema.json to remove the table
    try:
        if os.path.exists(SCHEMA_PATH):
            with open(SCHEMA_PATH, 'r') as f:
                schema = json.load(f)
            
            if table_name in schema:
                del schema[table_name]
                
                with open(SCHEMA_PATH, 'w') as f:
                    json.dump(schema, f, indent=2)
                print(f"Updated schema.json to remove '{table_name}'")
        else:
            print("Warning: schema.json file not found, cannot update schema")
    except Exception as e:
        print(f"Error updating schema.json: {e}")
    
    return True

def create_users_table():
    """Create a sample users table if it doesn't exist"""
    ensure_dirs_exist()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if table already exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
    if cursor.fetchone():
        print("The users table already exists. No changes made.")
        conn.close()
        return False
    
    # Create users table
    try:
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER,
            email TEXT
        )
        ''')
        
        # Insert sample data
        sample_data = [
            (1, 'Alice', 28, 'alice@example.com'),
            (2, 'Bob', 34, 'bob@example.com'),
            (3, 'Charlie', 22, 'charlie@example.com')
        ]
        
        cursor.executemany('INSERT INTO users VALUES (?, ?, ?, ?)', sample_data)
        conn.commit()
        
        print("Successfully created users table with sample data")
        
        # Create metadata
        metadata = {
            "name": "users",
            "description": "Sample user data for testing",
            "columns": [
                {"name": "id", "type": "INTEGER", "description": "Unique user identifier"},
                {"name": "name", "type": "TEXT", "description": "User's full name"},
                {"name": "age", "type": "INTEGER", "description": "User's age in years"},
                {"name": "email", "type": "TEXT", "description": "User's email address"}
            ]
        }
        
        metadata_path = os.path.join(METADATA_DIR, "users_metadata.json")
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        print(f"Created metadata file at {metadata_path}")
        
        # Update schema
        refresh_schema()
        
        return True
    
    except sqlite3.Error as e:
        print(f"Error creating users table: {e}")
        conn.close()
        return False
    finally:
        conn.close()

def refresh_schema():
    """Update schema.json based on current database tables"""
    ensure_dirs_exist()
    
    conn = sqlite3.connect(DB_PATH)
    
    # Get list of tables
    tables = list_tables()
    
    # Initialize schema dictionary
    schema = {}
    
    # Process each table
    for table in tables:
        # Get column info
        df = pd.read_sql(f"PRAGMA table_info({table})", conn)
        
        columns = []
        for _, row in df.iterrows():
            column = {
                "name": row["name"],
                "type": row["type"]
            }
            columns.append(column)
        
        # Get first 5 rows as sample
        try:
            df_sample = pd.read_sql(f"SELECT * FROM {table} LIMIT 5", conn)
            sample_data = df_sample.to_dict(orient="records")
        except:
            sample_data = []
        
        # Add table to schema
        schema[table] = {
            "table_name": table,
            "columns": columns,
            "sample_data": sample_data
        }
        
        # Check for metadata file
        metadata_path = os.path.join(METADATA_DIR, f"{table}_metadata.json")
        alt_metadata_path = os.path.join(METADATA_DIR, f"{table}.json")
        
        if os.path.exists(metadata_path):
            try:
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                # Add any column descriptions from metadata
                if "columns" in metadata:
                    for meta_col in metadata["columns"]:
                        for col in schema[table]["columns"]:
                            if col["name"] == meta_col["name"] and "description" in meta_col:
                                col["description"] = meta_col["description"]
            except Exception as e:
                print(f"Warning: Error reading metadata file {metadata_path}: {e}")
        elif os.path.exists(alt_metadata_path):
            try:
                with open(alt_metadata_path, 'r') as f:
                    metadata = json.load(f)
                # Add any column descriptions from metadata
                if "columns" in metadata:
                    for meta_col in metadata["columns"]:
                        for col in schema[table]["columns"]:
                            if col["name"] == meta_col["name"] and "description" in meta_col:
                                col["description"] = meta_col["description"]
            except Exception as e:
                print(f"Warning: Error reading metadata file {alt_metadata_path}: {e}")
    
    conn.close()
    
    # Write schema to file
    with open(SCHEMA_PATH, 'w') as f:
        json.dump(schema, f, indent=2)
    
    print(f"Schema refreshed and saved to {SCHEMA_PATH}")
    print(f"Processed {len(schema)} tables")
    
    return True

def show_help():
    """Display help information"""
    print(f"""
    Database Management Script for SQL RAG Dashboard
    ------------------------------------------------
    
    Commands:
      list                  List all tables in the database
      delete <table_name>   Delete a specific table and its metadata
      create-users          Create a sample users table with test data
      refresh-schema        Rebuild schema.json from current database state
      help                  Show this help message
    
    Examples:
      python {sys.argv[0]} list
      python {sys.argv[0]} delete users
      python {sys.argv[0]} create-users
      python {sys.argv[0]} refresh-schema
    """)

def main():
    parser = argparse.ArgumentParser(description='SQL RAG Database Manager', add_help=False)
    parser.add_argument('command', nargs='?', default='help', 
                        help='Command to execute (list, delete, create-users, refresh-schema, help)')
    parser.add_argument('table', nargs='?', help='Table name (for delete command)')
    
    args = parser.parse_args()
    
    # Handle commands
    if args.command == 'list':
        list_tables()
    elif args.command == 'delete':
        if not args.table:
            print("Error: Missing table name to delete")
            print("Usage: python db_manager.py delete <table_name>")
            return
        delete_table(args.table)
    elif args.command == 'create-users':
        create_users_table()
    elif args.command == 'refresh-schema':
        refresh_schema()
    elif args.command in ['help', '-h', '--help']:
        show_help()
    else:
        print(f"Unknown command: {args.command}")
        show_help()

if __name__ == "__main__":
    main() 
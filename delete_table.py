#!/usr/bin/env python3
"""
Simple script to delete a table from the SQLite database.
Usage: python delete_table.py <table_name>
"""

import sqlite3
import sys
import os
import json
import shutil

# Configuration
DB_PATH = "sqlite.db"
METADATA_DIR = "metadata"

def delete_table(table_name):
    """Delete a table and its associated metadata"""
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
    
    # Update schema.json to remove the table
    schema_path = "schema.json"
    try:
        if os.path.exists(schema_path):
            with open(schema_path, 'r') as f:
                schema = json.load(f)
            
            if table_name in schema:
                del schema[table_name]
                
                with open(schema_path, 'w') as f:
                    json.dump(schema, f, indent=2)
                print(f"Updated schema.json to remove '{table_name}'")
        else:
            print("Warning: schema.json file not found, cannot update schema")
    except Exception as e:
        print(f"Error updating schema.json: {e}")
    
    return True

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

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ['-h', '--help', 'help']:
        print("Usage: python delete_table.py <table_name>")
        print("       python delete_table.py list")
        print("\nOptions:")
        print("  <table_name>  Name of the table to delete")
        print("  list          List all tables in the database")
        return
    
    command = sys.argv[1]
    
    if command == "list":
        list_tables()
    else:
        table_name = command
        delete_table(table_name)

if __name__ == "__main__":
    main() 
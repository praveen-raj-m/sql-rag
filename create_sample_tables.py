#!/usr/bin/env python3
"""
Script to create 3 sample tables in the SQLite database
"""

import sqlite3
import os
import json
from pathlib import Path

# Configuration
DB_PATH = "sqlite.db"
METADATA_DIR = "metadata"
SCHEMA_PATH = "schema.json"

def ensure_dirs_exist():
    """Ensure necessary directories exist"""
    os.makedirs(METADATA_DIR, exist_ok=True)

def create_products_table():
    """Create a products table with sample data"""
    print("Creating 'products' table...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if table already exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='products'")
    if cursor.fetchone():
        print("The products table already exists. No changes made.")
        conn.close()
        return
    
    # Create products table
    cursor.execute('''
    CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT,
        category TEXT,
        price REAL,
        stock INTEGER,
        description TEXT
    )
    ''')
    
    # Insert sample data
    sample_data = [
        (1, 'Laptop', 'Electronics', 1299.99, 25, 'High-performance laptop with 16GB RAM'),
        (2, 'Coffee Maker', 'Kitchen', 89.99, 50, 'Programmable coffee maker with timer'),
        (3, 'Headphones', 'Electronics', 199.99, 100, 'Noise cancelling wireless headphones'),
        (4, 'Desk Chair', 'Furniture', 249.99, 15, 'Ergonomic office chair with lumbar support'),
        (5, 'Blender', 'Kitchen', 79.99, 30, 'High-speed blender for smoothies')
    ]
    
    cursor.executemany('INSERT INTO products VALUES (?, ?, ?, ?, ?, ?)', sample_data)
    conn.commit()
    conn.close()
    
    # Create metadata
    metadata = {
        "name": "products",
        "description": "Product inventory database",
        "columns": [
            {"name": "id", "type": "INTEGER", "description": "Unique product identifier"},
            {"name": "name", "type": "TEXT", "description": "Product name"},
            {"name": "category", "type": "TEXT", "description": "Product category"},
            {"name": "price", "type": "REAL", "description": "Product price in USD"},
            {"name": "stock", "type": "INTEGER", "description": "Current inventory quantity"},
            {"name": "description", "type": "TEXT", "description": "Product description"}
        ]
    }
    
    metadata_path = os.path.join(METADATA_DIR, "products_metadata.json")
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Created products table with 5 sample products")
    print(f"Created metadata file at {metadata_path}")

def create_employees_table():
    """Create an employees table with sample data"""
    print("Creating 'employees' table...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if table already exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='employees'")
    if cursor.fetchone():
        print("The employees table already exists. No changes made.")
        conn.close()
        return
    
    # Create employees table
    cursor.execute('''
    CREATE TABLE employees (
        id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        position TEXT,
        department TEXT,
        salary REAL,
        hire_date TEXT
    )
    ''')
    
    # Insert sample data
    sample_data = [
        (1, 'John', 'Smith', 'Manager', 'Sales', 75000.00, '2018-05-15'),
        (2, 'Emily', 'Johnson', 'Developer', 'Engineering', 85000.00, '2019-11-20'),
        (3, 'Michael', 'Williams', 'Designer', 'Marketing', 65000.00, '2020-02-10'),
        (4, 'Jessica', 'Brown', 'Analyst', 'Finance', 70000.00, '2021-07-05'),
        (5, 'David', 'Miller', 'HR Specialist', 'Human Resources', 60000.00, '2019-08-18'),
        (6, 'Sarah', 'Davis', 'Developer', 'Engineering', 90000.00, '2017-04-22')
    ]
    
    cursor.executemany('INSERT INTO employees VALUES (?, ?, ?, ?, ?, ?, ?)', sample_data)
    conn.commit()
    conn.close()
    
    # Create metadata
    metadata = {
        "name": "employees",
        "description": "Employee records database",
        "columns": [
            {"name": "id", "type": "INTEGER", "description": "Employee ID number"},
            {"name": "first_name", "type": "TEXT", "description": "Employee's first name"},
            {"name": "last_name", "type": "TEXT", "description": "Employee's last name"},
            {"name": "position", "type": "TEXT", "description": "Job title"},
            {"name": "department", "type": "TEXT", "description": "Department name"},
            {"name": "salary", "type": "REAL", "description": "Annual salary in USD"},
            {"name": "hire_date", "type": "TEXT", "description": "Date of hire (YYYY-MM-DD)"}
        ]
    }
    
    metadata_path = os.path.join(METADATA_DIR, "employees_metadata.json")
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Created employees table with 6 sample employees")
    print(f"Created metadata file at {metadata_path}")

def create_orders_table():
    """Create an orders table with sample data"""
    print("Creating 'orders' table...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if table already exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'")
    if cursor.fetchone():
        print("The orders table already exists. No changes made.")
        conn.close()
        return
    
    # Create orders table
    cursor.execute('''
    CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        customer_name TEXT,
        order_date TEXT,
        total_amount REAL,
        payment_method TEXT,
        status TEXT,
        shipping_address TEXT
    )
    ''')
    
    # Insert sample data
    sample_data = [
        (1, 'Robert Jones', '2023-01-15', 1389.98, 'Credit Card', 'Shipped', '123 Main St, Boston, MA'),
        (2, 'Amanda Garcia', '2023-01-20', 89.99, 'PayPal', 'Delivered', '456 Oak Ave, Chicago, IL'),
        (3, 'Thomas Wilson', '2023-02-05', 199.99, 'Credit Card', 'Processing', '789 Pine Rd, Seattle, WA'),
        (4, 'Lisa Martinez', '2023-02-12', 329.98, 'Debit Card', 'Shipped', '321 Elm Blvd, Miami, FL'),
        (5, 'Kevin Taylor', '2023-02-18', 79.99, 'PayPal', 'Pending', '654 Maple Dr, Denver, CO'),
        (6, 'Nicole Anderson', '2023-03-02', 449.98, 'Credit Card', 'Delivered', '987 Cedar St, Austin, TX'),
        (7, 'Christopher Lee', '2023-03-10', None, 'Credit Card', 'Cancelled', '246 Birch Ln, Portland, OR')
    ]
    
    cursor.executemany('INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)', sample_data)
    conn.commit()
    conn.close()
    
    # Create metadata
    metadata = {
        "name": "orders",
        "description": "Customer order records",
        "columns": [
            {"name": "id", "type": "INTEGER", "description": "Order ID number"},
            {"name": "customer_name", "type": "TEXT", "description": "Name of customer"},
            {"name": "order_date", "type": "TEXT", "description": "Date order was placed (YYYY-MM-DD)"},
            {"name": "total_amount", "type": "REAL", "description": "Total order amount in USD"},
            {"name": "payment_method", "type": "TEXT", "description": "Method of payment"},
            {"name": "status", "type": "TEXT", "description": "Current order status"},
            {"name": "shipping_address", "type": "TEXT", "description": "Delivery address"}
        ]
    }
    
    metadata_path = os.path.join(METADATA_DIR, "orders_metadata.json")
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Created orders table with 7 sample orders")
    print(f"Created metadata file at {metadata_path}")

def update_schema():
    """Update schema.json with the new tables"""
    print("Updating schema.json file...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [row[0] for row in cursor.fetchall()]
    
    schema = {}
    
    # Process each table
    for table in tables:
        # Get column info
        cursor.execute(f"PRAGMA table_info({table})")
        columns = []
        
        for col in cursor.fetchall():
            column = {
                "name": col[1],  # Column name
                "type": col[2]   # Column type
            }
            columns.append(column)
        
        # Get sample data
        cursor.execute(f"SELECT * FROM {table} LIMIT 5")
        rows = cursor.fetchall()
        
        # Get column names for sample data
        cursor.execute(f"PRAGMA table_info({table})")
        col_names = [col[1] for col in cursor.fetchall()]
        
        # Convert to list of dicts
        sample_data = []
        for row in rows:
            sample_data.append(dict(zip(col_names, row)))
        
        # Add to schema
        schema[table] = {
            "table_name": table,
            "columns": columns,
            "sample_data": sample_data
        }
        
        # Try to add descriptions from metadata
        metadata_path = os.path.join(METADATA_DIR, f"{table}_metadata.json")
        if os.path.exists(metadata_path):
            try:
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                
                if "columns" in metadata:
                    for i, col in enumerate(schema[table]["columns"]):
                        for meta_col in metadata["columns"]:
                            if meta_col["name"] == col["name"] and "description" in meta_col:
                                schema[table]["columns"][i]["description"] = meta_col["description"]
            except Exception as e:
                print(f"Error reading metadata for {table}: {e}")
    
    # Write to schema.json
    with open(SCHEMA_PATH, 'w') as f:
        json.dump(schema, f, indent=2)
    
    conn.close()
    print(f"Updated schema.json with {len(tables)} tables")

def main():
    """Create all sample tables and update schema"""
    print("Creating sample tables...")
    ensure_dirs_exist()
    
    create_products_table()
    create_employees_table()
    create_orders_table()
    
    update_schema()
    
    print("All sample tables created successfully!")

if __name__ == "__main__":
    main() 
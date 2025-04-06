import pandas as pd
import sqlite3
from sqlalchemy import create_engine
from llm_utils import LLMHandler
from db_utils import get_db_connection, DB_PATH, refresh_schema
import traceback

def format_result(result, max_rows=5):
    """Format output for better readability."""
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_rows', max_rows)
    
    if isinstance(result, pd.DataFrame):
        if len(result) > max_rows:
            return result.head(max_rows).to_string()
        return result.to_string()
    return result

def test_schema_refresh():
    """Test refreshing the schema."""
    result = refresh_schema(DB_PATH)
    print(f"Schema refresh result: {result}")

def test_table_listing():
    """Test listing all tables in the database."""
    llm = LLMHandler()
    
    # Test getting tables using the LLM handler
    sql = llm.nl_to_sql("what are the tables in this database?")
    print(f"Generated SQL: {sql}")
    
    # Connect to the database directly
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in cursor.fetchall()]
        print("\nTables in database:")
        
        for table in tables:
            print(f"\n📊 Table: {table}")
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"   Rows: {count}")
                
                # Get sample data
                if count > 0:
                    cursor.execute(f"PRAGMA table_info({table})")
                    columns = [row[1] for row in cursor.fetchall()]
                    print(f"   Columns: {len(columns)}")
                    print(f"   Column names: {', '.join(columns)}")
                    
                    # Get sample row
                    cursor.execute(f"SELECT * FROM {table} LIMIT 1")
                    sample = cursor.fetchone()
                    if sample:
                        print(f"   Sample row: {sample}")
            except Exception as e:
                print(f"   Error: {str(e)}")
                
        conn.close()
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")

def test_complex_queries():
    """Test complex SQL queries."""
    print("\nTesting complex column queries...")
    llm = LLMHandler()
    
    # Test queries
    test_queries = [
        "how many users are there?",
        "what are all users in the system?",
        "show me the user with the highest age"
    ]
    
    for query in test_queries:
        print(f"\n🔍 Testing: {query}")
        sql = llm.nl_to_sql(query)
        print(f"   SQL: {sql}")
        
        try:
            conn = get_db_connection()
            df = pd.read_sql_query(sql, conn)
            print(f"   Result:\n   {format_result(df)}")
            conn.close()
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")

def test_sql_execution():
    """Test direct SQL execution."""
    print("\nTesting SQL execution...")
    
    test_sqls = [
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';",
        "SELECT COUNT(*) FROM users;",
        "SELECT * FROM users LIMIT 3;"
    ]
    
    engine = create_engine(f"sqlite:///{DB_PATH}")
    
    for sql in test_sqls:
        print(f"\n🔍 Executing: {sql}")
        try:
            result = pd.read_sql_query(sql, engine)
            print(f"Result:\n{format_result(result)}")
        except Exception as e:
            print(f"❌ Error: {str(e)}")

print("🚀 Starting functionality tests...\n")
test_schema_refresh()
test_table_listing()
test_complex_queries()
test_sql_execution()
print("\n✅ All tests completed.") 
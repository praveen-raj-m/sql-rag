import sqlite3
import pandas as pd
from llm_utils import LLMHandler
from db_utils import DB_PATH

def format_sql_result(sql: str, result: pd.DataFrame) -> str:
    """Format the SQL result based on query type."""
    # For COUNT(*) queries, extract and format the count
    if "COUNT(*)" in sql:
        if not result.empty:
            count = result.iloc[0, 0]
            # For null value counts
            if "IS NULL" in sql:
                col_name = sql.split("WHERE")[1].split("IS NULL")[0].strip().replace('"', '')
                table_name = sql.split("FROM")[1].split("WHERE")[0].strip()
                return f"There are {count:,} null values in the {col_name} column of the {table_name} table."
            # For column counts
            elif "FROM pragma_table_info" in sql:
                table_name = sql.split("'")[1] if "'" in sql else "the table"
                return f"The {table_name} table has {count} columns."
            # For regular row counts
            else:
                table_name = sql.split("FROM")[1].strip().rstrip(';') if "FROM" in sql else "the table"
                return f"There are {count:,} rows in {table_name}."
                
    # For MAX/MIN queries
    if "MAX(" in sql or "MIN(" in sql:
        if not result.empty:
            value = result.iloc[0, 0]
            operation = "maximum" if "MAX(" in sql else "minimum"
            col_name = sql.split("(")[1].split(")")[0].replace('"', '')
            return f"The {operation} value of {col_name} is {value}."
            
    # For AVG queries
    if "AVG(" in sql:
        if not result.empty:
            value = result.iloc[0, 0]
            col_name = sql.split("(")[1].split(")")[0].replace('"', '')
            return f"The average value of {col_name} is {value}."
    
    # Default formatting for other queries
    if len(result) == 0:
        return "No results found."
    elif len(result) == 1:
        return "1 row found:\n" + result.to_string()
    else:
        return f"{len(result)} rows found:\n" + result.to_string()

def test_query(question):
    """Test a natural language query and show the results."""
    llm = LLMHandler()
    sql = llm.nl_to_sql(question)
    print(f"Question: {question}")
    print(f"Generated SQL: {sql}")
    
    if sql.startswith(("Tables in the database:", "Columns in")):
        print(f"Result: {sql}\n")
        return
    
    try:
        conn = sqlite3.connect(DB_PATH)
        result = pd.read_sql_query(sql, conn)
        conn.close()
        formatted_result = format_sql_result(sql, result)
        print(f"Result: {formatted_result}\n")
    except Exception as e:
        print(f"Error: {str(e)}\n")

def main():
    """Run tests for various query types."""
    test_queries = [
        "What tables are in this database?",
        "How many columns in the kettlepump table?",
        "How many rows in the kettlepump table?",
        "What is the highest AC current in kettlepump?",
        "Show me the average temperature from compressor",
        "What are all the columns in users table?",
        "Show me all users",
        "Which user has the highest age?",
        "What is the number of null values in the stopped column of the kettlepump table?",
        "How many empty values are in the Button Down column of kettlepump?",
        "Count missing values in AC Current column of kettlepump"
    ]
    
    for query in test_queries:
        test_query(query)

if __name__ == "__main__":
    main() 
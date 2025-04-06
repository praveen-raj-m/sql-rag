import sqlite3
import pandas as pd
from llm_utils import LLMHandler
from db_utils import DB_PATH

def format_sql_result(sql: str, result: pd.DataFrame) -> str:
    """Format the SQL result based on query type."""
    # Get column names from the result DataFrame
    column_names = list(result.columns) if not result.empty else []
    
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
    
    # For queries showing rows with NULL values
    if "IS NULL" in sql and "COUNT(*)" not in sql:
        col_name = sql.split("WHERE")[1].split("IS NULL")[0].strip().replace('"', '')
        table_name = sql.split("FROM")[1].split("WHERE")[0].strip()
        
        if len(result) == 0:
            return f"No rows found with NULL values in the {col_name} column of the {table_name} table."
        else:
            return f"Found {len(result)} rows with NULL values in the {col_name} column of the {table_name} table:\n{result.to_string()}"
    
    # For COUNT(DISTINCT) queries
    if "COUNT(DISTINCT" in sql:
        if not result.empty:
            count = result.iloc[0, 0]
            col_name = sql.split("(DISTINCT")[1].split(")")[0].strip().replace('"', '')
            table_name = sql.split("FROM")[1].strip().rstrip(';')
            return f"There are {count:,} unique values in the {col_name} column of the {table_name} table."
                
    # For MAX/MIN queries
    if "MAX(" in sql or "MIN(" in sql:
        if not result.empty:
            value = result.iloc[0, 0]
            operation = "maximum" if "MAX(" in sql else "minimum"
            col_name = sql.split("(")[1].split(")")[0].replace('"', '')
            table_name = sql.split("FROM")[1].strip().rstrip(';')
            return f"The {operation} value of {col_name} in the {table_name} table is {value}."
            
    # For AVG queries
    if "AVG(" in sql:
        if not result.empty:
            value = result.iloc[0, 0]
            col_name = sql.split("(")[1].split(")")[0].replace('"', '')
            table_name = sql.split("FROM")[1].strip().rstrip(';')
            return f"The average value of {col_name} in the {table_name} table is {value}."
            
    # For SUM queries
    if "SUM(" in sql:
        if not result.empty:
            value = result.iloc[0, 0]
            col_name = sql.split("(")[1].split(")")[0].replace('"', '')
            table_name = sql.split("FROM")[1].strip().rstrip(';')
            return f"The sum of {col_name} in the {table_name} table is {value}."
            
    # For LENGTH queries (character count)
    if "LENGTH(" in sql and "REPLACE" not in sql and "ORDER BY" not in sql:
        if not result.empty:
            value = result.iloc[0, 0]
            return f"The character count is {value}."
            
    # For word count queries
    if "LENGTH(" in sql and "REPLACE" in sql:
        if not result.empty:
            value = result.iloc[0, 0]
            return f"The word count is {value}."
    
    # For text length ordering (longest/shortest)
    if "text_length" in sql and ("ORDER BY text_length DESC" in sql or "ORDER BY text_length ASC" in sql):
        if not result.empty:
            # Get the column name and value
            direction = "longest" if "DESC" in sql else "shortest"
            
            # Find the text column - it should be any column that's not 'text_length' or numeric
            text_column = None
            text_value = None
            length_value = None
            
            for col in column_names:
                if col != 'text_length' and isinstance(result.iloc[0][col], str):
                    text_column = col
                    text_value = result.iloc[0][col]
                    break
            
            if 'text_length' in column_names:
                length_value = result.iloc[0]['text_length']
                
            if text_column and text_value and length_value:
                return f"The {direction} text in the {text_column} column is '{text_value}' with a length of {length_value} characters."
            
            # Generic fallback if we couldn't identify the text column
            return f"Found a record with text length {result.iloc[0]['text_length'] if 'text_length' in column_names else 'unknown'}."
    
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
        # Basic queries
        "What tables are in this database?",
        "How many columns in the kettlepump table?",
        "How many rows in the kettlepump table?",
        
        # Max/Min/Avg queries
        "What is the highest AC current in kettlepump?",
        "What is the minimum value in the AC Current column of kettlepump?",
        "Show me the average temperature from compressor",
        
        # Column and schema queries
        "What are all the columns in users table?",
        "Show me all users",
        "Which user has the highest age?",
        
        # Null value queries
        "What is the number of null values in the stopped column of the kettlepump table?",
        "How many empty values are in the Button Down column of kettlepump?",
        "Count missing values in AC Current column of kettlepump",
        
        # Show rows with NULL values
        "Show me the rows where Stopped column is null in kettlepump",
        "Show the data where Button Down is empty in kettlepump",
        "Display records where AC Current is null",
        
        # Survey table queries with complex column names
        "Show the data where How often do you use AI tools for academic purposes? is empty in survey",
        "Count null values in Do you think integrating AI into education could create more personalized learning experiences?",
        "How many missing values are in the What concerns do you have about AI in education? column",
        "Show the rows where Have you received training on using AI tools? is null",
        
        # Statistical queries
        "What is the sum of ages in the users table?",
        "Calculate the total of AC Current values in kettlepump",
        "What is the standard deviation of ages in users?",
        "Find the median age in users table",
        
        # Text analysis
        "Count the number of words in the name column of users",
        "What is the character count in the name field for users?",
        "Find the longest name in the users table",
        "What is the shortest name in the users table?",
        
        # Distinct count queries
        "How many unique ages are there in the users table?",
        "Count the distinct values in the AC Current column of kettlepump"
    ]
    
    print("=" * 80)
    print("SQL RAG DASHBOARD - COMPREHENSIVE TEST SUITE")
    print("=" * 80)
    
    for i, query in enumerate(test_queries, 1):
        print(f"\nTEST #{i}: {query}")
        print("-" * 80)
        test_query(query)
        
    print("\n" + "=" * 80)
    print("ALL TESTS COMPLETED")
    print("=" * 80)

if __name__ == "__main__":
    main() 
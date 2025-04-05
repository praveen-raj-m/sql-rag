from llm_utils import LLMHandler
import sqlite3

def run_query(question):
    print(f"\nQuestion: {question}")
    print("-" * 50)
    
    # Initialize LLM handler with MCP
    llm = LLMHandler()
    
    # Generate SQL
    sql = llm.nl_to_sql(question)
    print("Generated SQL:", sql)
    
    # Execute query
    if not sql.startswith("Error"):
        conn = sqlite3.connect('rag.db')
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        
        print("\nResults:")
        print("-" * 50)
        for row in results:
            print(row)
        conn.close()
    else:
        print("\nQuery failed validation")

# Test queries
questions = [
    "Select all users who are older than 28 years",
    "Calculate the average age from the users table",
    "Find the name and age of the youngest user in the users table",
    "List all users ordered by their age"
]

for question in questions:
    try:
        run_query(question)
        print("\n" + "="*50 + "\n")
    except Exception as e:
        print(f"Error: {str(e)}\n") 
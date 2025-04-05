import subprocess
from mcp_utils import MCPValidator
import json
import os
from typing import Dict, List, Optional

class LLMHandler:
    def __init__(self, model_name: str = "codellama:13b-instruct"):
        self.model_name = model_name
        self.mcp = MCPValidator()
        
    def _build_enhanced_prompt(self, user_question: str) -> str:
        """Build an enhanced prompt with schema information and examples"""
        prompt = (
            "You are an expert SQLite query generator. Follow these rules strictly:\n"
            "1. Use only valid SQLite syntax\n"
            "2. Only use tables and columns that exist in the schema\n"
            "3. Always use proper column name quoting for columns with spaces or special characters\n"
            "4. Return only the SQL query, no explanations\n"
            "5. Use proper SQL formatting and indentation\n"
            "6. Do not use DROP, DELETE, ALTER, or UPDATE statements\n"
            "7. Only use SELECT statements for queries\n\n"
            "Database Schema:\n"
        )
        
        # Add schema information with example queries
        for table, columns in self.mcp.schema.items():
            prompt += f"Table: {table}\n"
            for col_name, col_info in columns.items():
                if isinstance(col_info, dict):
                    # New schema format
                    example = col_info.get('example_query', col_name)
                    prompt += f"  - Column: {example}\n"
                    prompt += f"    Type: {col_info['type']}\n"
                    if col_info.get('primary_key'):
                        prompt += f"    (Primary Key)\n"
                else:
                    # Old schema format
                    prompt += f"  - {col_name}: {col_info}\n"
            prompt += "\n"
            
        # Add examples with proper quoting
        prompt += (
            "Examples:\n"
            'Question: What is the highest value in the "Temperature, F" column?\n'
            'SQL: SELECT MAX("Temperature, F") FROM measurements;\n\n'
            'Question: Show all records where "AC Current" is above 5 amps\n'
            'SQL: SELECT * FROM devices WHERE "AC Current" > 5;\n\n'
            "Now answer this question:\n"
            f"Question: {user_question}\n"
            "SQL:"
        )
        
        return prompt
        
    def nl_to_sql(self, user_question: str) -> str:
        """Convert natural language to SQL with MCP validation"""
        try:
            # Build enhanced prompt
            prompt = self._build_enhanced_prompt(user_question)
            
            # Run LLM
            result = subprocess.run(
                ["ollama", "run", self.model_name, prompt],
                capture_output=True,
                text=True
            )
            output = result.stdout.strip()
            
            # Extract SQL query
            if "```sql" in output:
                sql_query = output.split("```sql")[1].split("```")[0].strip()
            elif "```" in output:
                sql_query = output.split("```")[1].strip()
            else:
                sql_query = output
                
            # Validate query
            if not self.mcp.validate_sql_query(sql_query):
                return "Error: Invalid SQL query generated"
                
            return sql_query
            
        except Exception as e:
            return f"Error: {str(e)}"
            
    def format_response(self, sql_query: str, result: str) -> str:
        """Format the response using MCP"""
        return self.mcp.format_response(sql_query, result)
        
    def update_schema(self, table_name: str, columns: Dict[str, str]) -> None:
        """Update schema information"""
        self.mcp.update_schema(table_name, columns)

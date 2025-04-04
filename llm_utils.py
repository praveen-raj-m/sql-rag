import requests

OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"
MODEL = "mistral"  # Or llama2, codellama, etc.

def nl_to_sql(prompt: str, schema: str) -> str:
    full_prompt = f"""
You are an AI that converts natural language into SQL queries.
Schema:
{schema}

Natural language: {prompt}
SQL:"""

    response = requests.post(OLLAMA_ENDPOINT, json={
        "model": MODEL,
        "prompt": full_prompt,
        "stream": False
    })

    return response.json()['response'].strip()
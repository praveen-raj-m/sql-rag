import requests
import re

OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"
MODEL = "llama3"

def nl_to_sql(prompt: str, schema: str) -> str:
    full_prompt = """
You are an expert AI that translates natural language into pure SQL queries.
Do not include explanations or Markdown formatting. Just return the raw SQL.

Schema:
{schema}

User Query: {prompt}
SQL:
"""

    response = requests.post(OLLAMA_ENDPOINT, json={
        "model": MODEL,
        "prompt": full_prompt,
        "stream": False
    })

    raw = response.json()['response'].strip()
    match = re.search(r"(SELECT|INSERT|UPDATE|DELETE)[\\s\\S]+?;", raw, re.IGNORECASE)
    return match.group(0).strip() if match else raw

import subprocess

def nl_to_sql(prompt: str) -> str:
    try:
        result = subprocess.run(
            ["ollama", "run", "codellama:13b-instruct", prompt],
            capture_output=True,
            text=True
        )
        output = result.stdout.strip()

        if "```sql" in output:
            return output.split("```sql")[1].split("```")[0].strip()
        elif "```" in output:
            return output.split("```")[1].strip()
        return output
    except Exception as e:
        return f"Error: {str(e)}"

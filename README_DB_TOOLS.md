# Database Management Tools

This repository contains several tools to help manage your SQLite database for the SQL RAG Dashboard.

## Issue with Gradio Table Deletion

The original SQL RAG Dashboard app has a bug where deleting tables via the Gradio interface causes a "KeyError: 0" exception. This occurs in Gradio's internal processing when it tries to update components after table deletion.

## Solution: Standalone Database Management Tools

We've provided several alternatives to manage your database tables without relying on the problematic Gradio interface:

### 1. Command Line Tools (Recommended)

#### `db_manager.py`

A comprehensive command line tool for database operations.

**Usage:**

```bash
# List all tables
python db_manager.py list

# Delete a specific table
python db_manager.py delete <table_name>

# Create the sample users table
python db_manager.py create-users

# Refresh the schema.json file
python db_manager.py refresh-schema

# View help information
python db_manager.py help
```

#### `delete_table.py`

A simpler script focused only on deleting tables.

**Usage:**

```bash
# Delete a specific table
python delete_table.py <table_name>
```

### 2. Simplified Gradio App

We've created a simplified Gradio app that focuses just on table management. This app uses simpler callback structures to avoid the Gradio state errors.

**Usage:**

```bash
# Run the table manager app
python table_manager_app.py
```

The app provides a simple interface to:

- Select and preview tables
- Delete tables
- Create the sample users table

## Troubleshooting

If you encounter issues with any of the tools:

1. **Check Database Path**: Make sure the `DB_PATH` in each script points to your correct database file (currently set to `sqlite.db`).

2. **Metadata Directory**: Ensure the `metadata` directory exists and is writable.

3. **Manual Deletion**: If all else fails, you can manually delete tables using SQLite commands:
   ```bash
   sqlite3 sqlite.db "DROP TABLE IF EXISTS <table_name>;"
   ```
   Then manually delete any corresponding metadata files in the `metadata` directory.

## Workflow Recommendations

1. **Use the command line tools** for regular database management tasks. They're more reliable and provide clear feedback.

2. **Try the simplified Gradio app** if you prefer a graphical interface.

3. **Avoid using the table deletion feature** in the original SQL RAG Dashboard until the Gradio state issues are resolved.

## Future Improvements

To fix the original SQL RAG Dashboard app, the `delete_table_wrapper` function needs to be redesigned to better handle Gradio's component update mechanism. This would require a deeper refactoring of the app's component structure and callback system.

from flask import Flask, request, jsonify, render_template
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
import pandas as pd
import google.generativeai as genai
import re # Import the regular expression module

app = Flask(__name__, template_folder="templates", static_folder="static")

# --- DB paths ---
STORED_DB = "data/sqldb.db"
UPLOADED_DB = "data/uploaded_files_sqldb.db"

# --- Gemini API Key and Model Configuration ---
gemini_api_key = os.environ.get("GOOGLE_API_KEY")
if not gemini_api_key:
    raise ValueError("Set GOOGLE_API_KEY environment variable")

# Configure SDK
genai.configure(api_key=gemini_api_key)

# Create the model once for efficiency
model = genai.GenerativeModel('gemini-2.5-flash')

# --- Helper Functions ---

def ask_gemini(prompt: str) -> str:
    """Generates content using the Gemini model."""
    response = model.generate_content(prompt)
    return response.text

def get_db_schema(db_file):
    """Gets all column names from all tables, quoted and formatted for the prompt."""
    engine = create_engine(f"sqlite:///{db_file}")
    schema_str = ""
    with engine.connect() as conn:
        table_names = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table';")).fetchall()
        for table in table_names:
            table_name = table[0]
            if table_name.startswith("sqlite_"):
                continue
            schema_str += f"Table: `{table_name}`\nColumns:\n"
            columns = conn.execute(text(f"PRAGMA table_info(`{table_name}`);")).fetchall()
            # Provide a clean, quoted list of column names
            for col in columns:
                schema_str += f'- "{col[1]}"\n'
            schema_str += "\n"
    return schema_str

def execute_sql(db_file, sql_query):
    """Execute SQL query on SQLite DB"""
    engine = create_engine(f"sqlite:///{db_file}")
    with engine.connect() as conn:
        try:
            result = conn.execute(text(sql_query)).fetchall()
            return result
        except SQLAlchemyError as e:
            return f"SQL Error: {e.orig}"

# --- Routes ---

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/ask", methods=["POST"])
def ask():
    data = request.json
    question = data.get("question")
    chat_type = data.get("chat_type")

    db_file = STORED_DB if chat_type == "sql-stored" else UPLOADED_DB

    if not os.path.exists(db_file):
        return jsonify({"error": f"DB file {db_file} does not exist"}), 400

    try:
        db_schema = get_db_schema(db_file)
        if not db_schema:
             return jsonify({"error": f"No tables found in the database: {db_file}"}), 400

        sql_query = ""
        result_str = ""
        max_retries = 2

        for attempt in range(max_retries):
            # Step 1: Generate SQL query with advanced prompt
            if attempt == 0:
                prompt = (
                    f"You are an expert SQLite writer. You MUST follow these rules:\n"
                    f"1. **CRITICAL RULE:** Column names with spaces or special characters MUST be enclosed in double quotes (\"). For example, to use a column named 'Recharge from Rainfall-MON', you must write `SELECT \"Recharge from Rainfall-MON\"`.\n"
                    f"2. To find the total recharge from rainfall, you MUST add the \"Recharge from Rainfall-MON\" and \"Recharge from Rainfall-NM\" columns together.\n\n"
                    f"Here is the database schema:\n{db_schema}\n\n"
                    f"---\n\n"
                    f"Based on all the rules and the schema, write a single, valid SQLite query to answer the question: {question}"
                )
            else:
                prompt = (
                    f"The previous attempt failed. Please fix it. You MUST follow these rules:\n"
                    f"1. **CRITICAL RULE:** Column names with spaces or special characters MUST be enclosed in double quotes (\").\n"
                    f"2. To find total rainfall recharge, you MUST add \"Recharge from Rainfall-MON\" and \"Recharge from Rainfall-NM\".\n\n"
                    f"Schema:\n{db_schema}\n"
                    f"Original Question: {question}\n"
                    f"Failed Response/Query: {sql_query}\n"
                    f"Error Message: {result_str}\n\n"
                    f"Provide only the corrected, valid SQLite query inside a ```sql code block."
                )

            raw_response = ask_gemini(prompt)
            
            # Stricter SQL Extraction
            match = re.search(r"```sql\s*([\s\S]*?)\s*```", raw_response, re.IGNORECASE)
            
            if match:
                sql_query = match.group(1).strip()
                result = execute_sql(db_file, sql_query)
                result_str = str(result)
            else:
                sql_query = raw_response
                result_str = "SQL Error: The AI response did not contain a valid SQL code block."

            if not result_str.startswith("SQL Error:"):
                break

        if result_str.startswith("SQL Error:"):
            final_error_message = f"I tried to answer, but the process failed with an error: {result_str}"
            return jsonify({"answer": final_error_message})
            
        # Step 3: Generate a natural language answer
        answer_prompt = (
            f"User Question: {question}\n"
            f"SQL Result: {result_str}\n\n"
            f"Provide a clear, natural language answer."
        )
        answer = ask_gemini(answer_prompt)
        return jsonify({"answer": answer})
    
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@app.route("/upload", methods=["POST"])
def upload_files():
    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "No files uploaded"}), 400

    engine = create_engine(f"sqlite:///{UPLOADED_DB}")
    for f in files:
        filename = f.filename
        name = os.path.splitext(filename)[0].replace(" ", "_").replace("-", "_")
        ext = os.path.splitext(filename)[1]

        if ext.lower() == ".csv":
            df = pd.read_csv(f)
        elif ext.lower() == ".xlsx":
            df = pd.read_excel(f)
        else:
            return jsonify({"error": f"Unsupported file type {ext}"}), 400
        
        df.to_sql(name, engine, index=False, if_exists="replace")

    return jsonify({"message": "Files uploaded successfully."})

# --- Run Server ---
if __name__ == "__main__":
    if not os.path.exists("data"):
        os.makedirs("data")
    app.run(host="0.0.0.0", port=5000, debug=True)
import pandas as pd
from sqlalchemy import create_engine
import os

# --- Paths ---
CSV_XLSX_DIR = "data/csv_xlsx"
UPLOADED_DB = "data/sqldb.db"

# --- Create SQLite engine ---
engine = create_engine(f"sqlite:///{UPLOADED_DB}")

# --- Process all files ---
for filename in os.listdir(CSV_XLSX_DIR):
    file_path = os.path.join(CSV_XLSX_DIR, filename)
    name, ext = os.path.splitext(filename)
    
    if ext.lower() == ".csv":
        df = pd.read_csv(file_path)
    elif ext.lower() == ".xlsx":
        df = pd.read_excel(file_path)
    else:
        print(f"Skipping unsupported file: {filename}")
        continue
    
    # Save to SQLite table (table name = file name without extension)
    df.to_sql(name, engine, index=False, if_exists="replace")
    print(f"✅ {filename} → table '{name}' in {UPLOADED_DB}")

print("\nAll files processed successfully!")
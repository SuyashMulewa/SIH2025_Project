import sqlite3

conn = sqlite3.connect("data/sqldb.db")
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print("Tables:", cursor.fetchall())

conn.close()
import sqlite3
conn = sqlite3.connect("komunal.db")
conn.executescript(open("create_view.sql", "r", encoding="utf-8").read())
conn.close()
print("v_pivot_base created")

import sqlite3

DB_FILE = "komunal.db"
VIEW_NAME = "v_pivot_base"

conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

# List views
cur.execute("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name;")
views = [r[0] for r in cur.fetchall()]
print("Views:", views)

# Check that our pivot view exists
if VIEW_NAME not in views:
    print(f"\nERROR: view {VIEW_NAME} does not exist.")
    print("Create it first (CREATE VIEW ...).")
else:
    print(f"\nOK: view {VIEW_NAME} exists ✅")

    # Show columns in the view
    cur.execute(f"PRAGMA table_info({VIEW_NAME});")
    cols = cur.fetchall()
    print("\nColumns in v_pivot_base:")
    for cid, name, ctype, notnull, dflt, pk in cols:
        print(f" - {name} ({ctype})")

conn.close()

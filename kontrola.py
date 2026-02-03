# Súbor na rýchle zobrazenie údajov, prípadne kontrola "query".

import sqlite3, pandas as pd
conn = sqlite3.connect("komunal.db")
q = """

SELECT f.Rok, SUM(f.fldMnozstvo) as mnozstvo_odpadu
FROM fakty_odpad f
GROUP BY f.Rok


"""
print(pd.read_sql_query(q, conn))
conn.close()
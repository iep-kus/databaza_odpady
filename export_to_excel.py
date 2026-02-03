# Súbor na exportovanie vybraných údajov do excelu. Po každom spustení tohto súboru sa tabuľka aktualizuje.
# Spustím v terminali pomocou C:/ProgramData/anaconda3/python.exe export_to_excel.py
# Treba sa najprv uistiť, že sa nachádzam v správnom directory.

import sqlite3
import pandas as pd
from datetime import datetime
import time

DB_FILE = "komunal.db"
OUT_FILE = "komunal_view.xlsx"

def format_duration(seconds):
    minutes = int(seconds // 60)
    seconds = seconds % 60
    if minutes > 0:
        return f"{minutes} min {seconds:.1f} sec"
    else:
        return f"{seconds:.1f} sec"

def export_to_excel():
    
    start_time = time.perf_counter()

    conn = sqlite3.connect(DB_FILE)

    queries = {
        # Malá “meta” na kontrolu dát a posledného dátumu a času exportu
        "meta": f"""
            SELECT
              '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}' AS exported_at,
              (SELECT COUNT(*) FROM fakty_odpad) AS facts_rows,
              (SELECT COUNT(*) FROM obce) AS obce_rows,
              (SELECT COUNT(*) FROM odpady) AS odpady_rows,
              (SELECT COUNT(*) FROM nakladanie) AS nakladanie_rows,
              (SELECT COUNT(*) FROM odberatelia) AS odberatelia_rows;
        """,
        # Tuto píšeme "queries" podľa toho, aké tabuľky a údaje chceme zobraziť.
        # Každá "query" vytvorí do excelu tabuľku na samostatný hárok.
        "Populácia 2022":"""
          SELECT
            o.Obec, 
            p.Pocet_obyvatelov
          FROM populacia_obce p
          JOIN obce o ON o.fldICO_Povodca = p.fldICO_Povodca
          WHERE p.Rok = 2022
          ORDER BY o.Obec
        """,

        "Nárast populácie 2018-2022":"""
          SELECT
            o.Obec,
            p2018.Pocet_obyvatelov AS populacia_2018,
            p2022.Pocet_obyvatelov AS populacia_2022,
            (p2022.Pocet_obyvatelov - p2018.Pocet_obyvatelov) AS rozdiel
          FROM obce o
          JOIN populacia_obce p2018
            ON p2018.fldICO_Povodca = o.fldICO_Povodca
          AND p2018.Rok = 2018
          JOIN populacia_obce p2022
            ON p2022.fldICO_Povodca = o.fldICO_Povodca
          AND p2022.Rok = 2022
          ORDER BY Obec

        """,

        "Množstvo odpadu za rok":"""
          SELECT 
            f.Rok, 
            SUM(f.fldMnozstvo) as mnozstvo_odpadu
          FROM fakty_odpad f
          GROUP BY f.Rok
        """
    }
    # Write one Excel file, many sheets
    with pd.ExcelWriter(OUT_FILE, engine="openpyxl") as writer:
        for sheet, sql in queries.items():
            df = pd.read_sql_query(sql, conn)
            df.to_excel(writer, sheet_name=sheet[:31], index=False)  # Excel sheet name max 31 chars

    conn.close()

    end_time = time.perf_counter()
    duration = end_time - start_time

    print("Exported:", OUT_FILE)
    print(f"Export took {duration:.2f} seconds")

if __name__ == "__main__":
    export_to_excel()

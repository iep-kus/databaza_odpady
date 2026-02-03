# Súbor, ktorý vytvorí databázu z .csv súboru.
# V podstate už je nepotrebný, využije sa jedine ak chceme z nejakého dôvodu celú databázu zmeniť alebo vytvoriť
# nanovo. Mala by však byť vytvorená správne.

import sqlite3
import pandas as pd

CSV_FILE = "raw_waste_data.csv"
DB_FILE = "komunal.db"
CHUNK_SIZE = 50000
ENC = "windows-1250"
SEP = ";"

conn = sqlite3.connect(DB_FILE)
conn.execute("PRAGMA foreign_keys = ON;")
cur = conn.cursor()

cur.executescript("""
DROP TABLE IF EXISTS fakty_odpad;
DROP TABLE IF EXISTS populacia_obce;
DROP TABLE IF EXISTS odberatelia;
DROP TABLE IF EXISTS nakladanie;
DROP TABLE IF EXISTS odpady;
DROP TABLE IF EXISTS obce;

CREATE TABLE obce (
   fldICO_Povodca INTEGER PRIMARY KEY,
   Obec TEXT,
   Zdruzenie TEXT,
   Kraj TEXT,
   Kod_obec INTEGER
);
                  
CREATE TABLE populacia_obce (
   fldICO_Povodca INTEGER NOT NULL,
   Rok INTEGER NOT NULL,
   Pocet_obyvatelov INTEGER NOT NULL,
   PRIMARY KEY (fldICO_Povodca, Rok),
   FOREIGN KEY (fldICO_Povodca) REFERENCES obce(fldICO_Povodca)
);

CREATE INDEX idx_populacia_rok ON populacia_obce(Rok);


CREATE TABLE odpady (
   fldKodOdpadu INTEGER PRIMARY KEY,
   Typ_odpadu TEXT,
   Typ_odpadu2 TEXT,
   Typ_analyzy INTEGER,
   Typ_OZV INTEGER,
   Typ_triedene INTEGER,
   Trieda_odpadu TEXT,
   Typ_BIO INTEGER,
   Zakon_triedene INTEGER,
   Miera_triedenia TEXT,
   Triedene_nova_priloha1 TEXT
);

CREATE TABLE nakladanie (
   fldKodNakladania TEXT PRIMARY KEY,
   Nakladanie TEXT
);

CREATE TABLE odberatelia (
   fldICO_Odberatel INTEGER PRIMARY KEY,
   Odoberatel_nazov TEXT,
   Odoberatel_sidlo TEXT,
   Odoberatel_okres TEXT,
   fldKodObceOdberatela INTEGER,
   Obec_odberatela TEXT
);

CREATE TABLE fakty_odpad (
   Rok INTEGER NOT NULL,
   fldICO_Povodca INTEGER NOT NULL,
   fldKodOdpadu INTEGER NOT NULL,
   fldKodNakladania TEXT NOT NULL,
   fldMnozstvo REAL,
   fldICO_Odberatel INTEGER NULL,

   FOREIGN KEY (fldICO_Povodca) REFERENCES obce(fldICO_Povodca),
   FOREIGN KEY (fldKodOdpadu) REFERENCES odpady(fldKodOdpadu),
   FOREIGN KEY (fldKodNakladania) REFERENCES nakladanie(fldKodNakladania),
   FOREIGN KEY (fldICO_Odberatel) REFERENCES odberatelia(fldICO_Odberatel)
);

CREATE INDEX idx_fakty_povodca ON fakty_odpad(fldICO_Povodca);
CREATE INDEX idx_fakty_odpad ON fakty_odpad(fldKodOdpadu);
CREATE INDEX idx_fakty_rok ON fakty_odpad(Rok);
""")
conn.commit()
print("Schema created")

# Load dimension tables

# OBCE
obce_df = pd.read_csv(
    CSV_FILE,
    usecols=["fldICO_Povodca", "Obec", "Zdruzenie", "Kraj", "Kod_obec"],
    sep=SEP, encoding=ENC, low_memory=False
).drop_duplicates(subset=["fldICO_Povodca"])

obce_df["fldICO_Povodca"] = pd.to_numeric(obce_df["fldICO_Povodca"], errors="coerce").astype("Int64")
obce_df["Kod_obec"] = pd.to_numeric(obce_df["Kod_obec"], errors="coerce").astype("Int64")
obce_df = obce_df.dropna(subset=["fldICO_Povodca"])
obce_df.to_sql("obce", conn, if_exists="append", index=False)
print("Loaded obce", len(obce_df))

# POPULACIA
pop_df = pd.read_csv(
    CSV_FILE,
    usecols=["fldICO_Povodca", "Rok", "Pocet_obyvatelov"],
    sep=SEP, encoding=ENC, low_memory=False
)

pop_df["fldICO_Povodca"] = pd.to_numeric(pop_df["fldICO_Povodca"], errors="coerce").astype("Int64")
pop_df["Rok"] = pd.to_numeric(pop_df["Rok"], errors="coerce").astype("Int64")
pop_df["Pocet_obyvatelov"] = pd.to_numeric(pop_df["Pocet_obyvatelov"], errors="coerce").astype("Int64")
pop_df = pop_df.dropna(subset=["fldICO_Povodca", "Rok", "Pocet_obyvatelov"]) # keep only valid rows
pop_df = pop_df.drop_duplicates(subset=["fldICO_Povodca", "Rok"]) # one population value per (municipality, year)
pop_df.to_sql("populacia_obce", conn, if_exists="append", index=False)

print("Loaded populacia_obce", len(pop_df))

# ODPADY (typy)
odpady_df = pd.read_csv(
    CSV_FILE,
    usecols=[
        "fldKodOdpadu","Typ_odpadu","Typ_odpadu2","Typ_analyzy","Typ_OZV",
        "Typ_triedene","Trieda_odpadu","Typ_BIO","Zakon_triedene",
        "Miera_triedenia","Triedene_nova_priloha1"
    ],
    sep=SEP, encoding=ENC, low_memory=False
).drop_duplicates(subset=["fldKodOdpadu"])

odpady_df["fldKodOdpadu"] = pd.to_numeric(odpady_df["fldKodOdpadu"], errors="coerce").astype("Int64")
odpady_df = odpady_df.dropna(subset=["fldKodOdpadu"])
odpady_df.to_sql("odpady", conn, if_exists="append", index=False)
print("Loaded odpady", len(odpady_df))

# NAKLADANIE
nakladanie_df = pd.read_csv(
    CSV_FILE,
    usecols=["fldKodNakladania","Nakladanie"],
    sep=SEP, encoding=ENC, low_memory=False
).drop_duplicates(subset=["fldKodNakladania"])

nakladanie_df["fldKodNakladania"] = nakladanie_df["fldKodNakladania"].astype(str)
nakladanie_df = nakladanie_df.dropna(subset=["fldKodNakladania"])
nakladanie_df.to_sql("nakladanie", conn, if_exists="append", index=False)
print("Loaded nakladanie ", len(nakladanie_df))

# ODBERATELIA
odberatelia_df = pd.read_csv(
    CSV_FILE,
    usecols=[
        "fldICO_Odberatel","Odoberatel_nazov","Odoberatel_sidlo",
        "Odoberatel_okres","fldKodObceOdberatela","Obec_odberatela"
    ],
    sep=SEP, encoding=ENC, low_memory=False
).drop_duplicates(subset=["fldICO_Odberatel"])

odberatelia_df["fldICO_Odberatel"] = pd.to_numeric(odberatelia_df["fldICO_Odberatel"], errors="coerce").astype("Int64")
odberatelia_df["fldKodObceOdberatela"] = pd.to_numeric(odberatelia_df["fldKodObceOdberatela"], errors="coerce").astype("Int64")
odberatelia_df = odberatelia_df.dropna(subset=["fldICO_Odberatel"])
odberatelia_df.to_sql("odberatelia", conn, if_exists="append", index=False)
print("Loaded odberatelia ", len(odberatelia_df))

conn.commit()


# Load facts in chunks

total = 0
dropped = 0

# HLAVNA TABULKA
for chunk in pd.read_csv(CSV_FILE, chunksize=CHUNK_SIZE, sep=SEP, encoding=ENC, low_memory=False):
    fakty_df = chunk[[
        "Rok","fldICO_Povodca","fldKodOdpadu","fldKodNakladania","fldMnozstvo","fldICO_Odberatel"
    ]].copy()

    fakty_df["Rok"] = pd.to_numeric(fakty_df["Rok"], errors="coerce").astype("Int64")
    fakty_df["fldICO_Povodca"] = pd.to_numeric(fakty_df["fldICO_Povodca"], errors="coerce").astype("Int64")
    fakty_df["fldKodOdpadu"] = pd.to_numeric(fakty_df["fldKodOdpadu"], errors="coerce").astype("Int64")
    fakty_df["fldICO_Odberatel"] = pd.to_numeric(fakty_df["fldICO_Odberatel"], errors="coerce").astype("Int64")
    fakty_df["fldKodNakladania"] = fakty_df["fldKodNakladania"].astype(str)

    fakty_df["fldMnozstvo"] = (fakty_df["fldMnozstvo"].astype(str).str.replace(",", ".", regex=False))  # convert decimal comma → dot
    fakty_df["fldMnozstvo"] = pd.to_numeric(fakty_df["fldMnozstvo"], errors="coerce")

    # vymazat riadky, ktore neobsahuju hodnoty v dolezitych stlpcoch - mozeme vynechat
    before = len(fakty_df)
    fakty_df = fakty_df.dropna(subset=["Rok","fldICO_Povodca","fldKodOdpadu","fldKodNakladania"])
    dropped += before - len(fakty_df)

    fakty_df.to_sql("fakty_odpad", conn, if_exists="append", index=False)

    total += len(fakty_df)
    print(f"Chunk inserted, rows: {len(fakty_df)}   total: {total}")

conn.commit()

print("\nRow counts:")
for t in ["obce","populacia_obce","odpady","nakladanie","odberatelia","fakty_odpad"]:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(t, cur.fetchone()[0])

cur.execute("PRAGMA foreign_key_check;")
viol = cur.fetchall()
print("\nFK violations:", len(viol))

print("\nDropped rows (missing essential keys):", dropped)

conn.close()
print("\nImport done")


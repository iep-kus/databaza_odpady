import sqlite3

conn = sqlite3.connect("komunal.db")

conn.executescript("""
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

DROP VIEW IF EXISTS v_pivot_base;

CREATE VIEW v_pivot_base AS
SELECT
  -- Roky
  f.Rok,

  -- Pôvodcovia / obce + kódy obcí
  f.fldICO_Povodca,
  o.Obec,
  o.Kraj,
  o.Zdruzenie,
  o.Kod_obec,

  -- Typy odpadu + kódy
  f.fldKodOdpadu,
  od.Typ_odpadu,
  od.Typ_odpadu2,
  od.Trieda_odpadu,
  od.Typ_analyzy,
  od.Typ_OZV,
  od.Typ_triedene,
  od.Typ_BIO,
  od.Zakon_triedene,
  od.Miera_triedenia,
  od.Triedene_nova_priloha1,

  -- Typy nakladania + kódy
  f.fldKodNakladania,
  n.Nakladanie,

  -- Odberatelia + kódy (môže obsahovať hodnoty NULL)
  f.fldICO_Odberatel,
  ob.Odoberatel_nazov,
  ob.Odoberatel_okres,
  ob.fldKodObceOdberatela,
  ob.Obec_odberatela,

  -- Množstvo
  f.fldMnozstvo

FROM fakty_odpad f
JOIN obce o ON o.fldICO_Povodca = f.fldICO_Povodca
JOIN odpady od ON od.fldKodOdpadu = f.fldKodOdpadu
JOIN nakladanie n ON n.fldKodNakladania = f.fldKodNakladania
LEFT JOIN odberatelia ob ON ob.fldICO_Odberatel = f.fldICO_Odberatel;

""")

conn.close()
print("v_pivot_base created")

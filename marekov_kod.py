import numpy as np

print("hello world")
import pandas as pd
import time

def load_komunal(path):
    start = time.time()
    soll_KO = pd.read_excel(path)
    end = time.time()
    print("Loading of database: ", end-start)

    return soll_KO

def reduction(soll_KO, key, values):
    try:
        soll = pd.read_excel("df_" + key + ".xlsx")
        print("loaded, " + key)
    except:
        soll = soll_KO[values].copy()
        soll = soll.drop_duplicates(subset=[key]).reset_index(drop=True)
        soll.to_excel("df_" + key + ".xlsx", index = False)
    return soll


def load_matrices(soll_KO, soll_obce, mode):
    soll_blue     = reduction(soll_KO, 'fldKodOdpadu', ["fldKodOdpadu", "Typ_odpadu", "Typ_analyzy", "Typ_OZV", "Typ_triedene", "Trieda_odpadu", "Typ_odpadu2", "Typ_BIO", "Zakon_triedene", "Miera_triedenia", "Triedene_nova_priloha1"])
    soll_green    = reduction(soll_KO, 'fldKodNakladania', ["fldKodNakladania", "Nakladanie"])
    soll_yellow   = reduction(soll_KO,'fldICO_Odberatel',["fldICO_Odberatel", "Odoberatel_nazov", "Odoberatel_sidlo", "Odoberatel_okres"])
    soll_orange   = reduction(soll_KO,'fldICO_Povodca',["fldICO_Povodca", "Obec", "Zdruzenie", "Kraj"])
    ###
    ### If loaded excel already had renamed columns then pass otherwise rename them
    try:
        soll_grey = reduction(soll_obce, 'Kod', ["Kod", "Obec"])
        soll_grey = soll_grey.rename(columns = {"Kod" : "fldKodObceOdberatela", "Obec" : "Obec_odberatela"})
        soll_grey.to_excel("df_fldKodObceOdberatela.xlsx", index = False)
    except:
        soll_grey     = reduction(soll_obce,'fldKodObceOdberatela', ["fldKodObceOdberatela", "Obec_odberatela"])

    return soll_blue, soll_green, soll_yellow, soll_orange, soll_grey


def population():
    start = time.time()

    soll_2018 = pd.read_excel("Pocet_obyvatelov_2018.xlsx", sheet_name="Hárok1")
    soll_2019 = pd.read_excel("Pocet_obyvatelov_2019.xlsx", sheet_name="Hárok1", names=['Obec', '2019'])
    soll_2020 = pd.read_excel("Pocet_obyvatelov_2020.xlsx", sheet_name="Sheet1", header=None,
                              names=['Obec', 'Okres', '2020'])
    soll_2021 = pd.read_excel("Pocet_obyvatelov_2021.xlsx", sheet_name="Hárok1", header=None, names=['Obec', '2021'])
    soll_2022 = pd.read_excel("Pocet_obyvatelov_2022.xlsx", sheet_name="Hárok1", names=['Obec', '2022'])
    soll_2023 = pd.read_excel("Pocet_obyvatelov_2023.xlsx", sheet_name="Hárok1", skiprows=5)

    soll_2018 = pd.merge(soll_2018, soll_2019, on="Obec", how="left")
    soll_2018 = pd.merge(soll_2018, soll_2020[["Obec", "2020"]], on="Obec", how="left")
    soll_2018 = pd.merge(soll_2018, soll_2021, on="Obec", how="left")
    soll_2018 = pd.merge(soll_2018, soll_2022, on="Obec", how="left")
    soll_2018 = pd.merge(soll_2018, soll_2023[["Obec", "2023"]], on="Obec", how="left")
    # Filter rows in df_2019 where "Obec" is not in df_2018["Obec"]

    end = time.time()
    print("Population database: ", end-start)
    return soll_2018


def komunal(path, df_blue, df_green, df_yellow, df_orange, df_grey):
    df_KO_23 = load_komunal(path)

    ### Premenovanie stlpcov
    df_KO_23 = df_KO_23.rename(columns = {"KodOdpadu" : "fldKodOdpadu",
                                          "ICO_Povodca" : "fldICO_Povodca",
                                          "KodNakladania" : "fldKodNakladania",
                                          "ICO_Odberatel" : "fldICO_Odberatel",
                                          "KodObceOdberatela" : "fldKodObceOdberatela",
                                          "Mnozstvo" : "fldMnozstvo"})

    ### Merge stlpcov podla kodov
    df_KO_23 = pd.merge(df_KO_23, df_blue, on = "fldKodOdpadu", how = "left")
    df_KO_23 = pd.merge(df_KO_23, df_orange, on = "fldICO_Povodca", how = "left")
    df_KO_23 = pd.merge(df_KO_23, df_green, on = "fldKodNakladania", how = "left")
    df_KO_23 = pd.merge(df_KO_23, df_yellow, on = "fldICO_Odberatel", how = "left")
    df_KO_23 = pd.merge(df_KO_23, df_grey, on = "fldKodObceOdberatela", how = "left")

    ### Pridanie in7ch zdrojov
    df_KO_23["IneZdroje"] = 0

    ### Drop kod obce povodcu
    df_KO_23 = df_KO_23.drop("Kod_Obce_Pôvodcu", axis=1)
    return df_KO_23


def komunal_druhy(path, df_blue, df_green, df_yellow, df_orange, df_grey):
    df_KO_23_druhs = load_komunal(path)

    ### Premenovanie stlpcov
    df_KO_23_druhs = df_KO_23_druhs.rename(columns={"KodOdpadu": "fldKodOdpadu",
                                                    "ICO_Povodca": "fldICO_Povodca",
                                                    "KodNakladania": "fldKodNakladania",
                                                    "ICO": "fldICO_Odberatel",
                                                    "KodZUJ_Prevadzky": "fldKodObceOdberatela",
                                                    "Mnozstvo": "fldMnozstvo"})

    df_KO_23_druhs = df_KO_23_druhs.drop(["KodOkresu", "KodNACE"], axis=1)

    df_KO_23_druhs = pd.merge(df_KO_23_druhs, df_blue, on="fldKodOdpadu", how="left")
    df_KO_23_druhs = pd.merge(df_KO_23_druhs, df_orange, on="fldICO_Povodca", how="left")
    df_KO_23_druhs = pd.merge(df_KO_23_druhs, df_green, on="fldKodNakladania", how="left")
    df_KO_23_druhs = pd.merge(df_KO_23_druhs, df_yellow, on="fldICO_Odberatel", how="left")
    df_KO_23_druhs = pd.merge(df_KO_23_druhs, df_grey, on="fldKodObceOdberatela", how="left")

    df_KO_23_druhs["IneZdroje"] = 0
    return df_KO_23_druhs


def ine_zdroje(path, df_blue, df_green, df_yellow, df_orange, df_grey):
    df_KO_23_ine = load_komunal(path)

    ###
    df_obce = pd.read_excel("Obce_charakteristika.xlsx")
    dict_obce = dict(zip(df_obce["Kod"], df_obce["Obec"]))
    Bratislava_Kosice_dict = {528595 : "Bratislava", 529311 : "Bratislava", 529320 : "Bratislava", 529338 : "Bratislava",
                              529346 : "Bratislava", 529354 : "Bratislava", 529362 : "Bratislava", 529401 : "Bratislava",
                              529371 : "Bratislava", 529389 : "Bratislava", 529397 : "Bratislava", 529419 : "Bratislava",
                              529427 : "Bratislava", 529435 : "Bratislava", 529443 : "Bratislava", 529460 : "Bratislava",
                              529494 : "Bratislava",
                              599891 : "Košice", 598119 : "Košice", 598151 : "Košice", 599875 : "Košice",
                              598186 : "Košice", 598127 : "Košice", 598194 : "Košice", 599972 : "Košice",
                              598216 : "Košice", 598208 : "Košice", 599859 : "Košice", 599883 : "Košice",
                              599841 : "Košice", 598224 : "Košice", 598682 : "Košice", 599018 : "Košice",
                              599093 : "Košice", 599824 : "Košice", 599794 : "Košice", 599816 : "Košice",
                              599786 : "Košice", 599913 : "Košice"}
    dict_obce = dict_obce | Bratislava_Kosice_dict
    print(dict_obce)
    ### Premenovanie stlpcov
    df_KO_23_ine = df_KO_23_ine.rename(columns={"KodOdpadu": "fldKodOdpadu",
                                                "ICO_Povodca": "fldICO_Povodca",
                                                "KodNakladania": "fldKodNakladania",
                                                "ICO": "fldICO_Odberatel",
                                                "KodZUJ_Prevadzky": "Kod_obec",
                                                "Mnozstvo": "fldMnozstvo"})

    df_KO_23_ine = df_KO_23_ine.drop(["KodOkresu", "KodNACE"], axis=1)

    df_KO_23_ine = pd.merge(df_KO_23_ine, df_blue, on="fldKodOdpadu", how="left")

    df_KO_23_ine = pd.merge(df_KO_23_ine, df_green, on="fldKodNakladania", how="left")
    df_KO_23_ine = pd.merge(df_KO_23_ine, df_yellow, on="fldICO_Odberatel", how="left")

    df_KO_23_ine["Obec"] = df_KO_23_ine["Kod_obec"].map(dict_obce)
    df_KO_23_ine = pd.merge(df_KO_23_ine, df_orange, on = "Obec", how = "left")
    #df_KO_23_ine = pd.merge(df_KO_23_ine, df_grey, on="fldKodObceOdberatela", how="left")

    df_KO_23_ine["IneZdroje"] = 1
    return df_KO_23_ine

def zalohy(path, df_blue, df_green, df_yellow, df_orange, df_grey):
    df_KO_23_zalohy = load_komunal(path)

    ###
    df_obce = pd.read_excel("Obce_charakteristika.xlsx")
    dict_obce = dict(zip(df_obce["Kod"], df_obce["Obec"]))
    Bratislava_Kosice_dict = {528595 : "Bratislava", 529311 : "Bratislava", 529320 : "Bratislava", 529338 : "Bratislava",
                              529346 : "Bratislava", 529354 : "Bratislava", 529362 : "Bratislava", 529401 : "Bratislava",
                              529371 : "Bratislava", 529389 : "Bratislava", 529397 : "Bratislava", 529419 : "Bratislava",
                              529427 : "Bratislava", 529435 : "Bratislava", 529443 : "Bratislava", 529460 : "Bratislava",
                              529494 : "Bratislava",
                              599891 : "Košice", 598119 : "Košice", 598151 : "Košice", 599875 : "Košice",
                              598186 : "Košice", 598127 : "Košice", 598194 : "Košice", 599972 : "Košice",
                              598216 : "Košice", 598208 : "Košice", 599859 : "Košice", 599883 : "Košice",
                              599841 : "Košice", 598224 : "Košice", 598682 : "Košice", 599018 : "Košice",
                              599093 : "Košice", 599824 : "Košice", 599794 : "Košice", 599816 : "Košice",
                              599786 : "Košice", 599913 : "Košice"}
    dict_obce = dict_obce | Bratislava_Kosice_dict
    print(dict_obce)

    df_KO_23_zalohy["Kod odpadu"] = np.where(df_KO_23_zalohy["Kod odpadu"] == 150104, 200104,
                                             np.where(df_KO_23_zalohy["Kod odpadu"] == 150102, 200139, 0))
    ### Premenovanie stlpcov
    df_KO_23_zalohy = df_KO_23_zalohy.rename(columns={"Kod odpadu": "fldKodOdpadu",
                                                "ICO_Povodca": "fldICO_Povodca",
                                                "Kod_nakladania": "fldKodNakladania",
                                                "ICO": "fldICO_Odberatel",
                                                "KodZUJ_Prevadzky": "Kod_obec",
                                                "Mnozstvo": "fldMnozstvo"})

    df_KO_23_zalohy = df_KO_23_zalohy.drop(["KodOkresu", "KodNACE"], axis=1)

    df_KO_23_zalohy = pd.merge(df_KO_23_zalohy, df_blue, on="fldKodOdpadu", how="left")

    df_KO_23_zalohy = pd.merge(df_KO_23_zalohy, df_green, on="fldKodNakladania", how="left")
    df_KO_23_zalohy = pd.merge(df_KO_23_zalohy, df_yellow, on="fldICO_Odberatel", how="left")

    df_KO_23_zalohy["Typ_odpadu"] = np.where(df_KO_23_zalohy["fldKodOdpadu"] == 200104, "kovoveobaly_zalohovane",
                                             np.where(df_KO_23_zalohy["fldKodOdpadu"] == 200139, "plasty_zalohovane", ""))

    df_KO_23_zalohy["Obec"] = df_KO_23_zalohy["Kod_obec"].map(dict_obce)
    df_KO_23_zalohy = pd.merge(df_KO_23_zalohy, df_orange, on = "Obec", how = "left")
    #df_KO_23_ine = pd.merge(df_KO_23_ine, df_grey, on="fldKodObceOdberatela", how="left")

    df_KO_23_zalohy["IneZdroje"] = np.where(df_KO_23_zalohy["fldKodOdpadu"] == 200104, 1,
                                            np.where(df_KO_23_zalohy["fldKodOdpadu"] == 200139, 0, 0))
    df_KO_23_zalohy = df_KO_23_zalohy.drop(['ICO_Odberatel', 'KodObceOdberatela'], axis=1)
    return df_KO_23_zalohy

###### Main ######
timer_full = time.time()
### mode full ak chceme pracovat na celej databaze inak netreba
mode = "full"
if mode == "full":
    df_KO = load_komunal("KOMUNAL_2010_2023.xlsx")
    ### vyhodit stary rok 2023
    df_KO = df_KO[df_KO["Rok"] != 2023]
else:
    df_KO = load_komunal("copy_KOMUNAL_2010_2023.xlsx")

### naloadovat tabulky
df_obce = load_komunal("Obce_charakteristika.xlsx")
df_blue, df_green, df_yellow, df_orange, df_grey = load_matrices(df_KO, df_obce, mode)

### First part
df_KO_1 = komunal("KOMUNALNE_ODPADY_O_2023.xlsx", df_blue, df_green, df_yellow, df_orange, df_grey)
df_KO_2 = komunal_druhy("ODPAD_RISO_DRUH_190102_2023_N.xlsx", df_blue, df_green, df_yellow, df_orange, df_grey)
df_KO_3 = ine_zdroje("ODPAD_RISO_KOMUNAL_INEZDROJE_2023_N.xlsx", df_blue, df_green, df_yellow, df_orange, df_grey)
df_KO_4 = zalohy("# ODPAD_ZALOHOVANE_2023_N (## V DATABÁZE 9.830 ##).xlsx", df_blue, df_green, df_yellow, df_orange, df_grey)
df_KO_23 = pd.concat([df_KO_1, df_KO_2, df_KO_3, df_KO_4], ignore_index=True)

### TODO pocet obyvatelov novy prirastok
df_population = population()
df_KO_23= pd.merge(df_KO_23, df_population[["Obec", "2023"]], on = "Obec", how= "left")
df_KO_23 = df_KO_23.rename(columns = {"2023" : "Pocet_obyvatelov"})



df_KO_23.to_excel("copy_KOMUNALNE_ODPADY_O_2023.xlsx", index = False)




#print(df_population)
for i in range(2018,2024):
    updates_dict = dict(zip(df_population["Obec"], df_population[str(i)]))
    df_KO.loc[df_KO['Rok'] == i, 'Pocet_obyvatelov'] = df_KO.loc[df_KO['Rok'] == i, 'Obec'].map(updates_dict).fillna(
        df_KO.loc[df_KO['Rok'] == i, 'Pocet_obyvatelov'])
    #print(df_KO)

print("start concating 23 to main")
df_KO = pd.concat([df_KO, df_KO_23], ignore_index=True)

df_obce = pd.read_excel("Obce_charakteristika.xlsx")
dict_obce_inv = dict(zip(df_obce["Obec"], df_obce["Kod"]))
df_KO["Kod_obec"] = df_KO["Obec"].map(dict_obce_inv)

print("writing result into xlsx")
df_KO.to_excel("actualizacia_KOMUNAL_2010_2023_v2.xlsx",  index= False)

timer_end = time.time()
print("total time: ", timer_end - timer_full)
#print(df_KO)

### Vyhodenie
### TODO vyhodit stare hodnoty 2023

### Cast 2 zdroj 2 a in0 zdroje
### TODO ako to naparovat





#df_KO_23_druhs.to_excel("copy_ODPAD_RISO_DRUH_190102_2023_N.xlsx", index = False)


#df_KO = df_KO.head(5)
#df_KO.to_excel("copy_cat_KOMUNAL_2010_2023.xlsx",  index= False)

# _TEMP_Upload_Goava_CSV.py
import pandas as pd
from sqlalchemy import create_engine
import config
import sys
import numpy as np

# ===== UPPGIFTER ATT FYLLA I =====
GOAVA_EXCEL_FILE = 'Kunder-Period-1aug-9-okt2025_berikade_Goava.xlsx'
ENRICHMENT_TABLE_NAME = config.TABLE_NAMES['Customer_Enrichment_Dimension']

# Fyll i exakta kolumnnamn från din Excel-fil
ORGNR_COL_NAME = 'Organisation Number' # ELLER VAD DEN HETER (t.ex. 'Organisationsnummer')
SNI_GRUPP_COL_NAME = 'SNI-namn' # ELLER VAD DEN HETER (t.ex. 'Bransch')
GEO_LAN_COL_NAME = 'Län' # ELLER VAD DEN HETER (t.ex. 'Region')
OMSATTNING_COL_NAME = 'Omsättning' # ELLER VAD DEN HETER
# =================================

def clean_org_nr(series):
    """ 
    Exakt kopia av rensningsfunktionen från 1_Extract_Operative_Data.py
    för att garantera perfekt matchning.
    """
    print("-> Rensar organisationsnummer...")
    clean = series.astype(str).str.extract(r'^([\d-]+)', expand=False)
    clean = clean.str.replace('-', '', regex=False).str.strip()
    clean.replace(['', 'None', 'nan', 'NULL', 'Okänt'], pd.NA, inplace=True)
    return clean

print(f"Startar engångsladdning av Goava-data från {GOAVA_EXCEL_FILE}...")

try:
    mssql_engine = create_engine(config.MSSQL_CONN_STR)
    
    # Läs din Excel-fil
    df_goava = pd.read_excel(GOAVA_EXCEL_FILE)
    print(f"-> Läste {len(df_goava)} rader från Excel.")

    # === VIKTIGT: Rensa datan här! ===
    print(f"-> Rensar OrgNr från kolumnen: '{ORGNR_COL_NAME}'")
    df_goava['CustomerKey'] = clean_org_nr(df_goava[ORGNR_COL_NAME])
    
    # Ta bort rader där vi inte kunde hitta ett giltigt CustomerKey
    df_goava = df_goava.dropna(subset=['CustomerKey'])
    df_goava = df_goava.drop_duplicates(subset=['CustomerKey'])

    # Välj ut de kolumner vi vill spara
    cols_to_keep = {
        'Organisation Number': 'CustomerKey',
        'Företagsnamn' : 'Name',
        'Postadress' : 'Postadress',
        'SNI-kod' : 'SNI-kod',
        'Bransch' : 'Bransch',
        
    }
    
    # Kontrollera vilka kolumner som faktiskt finns
    final_cols = {}
    for k, v in cols_to_keep.items():
        if k in df_goava.columns:
            final_cols[k] = v
        else:
            print(f"VARNING: Kolumnen '{k}' hittades inte i Excel-filen.")
            
    df_to_save = df_goava[final_cols.keys()].rename(columns=final_cols)
    
    print(f"Laddar {len(df_to_save)} unika, berikade rader till MSSQL-tabell: {ENRICHMENT_TABLE_NAME}")
    
    # Ladda upp till databasen
    df_to_save.to_sql(
        ENRICHMENT_TABLE_NAME,
        mssql_engine,
        if_exists='replace', # Använd 'replace' för att skriva över gammal data
        index=False
    )
    
    print("KLART! Goava-datan finns nu i MSSQL.")

except FileNotFoundError:
    print(f"FATALT FEL: Filen '{GOAVA_EXCEL_FILE}' hittades inte.")
    print("Kontrollera att filnamnet är korrekt och att filen ligger i samma mapp.")
except KeyError as e:
    print(f"FATALT FEL: Kolumnnamnet {e} hittades inte i din Excel-fil.")
    print("Vänligen korrigera namnen i 'UPPGIFTER ATT FYLLA I' högst upp i skriptet.")
except Exception as e:
    print(f"FEL: Kunde inte ladda CSV-fil: {e}")
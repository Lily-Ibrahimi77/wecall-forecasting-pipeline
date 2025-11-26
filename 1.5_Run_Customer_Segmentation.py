import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

"""
================================================================
JOBB 1.5: Kundsegmentering (Business Logic) -
================================================================
- 'Segment_Sjukanmälan' (Kollar Key, Namn och Tjänst).
- Säkrar 'mode()' med safe_mode för att undvika krasch på tom data.
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import config
import sys
import traceback
from DataDriven_utils import add_all_features

# --- HJÄLPFUNKTION: Safe Mode ---
def safe_mode(x):
    """Hanterar om mode() returnerar tomt (t.ex. vid unik data eller tom serie)"""
    m = x.mode()
    if m.empty: return 'Okänd Typ'
    return m.iloc[0]

def create_and_save_segments():
    print("--- Startar Jobb 1.5: Kundsegmentering (Business Logic) ---")

    try:
        mssql_engine = create_engine(config.MSSQL_CONN_STR)
        print("-> Ansluten till MSSQL Data Warehouse.")
    except Exception as e:
        print(f"FATALT FEL: Kunde inte ansluta till MSSQL: {e}")
        raise Exception('Processen avbröts pga fel')

    # === STEG 1: Läs in historik ===
    table_name_training = config.TABLE_NAMES['Operative_Training_Data']
    print(f"-> Läser in historik från '{table_name_training}'...")

    try:
        cols_to_use = ['Created', 'Name', 'QueueId', 'CustomerKey', 'TalkTimeInSec', 'CallId', 'TjänstTyp'] 
        cols_str = ", ".join([f'[{col}]' for col in cols_to_use])
        
        sql_query = f'SELECT {cols_str} FROM [{table_name_training}]'
        df_history = pd.read_sql(sql_query, mssql_engine)
        df_history['Created'] = pd.to_datetime(df_history['Created']).dt.tz_localize(None)
        
        if df_history.empty:
            print("VARNING: Ingen historisk data hittades.")
            return 
            
        print(f"-> Läste {len(df_history)} samtalshändelser.")
        
    except Exception as e:
        print(f"FEL: Kunde inte läsa data från '{table_name_training}'.")
        print(f"Tekniskt fel: {e}")
        raise Exception('Processen avbröts pga fel')

    # === STEG 2: Aggregera ===
    print("-> Aggregerar per (CustomerKey)...")
    df_history_features = df_history.rename(columns={'Created': 'ds'})
    df_history_features = add_all_features(df_history_features, ds_col='ds')
    
    df_agg = df_history_features.groupby(['CustomerKey']).agg(
        Total_Samtal=('CallId', 'count'),
        Total_Samtalstid_Sek=('TalkTimeInSec', 'sum'),
        Name=('Name', 'first'),
        # Använder safe_mode här
        TjänstTyp=('TjänstTyp', safe_mode) 
    ).reset_index()
    
    df_agg['Genomsnittlig_AHT_Sek'] = (df_agg['Total_Samtalstid_Sek'] / df_agg['Total_Samtal']).fillna(0).astype(int)

    # === STEG 3: Peak Pattern ===
    print("-> Beräknar 'Peak Pattern'...")
    df_peak_pattern = df_history_features.groupby(['CustomerKey', 'veckodag', 'timme']).agg(
        Antal=('CallId', 'count')
    ).reset_index()
    df_peak_pattern = df_peak_pattern.sort_values(by='Antal', ascending=False)
    df_peak = df_peak_pattern.drop_duplicates(subset=['CustomerKey'], keep='first').copy()

    dag_map = {0: 'Mån', 1: 'Tis', 2: 'Ons', 3: 'Tor', 4: 'Fre', 5: 'Lör', 6: 'Sön'}
    df_peak['Peak_Pattern'] = df_peak['veckodag'].map(dag_map) + '-' + df_peak['timme'].astype(str)
    df_segments = pd.merge(df_agg, df_peak[['CustomerKey', 'Peak_Pattern']], on=['CustomerKey'], how='left')
    df_segments['Peak_Pattern'] = df_segments['Peak_Pattern'].fillna('Oklart')

    # === STEG 4: SEGMENTERING ===
    print("-> Skapar segment (Logiska Kvantiler)...")
    
    vol_limit = df_segments['Total_Samtal'].quantile(0.80)
    aht_limit = df_segments['Genomsnittlig_AHT_Sek'].quantile(0.50)

    print(f"   -> Gräns Hög Volym (Top 20%): > {vol_limit:.0f} samtal")
    print(f"   -> Gräns Långa Ärenden (Median): > {aht_limit:.0f} sek")

    def get_segment(row):
        
        # kollar både ID, Namn och TjänstTyp för att vara säkra (Case Insensitive)
        c_key = str(row['CustomerKey']).upper().strip()
        c_name = str(row['Name']).lower()
        c_service = str(row['TjänstTyp']).lower()
        
        # Logic: Om någon av dessa stämmer är det Sjukanmälan
        is_sick = (c_key == 'INTERNAL_SICK') or \
                  ('sjukanmälan' in c_name) or \
                  ('personal' in c_service) or \
                  ('sjuk' in c_service)

        if is_sick:
            return 'Segment_Sjukanmälan'
        
        # Vanlig logik
        vol_label = 'Hög-Volym' if row['Total_Samtal'] > vol_limit else 'Låg-Volym'
        aht_label = 'Långa-Ärenden' if row['Genomsnittlig_AHT_Sek'] > aht_limit else 'Korta-Ärenden'
        
        return f"{vol_label}_{aht_label}"

    df_segments['Behavior_Segment'] = df_segments.apply(get_segment, axis=1)
    
    print("   -> Segmentfördelning:")
    print(df_segments['Behavior_Segment'].value_counts())

    # === STEG 5: Spara Dim_Customer_Behavior ===
    output_table_name = config.TABLE_NAMES['Customer_Behavior_Dimension']
    staging_table_name = f"{output_table_name}_STAGING"
    
    final_cols = ['CustomerKey', 'Name', 'TjänstTyp', 'Total_Samtal', 'Genomsnittlig_AHT_Sek', 'Behavior_Segment']
    df_to_save = df_segments[final_cols]

    try:
        print(f"-> Sparar till STAGING '{staging_table_name}'...")
        df_to_save.to_sql(staging_table_name, mssql_engine, if_exists='replace', index=False, chunksize=1000)

        print(f"-> Flyttar till PROD '{output_table_name}'...")
        # Använder Transaction & Rollback (Säkerhet)
        with mssql_engine.connect() as connection:
            connection.execute(text(f"""
                BEGIN TRY
                    BEGIN TRANSACTION;
                    IF OBJECT_ID('{output_table_name}', 'U') IS NOT NULL DROP TABLE [{output_table_name}];
                    SELECT * INTO [{output_table_name}] FROM [{staging_table_name}];
                    COMMIT TRANSACTION;
                END TRY
                BEGIN CATCH
                    ROLLBACK TRANSACTION;
                    THROW;
                END CATCH;
            """))
            connection.commit() 
        print(f"-> KLART: '{output_table_name}' uppdaterad.")

    except Exception as e:
        print(f"FATALT FEL vid sparning av segment: {e}")
        raise Exception('Processen avbröts pga fel')

    # === STEG 6: Peak Analysis ===
    print("-> Analyserar månatliga topp-tider...")
    try:
        df_samtal = df_history_features[df_history_features['TalkTimeInSec'] > 0].copy()
        if not df_samtal.empty:
            lower_limit = df_samtal['TalkTimeInSec'].quantile(0.33)
            upper_limit = df_samtal['TalkTimeInSec'].quantile(0.66)
            if lower_limit == 0: lower_limit = 1 
            if upper_limit <= lower_limit: upper_limit = lower_limit + 60 

            df_samtal['Samtalstyp'] = 'Normal'
            df_samtal.loc[df_samtal['TalkTimeInSec'] < lower_limit, 'Samtalstyp'] = 'Kort'
            df_samtal.loc[df_samtal['TalkTimeInSec'] > upper_limit, 'Samtalstyp'] = 'Långt'

            df_peak_monthly = df_samtal.groupby(
                ['CustomerKey', 'Name', 'månad_namn', 'månad', 'veckodag_namn', 'veckodag', 'timme', 'Samtalstyp']
            ).agg(Antal_Samtal_Denna_Timme=('CallId', 'count')).reset_index()

            df_monthly_totals = df_peak_monthly.groupby(
                ['CustomerKey', 'månad_namn', 'Samtalstyp']
            )['Antal_Samtal_Denna_Timme'].sum().to_frame('Totala_Samtal_Manad_Typ').reset_index()

            df_peak_monthly = pd.merge(df_peak_monthly, df_monthly_totals, on=['CustomerKey', 'månad_namn', 'Samtalstyp'], how='left')
            
            mask = df_peak_monthly['Totala_Samtal_Manad_Typ'] > 0
            df_peak_monthly.loc[mask, 'Procent_Av_Manad_Typ'] = (df_peak_monthly['Antal_Samtal_Denna_Timme'] / df_peak_monthly['Totala_Samtal_Manad_Typ'])
            df_peak_monthly['Procent_Av_Manad_Typ'] = df_peak_monthly['Procent_Av_Manad_Typ'].fillna(0)

            df_peak_monthly['Peak_Rank'] = df_peak_monthly.groupby(
                ['CustomerKey', 'månad_namn', 'Samtalstyp']
            )['Antal_Samtal_Denna_Timme'].rank(method='first', ascending=False)

            df_top_peaks = df_peak_monthly[df_peak_monthly['Peak_Rank'] <= 3].copy()
            df_top_peaks = df_top_peaks.sort_values(
                by=['CustomerKey', 'månad_namn', 'Samtalstyp', 'Peak_Rank']
            ).drop(columns=['Totala_Samtal_Manad_Typ'])

            peak_table_name = config.TABLE_NAMES.get('Monthly_Peak_Analysis', 'Dim_Customer_Monthly_Peaks')
            peak_staging_table_name = f"{peak_table_name}_STAGING"

            df_top_peaks.to_sql(peak_staging_table_name, mssql_engine, if_exists='replace', index=False, chunksize=1000)
            
            with mssql_engine.connect() as connection:
                connection.execute(text(f"""
                    BEGIN TRY
                        BEGIN TRANSACTION;
                        IF OBJECT_ID('{peak_table_name}', 'U') IS NOT NULL DROP TABLE [{peak_table_name}];
                        SELECT * INTO [{peak_table_name}] FROM [{peak_staging_table_name}];
                        COMMIT TRANSACTION;
                    END TRY
                    BEGIN CATCH
                        ROLLBACK TRANSACTION;
                        THROW;
                    END CATCH;
                """))
                connection.commit()
            
            print(f"-> KLART: '{peak_table_name}' sparad.")

    except Exception as e:
        print(f"VARNING: Kunde inte spara peak-tabellen: {e}")
        
    print("\n--- Kundsegmentering slutförd ---")

if __name__ == '__main__':
    create_and_save_segments()
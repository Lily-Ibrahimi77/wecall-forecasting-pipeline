"""
================================================================
JOBB 4: Utvärdera Prognos (SNIPER EDITION)
================================================================
 Hämtar inte bara på datum (YYYY-MM-DD) utan 
  identifierar den EXAKTA tidsstämpeln för senaste körningen.
- Ignorerar "Zombie-data" som kan ligga kvar från gamla körningar
  samma dag.
"""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import config
import sys

def evaluate_holdout_period():
    print("--- Startar Jobb 4 (SNIPER MODE) ---")
    if config.RUN_MODE != 'VALIDATION': return
    
    mssql_engine = create_engine(config.MSSQL_CONN_STR)
    start_date_str = config.VALIDATION_SETTINGS['EVALUATION_START_DATE']
    end_date_str = config.VALIDATION_SETTINGS['EVALUATION_END_DATE']

    archive_table = config.TABLE_NAMES['Forecast_Archive']
    history_table = config.TABLE_NAMES['Hourly_Aggregated_History']
    
    # 1. Hitta SENASTE EXAKTA TIDSSTÄMPELN
    print(f"-> Siktar in sig på senaste körningen...")
    try:
        # Vi hämtar MAX av hela datetime-objektet, inte bara datumsträngen
        sql_check = f"""
            SELECT MAX(ForecastRunDate) as MaxTimestamp
            FROM [{archive_table}] 
            WHERE CONVERT(date, DatumTid) BETWEEN '{start_date_str}' AND '{end_date_str}'
        """
        df_run = pd.read_sql(sql_check, mssql_engine)
        
        if df_run.empty or df_run.iloc[0]['MaxTimestamp'] is None:
            print("VARNING: Inga prognoser hittades i arkivet.")
            return
            
        # Detta är den exakta tidsstämpeln (t.ex. 2025-09-23 14:05:01.123)
        latest_timestamp = df_run.iloc[0]['MaxTimestamp']
        
        # Konvertera till sträng som SQL förstår exakt
        latest_ts_str = str(latest_timestamp)
        # Hack för att hantera pandas/sql format ibland (ta bort .000 om det behövs, men oftast ok)
        if '.' in latest_ts_str and len(latest_ts_str.split('.')[-1]) < 3:
             latest_ts_str = latest_ts_str.split('.')[0] # Fallback om formatet bråkar

        print(f"   -> Låst på exakt tid: {latest_ts_str}")
        
    except Exception as e:
        print(f"FEL vid sökning av prognos: {e}")
        return

    # 2. Hämta Prognos (Filtrera på EXAKT tidsstämpel)
    print(f"-> Hämtar prognosdata (rensar bort zombies)...")
    
    # Använd parametriserad fråga eller mycket specifik sträng för att träffa rätt
    sql_fc = f"""
        SELECT DatumTid, TjänstTyp, Prognos_Antal_Samtal 
        FROM [{archive_table}]
        WHERE ForecastRunDate = '{latest_ts_str}'
        AND CONVERT(date, DatumTid) BETWEEN '{start_date_str}' AND '{end_date_str}'
    """
    
    df_fc = pd.read_sql(sql_fc, mssql_engine)
    
    if df_fc.empty:
        print("   VARNING: Inga rader matchade exakta tidsstämpeln. Försöker med datum-sträng (Fallback)...")
        # Fallback om millisekunderna strular
        run_date_only = str(latest_timestamp).split(' ')[0]
        sql_fc = f"""
            SELECT DatumTid, TjänstTyp, Prognos_Antal_Samtal 
            FROM [{archive_table}]
            WHERE CONVERT(VARCHAR, ForecastRunDate, 23) = '{run_date_only}'
            AND CONVERT(date, DatumTid) BETWEEN '{start_date_str}' AND '{end_date_str}'
        """
        df_fc = pd.read_sql(sql_fc, mssql_engine)

    print(f"   -> Hittade {len(df_fc)} prognosrader.")
    
    df_fc['Datum'] = pd.to_datetime(df_fc['DatumTid']).dt.normalize()
    df_fc['Key'] = df_fc['TjänstTyp'].astype(str).str.strip().str.lower()
    
    # Aggregera prognos
    df_fc_agg = df_fc.groupby(['Datum', 'Key'])['Prognos_Antal_Samtal'].sum().reset_index()

    # 3. Hämta Facit
    print(f"-> Hämtar facit från {history_table}...")
    sql_act = f"""
        SELECT ds, Tj_nstTyp, Antal_Samtal 
        FROM [{history_table}] 
        WHERE CONVERT(date, ds) BETWEEN '{start_date_str}' AND '{end_date_str}'
    """
    df_act = pd.read_sql(sql_act, mssql_engine)
    df_act['Datum'] = pd.to_datetime(df_act['ds']).dt.normalize()
    df_act['Key'] = df_act['Tj_nstTyp'].astype(str).str.strip().str.lower()
    
    df_act_agg = df_act.groupby(['Datum', 'Key'])['Antal_Samtal'].sum().reset_index()

    # 4. Jämför
    df_merged = pd.merge(df_act_agg, df_fc_agg, on=['Datum', 'Key'], how='inner', suffixes=('_act', '_fc'))
    
    total_act = df_merged['Antal_Samtal'].sum()
    total_fc = df_merged['Prognos_Antal_Samtal'].sum()
    
    if total_act == 0:
        print("Ingen matchning mellan prognos och verklighet (kontrollera tjänstenamn).")
        return

    df_merged['AbsErr'] = (df_merged['Antal_Samtal'] - df_merged['Prognos_Antal_Samtal']).abs()
    wmape = (df_merged['AbsErr'].sum() / total_act) * 100
    accuracy = max(0, 100 - wmape)

    print(f"\nRESULTAT ({start_date_str} - {end_date_str}):")
    print("="*40)
    print(f"Körning ID (Tid):   {latest_ts_str}")
    print("-" * 20)
    print(f"Verklig Volym:      {int(total_act)}")
    print(f"Prognos Volym:      {int(total_fc)}")
    print(f"Diff (Antal):       {int(total_fc - total_act)}")
    print("-" * 20)
    print(f"wMAPE (Felprocent): {wmape:.2f}%")
    print(f"Träffsäkerhet:      {accuracy:.2f}%")
    print("="*40)

if __name__ == '__main__':
    evaluate_holdout_period()
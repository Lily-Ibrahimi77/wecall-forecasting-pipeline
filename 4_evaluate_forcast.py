"""
================================================================
JOBB 4: Utvärdera Prognos (ALL-IN-ONE: SNIPER + GRAFIK)
================================================================
Kombinerar det bästa av två världar:
1. SNIPER MODE: Hämtar exakt rätt data (inga dubbletter).
2. FAIR METRICS: Räknar wMAPE på Daglig Total (Rättvis för budget).
3. VISUALS: Skapar trend- och stapelgrafer direkt.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sqlalchemy import create_engine
import config
import sys
import os

# Snyggare grafer
plt.style.use('ggplot')
COLORS = {'Actual': '#2E86C1', 'Forecast': '#E67E22'}

def evaluate_and_plot():
    print("--- Startar Jobb 4 (ALL-IN-ONE) ---")
    if config.RUN_MODE != 'VALIDATION': 
        print("OBS: Körs ej i PRODUCTION mode.")
        return
    
    mssql_engine = create_engine(config.MSSQL_CONN_STR)
    
    # Hämta datum från config
    start_date_str = config.VALIDATION_SETTINGS['EVALUATION_START_DATE']
    end_date_str = config.VALIDATION_SETTINGS['EVALUATION_END_DATE']
    
    print(f"   Period: {start_date_str} till {end_date_str}")

    archive_table = config.TABLE_NAMES['Forecast_Archive']
    history_table = config.TABLE_NAMES['Hourly_Aggregated_History']
    
    # ---------------------------------------------------------
    # 1. SNIPER MODE: Hitta exakt tidsstämpel
    # ---------------------------------------------------------
    print(f"-> 1. Siktar in sig på senaste körningen...")
    try:
        sql_check = f"""
            SELECT MAX(ForecastRunDate) as MaxTimestamp
            FROM [{archive_table}] 
            WHERE CONVERT(date, DatumTid) BETWEEN '{start_date_str}' AND '{end_date_str}'
        """
        df_run = pd.read_sql(sql_check, mssql_engine)
        
        if df_run.empty or df_run.iloc[0]['MaxTimestamp'] is None:
            print("   VARNING: Inga prognoser hittades för perioden.")
            return

        latest_ts = df_run.iloc[0]['MaxTimestamp']
        # Konvertera till sträng för SQL-matchning
        latest_ts_str = str(latest_timestamp_clean(latest_ts))
        print(f"   -> Låst på körning: {latest_ts_str}")
        
    except Exception as e:
        print(f"   FEL vid sökning: {e}")
        return

    # ---------------------------------------------------------
    # 2. HÄMTA DATA (Aggregerat per DAG för rättvis wMAPE)
    # ---------------------------------------------------------
    print(f"-> 2. Hämtar Prognos & Facit (Daglig nivå)...")
    
    # PROGNOS (Filtrerad på Sniper-tid)
    sql_fc = f"""
        SELECT CAST(DatumTid AS DATE) as Datum, SUM(Prognos_Antal_Samtal) as Forecast_Volym 
        FROM [{archive_table}]
        WHERE ForecastRunDate = '{latest_ts_str}'
        AND CONVERT(date, DatumTid) BETWEEN '{start_date_str}' AND '{end_date_str}'
        GROUP BY CAST(DatumTid AS DATE)
    """
    
    # FACIT
    sql_act = f"""
        SELECT CAST(ds AS DATE) as Datum, SUM(Antal_Samtal) as Actual_Volym 
        FROM [{history_table}] 
        WHERE CONVERT(date, ds) BETWEEN '{start_date_str}' AND '{end_date_str}'
        GROUP BY CAST(ds AS DATE)
    """
    
    try:
        df_fc = pd.read_sql(sql_fc, mssql_engine)
        # Fallback om exakt timestamp strular (ibland är SQL kinkig med millisekunder)
        if df_fc.empty:
            print("   (Sniper missade millisekunder, testar datum-matchning...)")
            run_date_only = str(latest_ts).split(' ')[0]
            sql_fc = sql_fc.replace(f"ForecastRunDate = '{latest_ts_str}'", f"CONVERT(VARCHAR, ForecastRunDate, 23) = '{run_date_only}'")
            df_fc = pd.read_sql(sql_fc, mssql_engine)

        df_act = pd.read_sql(sql_act, mssql_engine)
        
        # Konvertera datum
        df_fc['Datum'] = pd.to_datetime(df_fc['Datum'])
        df_act['Datum'] = pd.to_datetime(df_act['Datum'])
        
    except Exception as e:
        print(f"   FEL vid datahämtning: {e}")
        return

    # ---------------------------------------------------------
    # 3. JÄMFÖR & RÄKNA (Fair Metrics)
    # ---------------------------------------------------------
    df_merged = pd.merge(df_act, df_fc, on='Datum', how='outer').fillna(0).sort_values('Datum')
    
    total_act = df_merged['Actual_Volym'].sum()
    total_fc = df_merged['Forecast_Volym'].sum()
    diff = total_fc - total_act
    
    # Rättvis wMAPE (Daglig nivå)
    df_merged['AbsError'] = (df_merged['Actual_Volym'] - df_merged['Forecast_Volym']).abs()
    
    if total_act > 0:
        wmape = (df_merged['AbsError'].sum() / total_act) * 100
        accuracy = 100 - wmape
    else:
        wmape = 0
        accuracy = 0

    print(f"\nRESULTAT ({start_date_str} - {end_date_str}):")
    print("="*50)
    print(f"Verklig Volym:      {int(total_act)}")
    print(f"Prognos Volym:      {int(total_fc)}")
    print(f"Diff (Antal):       {int(diff)} ({diff/total_act:+.1%})")
    print("-" * 30)
    print(f"wMAPE (Rättvis):    {wmape:.2f}%  <-- Denna siffra ska till rapporten!")
    print(f"Träffsäkerhet:      {accuracy:.2f}%")
    print("="*50)

    # ---------------------------------------------------------
    # 4. SKAPA GRAFER (Engelska för rapporten)
    # ---------------------------------------------------------
    print("-> 4. Genererar grafer...")
    
    # Graf 1: Trend
    plt.figure(figsize=(10, 6))
    plt.plot(df_merged['Datum'], df_merged['Actual_Volym'], label='Actuals', color=COLORS['Actual'], marker='o', linewidth=3)
    plt.plot(df_merged['Datum'], df_merged['Forecast_Volym'], label='AI Forecast', color=COLORS['Forecast'], marker='o', linestyle='--', linewidth=3)
    
    plt.title(f"Daily Forecast Accuracy: {start_date_str} - {end_date_str}", fontsize=16)
    plt.ylabel("Call Volume", fontsize=12)
    plt.xlabel("Date", fontsize=12)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('Rapport_Figur_1_Trend.png', dpi=300)
    
    # Graf 2: Total
    plt.figure(figsize=(7, 6))
    bars = plt.bar(['Actuals', 'AI Forecast'], [total_act, total_fc], color=[COLORS['Actual'], COLORS['Forecast']], width=0.6)
    
    for i, bar in enumerate(bars):
        height = bar.get_height()
        label = f"{int(height)}"
        if i == 1: 
            diff_pct = (total_fc / total_act) - 1
            label += f"\n({diff_pct:+.1%})"
        plt.text(bar.get_x() + bar.get_width()/2, height/2, label, ha='center', va='center', fontsize=14, fontweight='bold', color='white')
        
    plt.title(f"Total Volume & wMAPE ({wmape:.1f}%)", fontsize=16)
    plt.ylabel("Total Call Volume")
    plt.tight_layout()
    plt.savefig('Rapport_Figur_2_Total.png', dpi=300)
    
    print("-> KLART! Bilder sparade: 'Rapport_Figur_1_Trend.png' & 'Rapport_Figur_2_Total.png'")

# Hjälpfunktion för att städa tidsstämpel
def latest_timestamp_clean(ts):
    str_ts = str(ts)
    if '.' in str_ts and len(str_ts.split('.')[-1]) < 3:
        return str_ts.split('.')[0]
    return str_ts

if __name__ == '__main__':
    evaluate_and_plot()
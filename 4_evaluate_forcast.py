"""
================================================================
JOBB 4: Utvärdera Prognos (Feedback-loop / MLOps)
================================================================
*** UPPDATERAD (ROBUST VERSION) ***
- Styrs av config.RUN_MODE och utvärderar per SEGMENT.
- Hämtar data robust utan att krascha om tabeller saknas.
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import config
import sys
import traceback
from DataDriven_utils import get_current_time

def evaluate_holdout_period():
    print("--- Startar Jobb 4: Utvärdering av HOLD-OUT-period ---")

    if config.RUN_MODE != 'VALIDATION':
        print(f"-> INFO: 'RUN_MODE' är satt till '{config.RUN_MODE}'.")
        print("-> Utvärderings-skriptet körs endast när 'RUN_MODE' är 'VALIDATION'. Avslutar.")
        return
    
    print("*** VALIDATION MODE AKTIVT ***")
    
    try:
        mssql_engine = create_engine(config.MSSQL_CONN_STR)
        print("-> Ansluten till MSSQL Data Warehouse.")
    except Exception as e:
        print(f"FATALT FEL: Kunde inte ansluta till MSSQL: {e}")
        sys.exit(1)

    # === STEG 1: Hämta datum från config ===
    try:
        settings = config.VALIDATION_SETTINGS
        FORECAST_RUN_DATE_SQL = settings['FORECAST_RUN_DATE_SQL']
        START_DATE_SQL = settings['EVALUATION_START_DATE']
        END_DATE_SQL = settings['EVALUATION_END_DATE']
    except Exception as e:
        print(f"FATALT FEL: Kunde inte läsa 'VALIDATION_SETTINGS' från config.py. Fel: {e}")
        sys.exit(1)

    print(f"-> Utvärderar prognos för perioden: {START_DATE_SQL} till {END_DATE_SQL}")
    print(f"-> (Jämför med prognos körd: {FORECAST_RUN_DATE_SQL})")

    # === STEG 2: Hämta VERKLIGT utfall (Facit) ===
    actuals_table = config.TABLE_NAMES['Hourly_Aggregated_History']
    try:
        # KORRIGERING: Hämta per SEGMENT
        sql_actuals = f"""
            SELECT ds, Behavior_Segment, Antal_Samtal 
            FROM [{actuals_table}]
            WHERE CONVERT(date, ds) BETWEEN '{START_DATE_SQL}' AND '{END_DATE_SQL}'
        """
        df_actuals = pd.read_sql(sql_actuals, mssql_engine)
        df_actuals['ds'] = pd.to_datetime(df_actuals['ds']).dt.tz_localize(None)

        if df_actuals.empty:
            print(f"VARNING: Inga VERKLIGA data hittades i '{actuals_table}' för perioden.")
            print("   -> Har du kört hela pipelinen (Fil 2) korrekt så att tabellen skapades?")
            return
        print(f"-> Hämtade {len(df_actuals)} verkliga tim-värden.")
    except Exception as e:
        print(f"FEL: Kunde inte hämta verkliga data från '{actuals_table}'.")
        print(f"Tekniskt fel: {e}")
        return

    # === STEG 3: Hämta PROGNOS ===
    archive_table = config.TABLE_NAMES['Forecast_Archive']
    try:
        prognos_kolumn = "Prognos_Antal_Samtal" # (Median-prognosen)
        sql_forecast = f"""
            SELECT 
                ds, Behavior_Segment, 
                {prognos_kolumn} AS Prognos_Att_Jamfora
            FROM [{archive_table}]
            WHERE 
                CONVERT(date, ForecastRunDate) = '{FORECAST_RUN_DATE_SQL}' 
                AND CONVERT(date, ds) BETWEEN '{START_DATE_SQL}' AND '{END_DATE_SQL}'
        """
        df_forecast = pd.read_sql(sql_forecast, mssql_engine)
        df_forecast['ds'] = pd.to_datetime(df_forecast['ds']).dt.tz_localize(None)

        if df_forecast.empty:
            print("VARNING: Inga PROGNOS-data hittades för perioden. Kan inte utvärdera.")
            print(f"   -> Kontrollera att det finns en prognos i '{archive_table}' med ForecastRunDate = '{FORECAST_RUN_DATE_SQL}'.")
            return
            
        print(f"-> Hämtade {len(df_forecast)} prognos-värden.")
    except Exception as e:
        print(f"FEL: Kunde inte hämta prognosdata från '{archive_table}': {e}")
        return

    # === STEG 4: Jämför och beräkna fel ===
    print("-> Jämför verkligt utfall med prognos...")
    
    df_merged = pd.merge(
        df_actuals,
        df_forecast,
        on=['ds', 'Behavior_Segment'],
        how='outer' 
    )
    
    df_merged['Antal_Samtal'] = df_merged['Antal_Samtal'].fillna(0)
    df_merged['Prognos_Att_Jamfora'] = df_merged['Prognos_Att_Jamfora'].fillna(0)
    
    df_merged['Error'] = df_merged['Antal_Samtal'] - df_merged['Prognos_Att_Jamfora']
    df_merged['Abs_Error'] = df_merged['Error'].abs()
    df_merged['Abs_Pct_Error'] = (df_merged['Abs_Error'] / df_merged['Antal_Samtal']).replace(np.inf, 0)
    
    # === STEG 5: Beräkna totala KPI:er ===
    total_actual = df_merged['Antal_Samtal'].sum()
    total_forecast = df_merged['Prognos_Att_Jamfora'].sum()
    total_abs_error = df_merged['Abs_Error'].sum()
    
    # Undvik division med noll
    mape = 0
    if len(df_merged[df_merged['Antal_Samtal'] > 0]) > 0:
        mape = df_merged[df_merged['Antal_Samtal'] > 0]['Abs_Pct_Error'].mean()
        
    rmse = np.sqrt(np.mean(df_merged['Error']**2))
    
    print("-" * 30)
    print(f"UTVÄRDERING (för {START_DATE_SQL} till {END_DATE_SQL}):")
    print(f"  Verkligt antal samtal: {total_actual}")
    print(f"  Prognos antal samtal: {total_forecast}")
    print(f"  Totalt absolut fel:    {total_abs_error}")
    print(f"  MAPE (på tim-nivå):    {mape:.2%}")
    print(f"  RMSE (på tim-nivå):    {rmse:.2f}")
    print("-" * 30)
    
    # === STEG 6: Spara loggen till MSSQL (SÄKRAD) ===
    log_table_name = config.TABLE_NAMES['Forecast_Performance']
    try:
        df_log = pd.DataFrame({
            'LogRunDate': [get_current_time()],
            'ForecastDate': [f"{START_DATE_SQL}_till_{END_DATE_SQL}"],
            'MAPE': [mape], 'RMSE': [rmse], 'TotalActual': [total_actual],
            'TotalForecast': [total_forecast], 'TotalAbsError': [total_abs_error]
        })
        
        # Pandas 'append' sköter oftast transaktionen själv, men vi säkrar upp.
        df_log.to_sql(log_table_name, mssql_engine, if_exists='append', index=False)
        print(f"-> Prestanda-logg sparad till '{log_table_name}'.")
        
    except Exception as e:
        print(f"FEL: Kunde inte spara prestanda-logg: {e}")

if __name__ == '__main__':
    evaluate_holdout_period()
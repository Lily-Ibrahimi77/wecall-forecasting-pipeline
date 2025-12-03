"""
================================================================
JOBB 5: Generate Report Visuals (Final Version)
================================================================
Data: 
  - Forecast: 'Frcast_Operative_Calls_By_Service'
  - Actuals: 'Fact_Hourly_Aggregated_History'
Period: hämtas från config validation mode
Output: KPIs (wMAPE) + Graphs (.png)
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sqlalchemy import create_engine
import config
import os

# Snyggare grafer
plt.style.use('ggplot')
# Färger: Actual (Blå), Forecast (Orange)
COLORS = {'Actual': '#2E86C1', 'Forecast': '#E67E22'} 

def generate_report_evidence():
    print("--- GENERATING REPORT EVIDENCE (FROM FACT TABLES) ---")
    
    try:
        mssql_engine = create_engine(config.MSSQL_CONN_STR)
    except Exception as e:
        print(f"Connection error: {e}")
        return

    # Hämta datum dynamiskt från config istället för att hårdkoda
    start_date = config.VALIDATION_SETTINGS['EVALUATION_START_DATE']
    end_date = config.VALIDATION_SETTINGS['EVALUATION_END_DATE']
    
    # ---------------------------------------------------------
    # 1. GET FORECAST
    # ---------------------------------------------------------
    table_forecast = config.TABLE_NAMES['Forecast_Archive']
    print(f"1. Fetching FORECAST from {table_forecast}...")
    
    q_fc = f"""
        SELECT 
            CAST(DatumTid AS DATE) as Datum,
            SUM(Prognos_Antal_Samtal) as Forecast_Volym
        FROM [{table_forecast}]
        WHERE CAST(DatumTid AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY CAST(DatumTid AS DATE)
    """
    df_fc = pd.read_sql(q_fc, mssql_engine)
    df_fc['Datum'] = pd.to_datetime(df_fc['Datum'])
    
    # ---------------------------------------------------------
    # 2. GET ACTUALS (FACIT)
    # ---------------------------------------------------------
    table_history = config.TABLE_NAMES['Hourly_Aggregated_History']
    print(f"2. Fetching ACTUALS from {table_history}...")
    
    q_act = f"""
        SELECT 
            CAST(ds AS DATE) as Datum,
            SUM(Antal_Samtal) as Actual_Volym
        FROM [{table_history}]
        WHERE CAST(ds AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY CAST(ds AS DATE)
    """
    df_act = pd.read_sql(q_act, mssql_engine)
    df_act['Datum'] = pd.to_datetime(df_act['Datum'])

    # ---------------------------------------------------------
    # 3. MERGE & CALCULATE KPI
    # ---------------------------------------------------------
    print("3. Calculating KPIs...")
    df_merged = pd.merge(df_act, df_fc, on='Datum', how='outer').fillna(0)
    df_merged = df_merged.sort_values('Datum')
    
    total_act = df_merged['Actual_Volym'].sum()
    total_fc = df_merged['Forecast_Volym'].sum()
    diff = total_fc - total_act
    
    # wMAPE (Weighted Mean Absolute Percentage Error)
    df_merged['AbsError'] = (df_merged['Actual_Volym'] - df_merged['Forecast_Volym']).abs()
    
    if total_act > 0:
        wmape = (df_merged['AbsError'].sum() / total_act) * 100
        accuracy = 100 - wmape
    else:
        wmape = 0
        accuracy = 0

    print("\n" + "="*50)
    print(f" REPORT DATA: {start_date} to {end_date}")
    print("="*50)
    print(f" Actual Total:     {int(total_act)}")
    print(f" Forecast Total:   {int(total_fc)}")
    print(f" Difference:       {int(diff)} ({diff/total_act:+.1%})")
    print("-" * 30)
    print(f" wMAPE (Error):    {wmape:.2f}%")
    print(f" Accuracy:         {accuracy:.2f}%")
    print("="*50 + "\n")

    # ---------------------------------------------------------
    # 4. CREATE GRAPHS (ENGLISH)
    # ---------------------------------------------------------
    print("4. Creating graphs...")
    
    # Graf 1: Daglig Trend (Linje)
    plt.figure(figsize=(10, 6))
    
    # ÄNDRAT HÄR: Labels till engelska ('Actuals', 'AI Forecast')
    plt.plot(df_merged['Datum'], df_merged['Actual_Volym'], label='Actuals', color=COLORS['Actual'], marker='o', linewidth=3)
    plt.plot(df_merged['Datum'], df_merged['Forecast_Volym'], label='AI Forecast', color=COLORS['Forecast'], marker='o', linestyle='--', linewidth=3)
    
    # ÄNDRAT HÄR: Titlar och axlar till engelska
    plt.title(f"Daily Forecast Accuracy: {start_date} - {end_date}", fontsize=16)
    plt.ylabel("Call Volume", fontsize=12)
    plt.xlabel("Date", fontsize=12)
    plt.legend(fontsize=12)
    plt.grid(True, alpha=0.3)
    
    # Snygga datum på X-axeln
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator())
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig('Rapport_Figur_1_Trend.png', dpi=300)
    print("   -> Saved 'Rapport_Figur_1_Trend.png'")

    # Graf 2: Totaljämförelse (Stapel)
    plt.figure(figsize=(7, 6))
    
    # ÄNDRAT HÄR: 'Actuals' istället för 'Verkligt', 'AI Forecast' istället för 'AI Prognose'
    bars = plt.bar(['Actuals', 'AI Forecast'], [total_act, total_fc], color=[COLORS['Actual'], COLORS['Forecast']], width=0.6)
    
    # Lägg till siffror och % på staplarna
    for i, bar in enumerate(bars):
        height = bar.get_height()
        label = f"{int(height)}"
        if i == 1: # På prognos-stapeln, lägg till felprocent
            diff_pct = (total_fc / total_act) - 1
            label += f"\n({diff_pct:+.1%})"
            
        plt.text(bar.get_x() + bar.get_width()/2, height/2, label, ha='center', va='center', fontsize=14, fontweight='bold', color='white')
        
    # ÄNDRAT HÄR: Engelsk titel och Y-axel
    plt.title(f"Total Volume & wMAPE ({wmape:.1f}%)", fontsize=16)
    plt.ylabel("Total Call Volume", fontsize=12)
    
    plt.tight_layout()
    plt.savefig('Rapport_Figur_2_Total.png', dpi=300)
    print("   -> Saved 'Rapport_Figur_2_Total.png'")

    print("\nDONE! Use the generated images in your report.")

if __name__ == '__main__':
    generate_report_evidence()
"""
================================================================
JOBB 5: Skapa Rapportunderlag (Slutgiltig Version)
================================================================
Data: 
  - Prognos: 'Frcast_Operative_Calls_By_Service' (Lager 1 Output)
  - Verklighet: 'Fact_Hourly_Aggregated_History' (Facit)
Period: 2025-09-24 till 2025-09-30
Output: KPI:er (wMAPE) + Grafer (.png)
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sqlalchemy import create_engine
import config
import os



# grafer
plt.style.use('ggplot')
COLORS = {'Actual': '#2E86C1', 'Forecast': '#E67E22'} 

def generate_report_evidence():
    print("--- GENERERAR RAPPORT-UNDERLAG (FRÅN FACT TABLES) ---")
    
    try:
        mssql_engine = create_engine(config.MSSQL_CONN_STR)
    except Exception as e:
        print(f"Fel vid anslutning: {e}")
        return

    start_date = '2025-09-24'
    end_date = '2025-09-30'
    
    # ---------------------------------------------------------
    # 1. HÄMTA PROGNOS
    # ---------------------------------------------------------
    table_forecast = config.TABLE_NAMES['Forecast_Archive'] # Frcast_Operative_Calls_By_Service
    print(f"1. Hämtar PROGNOS från {table_forecast}...")
    
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
    # 2. HÄMTA VERKLIGHET (FACIT)
    # ---------------------------------------------------------
    table_history = config.TABLE_NAMES['Hourly_Aggregated_History'] # Fact_Hourly_Aggregated_History
    print(f"2. Hämtar FACIT från {table_history}...")
    
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
    # 3. JÄMFÖR & RÄKNA KPI
    # ---------------------------------------------------------
    print("3. Beräknar KPI:er...")
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
    print(f" RAPPORT-DATA: {start_date} till {end_date}")
    print("="*50)
    print(f" Verkligt Antal:   {int(total_act)}")
    print(f" Prognos Antal:    {int(total_fc)}")
    print(f" Differens:        {int(diff)} ({diff/total_act:+.1%})")
    print("-" * 30)
    print(f" wMAPE (Fel):      {wmape:.2f}%")
    print(f" Träffsäkerhet:    {accuracy:.2f}%")
    print("="*50 + "\n")

    # ---------------------------------------------------------
    # 4. SKAPA GRAFER TILL RAPPORTEN
    # ---------------------------------------------------------
    print("4. Skapar grafer...")
    
    # Graf 1: Daglig Trend (Linje)
    plt.figure(figsize=(10, 6))
    plt.plot(df_merged['Datum'], df_merged['Actual_Volym'], label='Verkligt Utfall', color=COLORS['Actual'], marker='o', linewidth=3)
    plt.plot(df_merged['Datum'], df_merged['Forecast_Volym'], label='AI Prognos', color=COLORS['Forecast'], marker='o', linestyle='--', linewidth=3)
    
    plt.title(f"Daglig Träffsäkerhet: {start_date} - {end_date}", fontsize=16)
    plt.ylabel("Antal Samtal", fontsize=12)
    plt.xlabel("Datum", fontsize=12)
    plt.legend(fontsize=12)
    plt.grid(True, alpha=0.3)
    
    # datum på X-axeln
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator())
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig('Rapport_Figur_1_Trend.png', dpi=300)
    print("   -> Sparade 'Rapport_Figur_1_Trend.png'")

    # Graf 2: Totaljämförelse (Stapel)
    plt.figure(figsize=(7, 6))
    bars = plt.bar(['Verkligt', 'AI Prognos'], [total_act, total_fc], color=[COLORS['Actual'], COLORS['Forecast']], width=0.6)
    
    # Lägg till siffror och % på staplarna
    for i, bar in enumerate(bars):
        height = bar.get_height()
        label = f"{int(height)}"
        if i == 1: 
            diff_pct = (total_fc / total_act) - 1
            label += f"\n({diff_pct:+.1%})"
            
        plt.text(bar.get_x() + bar.get_width()/2, height/2, label, ha='center', va='center', fontsize=14, fontweight='bold', color='white')
        
    plt.title(f"Total Volym & wMAPE ({wmape:.1f}%)", fontsize=16)
    plt.ylabel("Totalt Antal Samtal", fontsize=12)
    plt.tight_layout()
    plt.savefig('Rapport_Figur_2_Total.png', dpi=300)
    print("   -> Sparade 'Rapport_Figur_2_Total.png'")

    print("\nKLART! Använd bilderna och siffrorna i din rapport.")

if __name__ == '__main__':
    generate_report_evidence()
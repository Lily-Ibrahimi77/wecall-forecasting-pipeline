import pandas as pd
from sqlalchemy import create_engine
import config

def debug_archive():
    print("--- DIAGNOS AV DATABASEN ---")
    engine = create_engine(config.MSSQL_CONN_STR)
    table = config.TABLE_NAMES['Forecast_Archive']
    
    print(f"Läser från tabell: {table}\n")
    
    # Se alla unika körningar och deras totala volym
    sql = f"""
    SELECT 
        ForecastRunDate, 
        COUNT(*) as Antal_Rader,
        SUM(Prognos_Antal_Samtal) as Total_Volym
    FROM [{table}]
    GROUP BY ForecastRunDate
    ORDER BY ForecastRunDate DESC
    """
    
    try:
        df = pd.read_sql(sql, engine)
        print(df)
        print("\n------------------------------------------------")
        print("OM DU SER FLERA RADER MED SAMMA DATUM (t.ex. olika klockslag)")
        print("SÅ ÄR DETTA ORSAKEN TILL DUBBLETTERNA.")
        print("------------------------------------------------")
    except Exception as e:
        print(f"Kunde inte läsa: {e}")

if __name__ == "__main__":
    debug_archive()
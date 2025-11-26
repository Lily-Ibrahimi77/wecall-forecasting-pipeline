"""
================================================================
JOBB 0: Ladda Bronze-lagret (0_Load_Bronze_Data.py)
================================================================
Syfte: Kopiera ALLA rådata-tabeller från MariaDB till MSSQL (Bronze).
Strategi:
  - Stora tabeller (CDR, Cases): Inkrementell laddning (baserat på Created).
  - Små tabeller (Customers, Queues, Users): Full laddning (Truncate/Insert).
"""

import pandas as pd
from sqlalchemy import create_engine, text
import config
import sys
import traceback

# Konfiguration för Bronze-laddning
BRONZE_JOBS = [
    # --- STORA TABELLER (Inkrementell) ---
    {
        "source_db_conn": config.QUEUE_DB_CONN_STR,
        "source_table": "queue_cdr",
        "target_table": "Bronze_Queue_CDR",
        "load_type": "INCREMENTAL",
        "time_col": "Created"
    },
    {
        "source_db_conn": config.CASE_DB_CONN_STR,
        "source_table": "cases",
        "target_table": "Bronze_Cases",
        "load_type": "INCREMENTAL",
        "time_col": "Created"
    },
    
    # --- SMÅ TABELLER (Full Load - Ersätt allt) ---
    {
        "source_db_conn": config.BILLING_DB_CONN_STR,
        "source_table": "customers",
        "target_table": "Bronze_Billing_Customers",
        "load_type": "FULL"
    },
    {
        "source_db_conn": config.QUEUE_DB_CONN_STR,
        "source_table": "queuegroups",
        "target_table": "Bronze_Queue_Groups",
        "load_type": "FULL"
    },
    {
        "source_db_conn": config.CASE_DB_CONN_STR,
        "source_table": "users",
        "target_table": "Bronze_Case_Users",
        "load_type": "FULL"
    }
]

def sync_bronze_layer():
    print("--- Startar Jobb 0: Synkronisera Bronze-lager (Multi-Table) ---")

    try:
        mssql_engine = create_engine(config.MSSQL_CONN_STR)
        print("-> Ansluten till MSSQL (Mål).")
    except Exception as e:
        print(f"FATALT FEL: Kunde inte ansluta till MSSQL: {e}")
        sys.exit(1)

    for job in BRONZE_JOBS:
        target_table = job['target_table']
        source_table = job['source_table']
        load_type = job['load_type']
        
        print(f"\n-> Bearbetar: {source_table} -> {target_table} ({load_type})...")

        try:
            # Anslut till specifik källdatabas för detta jobb
            source_engine = create_engine(job['source_db_conn'])
            
            if load_type == 'INCREMENTAL':
                # 1. Hitta sista datum i Bronze
                time_col = job['time_col']
                last_date = None
                
                try:
                    check_sql = f"SELECT MAX({time_col}) as last_entry FROM [{target_table}]"
                    # Obs: Detta kastar fel om tabellen inte finns, vilket vi fångar
                    df_max = pd.read_sql(check_sql, mssql_engine)
                    if not df_max.empty and pd.notna(df_max.iloc[0]['last_entry']):
                        last_date = df_max.iloc[0]['last_entry']
                        print(f"   -> Senaste data i Bronze: {last_date}")
                except:
                    print("   -> Tabellen finns inte eller är tom. Kör full historik.")

                # 2. Hämta data nyare än sista datum
                if last_date:
                    query = f"SELECT * FROM {source_table} WHERE {time_col} > '{last_date}'"
                else:
                    query = f"SELECT * FROM {source_table}"

                # 3. Spara (Append)
                rows_count = 0
                for chunk in pd.read_sql(query, source_engine, chunksize=50000):
                    chunk.to_sql(target_table, mssql_engine, if_exists='append', index=False)
                    rows_count += len(chunk)
                    print(f"   -> Laddat {len(chunk)} rader...")
                
                if rows_count == 0:
                    print("   -> Inga nya rader.")
                else:
                    print(f"   -> KLART! Totalt {rows_count} nya rader.")

            elif load_type == 'FULL':
                # För små register: Hämta allt och skriv över
                query = f"SELECT * FROM {source_table}"
                df_full = pd.read_sql(query, source_engine)
                
                if not df_full.empty:
                    df_full.to_sql(target_table, mssql_engine, if_exists='replace', index=False)
                    print(f"   -> KLART! Ersatte tabellen med {len(df_full)} rader.")
                else:
                    print("   -> VARNING: Källtabellen var tom.")

        except Exception as e:
            print(f"FEL vid synk av {target_table}: {e}")
            traceback.print_exc()
            # fortsätter till nästa tabell även om en misslyckas

    print("\n--- Bronze-laddning slutförd ---")

if __name__ == '__main__':
    sync_bronze_layer()
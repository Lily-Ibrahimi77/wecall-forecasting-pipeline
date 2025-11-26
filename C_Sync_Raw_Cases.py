"""
================================================================
JOBB 4: Synka Agent-data (C_Sync_Raw_Cases.py)
================================================================
- Utför flytten helt internt i MSSQL (Bronze -> Fact).
- Inkluderar 'QueueId'.
- Filtrerar BORT exkluderade köer (enligt config).
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime            
from dateutil.relativedelta import relativedelta 
import config
import traceback
import sys

def get_last_date_from_source(engine):
    try:
        #  baserar tiden på samtalsdata (Bronze CDR) för att hålla det synkat
        bronze_cdr = config.BRONZE_TABLES['cdr']
        query = f"SELECT MAX(Created) as last_date FROM [{bronze_cdr}]"
        df = pd.read_sql(query, engine)
        if not df.empty and pd.notna(df.iloc[0]['last_date']):
            # Returnera dagen EFTER sista datan
            return pd.to_datetime(df.iloc[0]['last_date']) + relativedelta(days=1)
    except: pass
    return datetime(2025, 10, 10)

def sync_raw_cases_for_pbi():
    print("--- Startar SNABB synkronisering av Cases (SQL-Native) ---")
    
    try:
        mssql_engine = create_engine(config.MSSQL_CONN_STR)
        
        # 1. Räkna ut datum
        today = get_last_date_from_source(mssql_engine) 
        if config.RUN_MODE == 'VALIDATION':
            # Om Validation, tvinga datum till 1 okt
            today = pd.to_datetime(config.VALIDATION_SETTINGS['TRAINING_END_DATE']) + relativedelta(days=1)

        end_date_dt = today.replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta(seconds=1)
        start_date_dt = (end_date_dt + relativedelta(seconds=1) - relativedelta(months=config.OPERATIONAL_MONTHS_AGO)).replace(day=1)
        
        start_date_sql = start_date_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_date_sql = end_date_dt.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"-> Datumperiod: {start_date_sql} till {end_date_sql}")

        # 2. Förbered variabler
        exclude_queues_str = ", ".join([f"'{str(qid)}'" for qid in config.EXCLUDE_QUEUE_IDS])
        
        bronze_cases = config.BRONZE_TABLES['cases']
        bronze_users = config.BRONZE_TABLES['users']
        target_table = config.TABLE_NAMES.get("Raw_Cases", "Fact_Cases")
        
        print(f"-> Kör intern SQL-transformering ({bronze_cases} -> {target_table})...")

        # 3. Körs helt i databasen
        sql_transaction = f"""
        IF OBJECT_ID('{target_table}', 'U') IS NOT NULL DROP TABLE [{target_table}];

        SELECT 
            c.CaseId, 
            c.Status, 
            c.Created, 
            c.InternalType, 
            c.UserId, 
            u.Name AS AgentName,
            c.GroupId AS QueueId
        INTO [{target_table}]
        FROM [{bronze_cases}] AS c
        LEFT JOIN [{bronze_users}] AS u ON c.UserId = u.UserId
        WHERE 
            c.InternalType = '{config.CALL_CHANNEL_NAME}'
            AND c.Created BETWEEN '{start_date_sql}' AND '{end_date_sql}'
            AND c.GroupId NOT IN ({exclude_queues_str});
        """

        # 4. Utför (Execute & Commit)
        with mssql_engine.connect() as connection:
            connection.execute(text(sql_transaction))
            connection.commit()

        print(f"-> KLART! Tabellen '{target_table}' är återskapad och fylld (inkl. QueueId).")

    except Exception as e:
        print(f"FEL vid synkning: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    sync_raw_cases_for_pbi()
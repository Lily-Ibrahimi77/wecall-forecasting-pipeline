"""
================================================================
JOBB 1: Extrahera Data (FRÅN BRONZE)
================================================================
*** UPPDATERAD FÖR BRONZE & STABILITET ***
- Läser från MSSQL (Bronze) istället för MariaDB.
- Använder CAST(Created AS TIME) för att fungera med MSSQL T-SQL.
- Innehåller COMMIT och AUTO-CREATE för tabeller.
- Inkluderar CaseId och LandingNumber.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text 
from datetime import datetime
from dateutil.relativedelta import relativedelta
import config
from DataDriven_utils import map_queue_to_service, get_customer_data
import sys
import traceback
import numpy as np 

def get_last_date_from_source(engine):
    """ Hämtar sista datum från BRONZE-tabellen i MSSQL """
    try:
        # Använder namnet från config.BRONZE_TABLES
        bronze_cdr = config.BRONZE_TABLES['cdr']
        query = f"SELECT MAX(Created) as last_date FROM [{bronze_cdr}]"
        
        df_last_date = pd.read_sql(query, engine)
        if not df_last_date.empty and pd.notna(df_last_date.iloc[0]['last_date']):
            last_date = pd.to_datetime(df_last_date.iloc[0]['last_date']).tz_localize(None)
            pseudo_today = last_date.replace(hour=0, minute=0, second=0, microsecond=0) + relativedelta(days=1)
            return pseudo_today
    except Exception as e:
        print(f"VARNING: Kunde inte dynamiskt hitta sista datum: {e}")
    return datetime(2025, 10, 10) # Fallback

def update_dim_queue(mssql_engine):
    print("-> Startar uppdatering av 'Dim_Queue' (från Bronze)...")
    try:
        # Läser från BRONZE i MSSQL
        bronze_groups = config.BRONZE_TABLES['groups']
        queue_query = f"SELECT ID, Name FROM [{bronze_groups}]"
        
        df_queues = pd.read_sql(queue_query, mssql_engine)
        df_queues.rename(columns={"ID": "QueueId", "Name": "QueueName"}, inplace=True)
        
        table_name = config.TABLE_NAMES['Queue_Dimension']
        staging_table_name = f"{table_name}_STAGING"

        print(f"   -> Sparar {len(df_queues)} köer till STAGING '{staging_table_name}'...")
        df_queues.to_sql(staging_table_name, mssql_engine, if_exists='replace', index=False)
        
        print(f"   -> Flyttar data till PROD '{table_name}' (Auto-Create)...")
        
        sql_transaction = f"""
        IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE [{table_name}];
        SELECT * INTO [{table_name}] FROM [{staging_table_name}];
        """
        with mssql_engine.connect() as connection:
            connection.execute(text(sql_transaction))
            connection.commit() # VIKTIGT

        print(f"-> KLART: 'Dim_Queue' har uppdaterats.")
    except Exception as e:
        print(f"FEL: Kunde inte uppdatera 'Dim_Queue': {e}")
        traceback.print_exc()
        sys.exit(1)

def update_dim_customer_and_phone(mssql_engine, df_clean_call_data):
    print("-> Startar uppdatering av 'Dim_Customer' och 'Dim_Phone_Lookup'...")
    if df_clean_call_data is None or df_clean_call_data.empty:
        print("FEL: Ingen ren data mottogs. Kan inte bygga kund-dimensioner.")
        sys.exit(1)
    
    # === DEL 1: 'Dim_Customer' ===
    try:
        customer_cols_raw = ['CustomerId', 'Name', 'CustomerKey', 'ParentId', 'ParentName', 'BillingType', 'är_dotterbolag', 'LandingNumber']
        customer_cols = [col for col in customer_cols_raw if col in df_clean_call_data.columns]
        
        subset_key = 'CustomerKey' 
        df_dim_customer = df_clean_call_data[customer_cols].drop_duplicates(subset=[subset_key])
        
        customer_table_name = config.TABLE_NAMES['Customer_Dimension']
        customer_staging_table = f"{customer_table_name}_STAGING"
        
        print(f"   -> Sparar {len(df_dim_customer)} unika kunder till STAGING...")
        df_dim_customer.to_sql(customer_staging_table, mssql_engine, if_exists='replace', index=False)
        
        print(f"   -> Flyttar data till PROD '{customer_table_name}'...")
        sql_transaction_cust = f"""
        IF OBJECT_ID('{customer_table_name}', 'U') IS NOT NULL DROP TABLE [{customer_table_name}];
        SELECT * INTO [{customer_table_name}] FROM [{customer_staging_table}];
        """
        with mssql_engine.connect() as connection:
            connection.execute(text(sql_transaction_cust))
            connection.commit() # VIKTIGT
        
        print(f"-> KLART: Sparade {len(df_dim_customer)} kunder till '{customer_table_name}'.")

    except Exception as e:
        print(f"FEL: Kunde inte uppdatera 'Dim_Customer': {e}")
        traceback.print_exc()
        sys.exit(1)

    # === DEL 2: 'Dim_Phone_Lookup' ===
    try:
        phone_cols_raw = ['CustomerId', 'LandingNumber', 'CustomerKey']
        phone_cols = [col for col in phone_cols_raw if col in df_clean_call_data.columns]
        df_phone_base = df_clean_call_data[phone_cols].dropna(subset=['LandingNumber'])
        df_phone_list = df_phone_base.assign(LandingNumber=df_phone_base['LandingNumber'].str.split(','))
        df_phone_lookup = df_phone_list.explode('LandingNumber').reset_index(drop=True)
        df_phone_lookup['LandingNumber'] = df_phone_lookup['LandingNumber'].str.strip()
        df_phone_lookup = df_phone_lookup[df_phone_lookup['LandingNumber'] != '']
        df_phone_lookup = df_phone_lookup.drop_duplicates()
        
        phone_table_name = config.TABLE_NAMES['Phone_Lookup_Dimension']
        phone_staging_table = f"{phone_table_name}_STAGING"

        print(f"   -> Sparar {len(df_phone_lookup)} telefonnummer till STAGING...")
        df_phone_lookup.to_sql(phone_staging_table, mssql_engine, if_exists='replace', index=False)

        print(f"   -> Flyttar data till PROD '{phone_table_name}'...")
        sql_transaction_phone = f"""
        IF OBJECT_ID('{phone_table_name}', 'U') IS NOT NULL DROP TABLE [{phone_table_name}];
        SELECT * INTO [{phone_table_name}] FROM [{phone_staging_table}];
        """
        with mssql_engine.connect() as connection:
            connection.execute(text(sql_transaction_phone))
            connection.commit() # VIKTIGT
        
        print(f"-> KLART: Sparade {len(df_phone_lookup)} nummer till '{phone_table_name}'.")
    except Exception as e:
        print(f"FEL: Kunde inte uppdatera 'Dim_Phone_Lookup': {e}")
        traceback.print_exc()
        sys.exit(1)


def clean_and_export_call_data():
    print(f"Startar skript för datainsamling (BRONZE -> SILVER)...")
    try:
        mssql_engine = create_engine(config.MSSQL_CONN_STR)
        print("-> Ansluten till MSSQL (Bronze & Silver).")
    except Exception as e:
        print(f"FATALT FEL: {e}")
        return None, None

    # === STEG 1: Hämta exkluderingar ===
    try:
        exclude_df = pd.read_csv(config.EXCLUDE_NUMBERS_FILE, dtype={'LandingNumber': str})
        nummer_att_exkludera = exclude_df['LandingNumber'].str.strip().tolist()
    except FileNotFoundError:
        nummer_att_exkludera = []

    # === STEG 2: Hämta Kund-mappning (FRÅN BRONZE) ===
    print("-> Hämtar central kund-mappning (via MSSQL)...")
    try:
        # get_customer_data i utils är nu uppdaterad att läsa från Bronze om engine är MSSQL
        df_customer_mapping = get_customer_data(engine=mssql_engine)
        if df_customer_mapping is None or df_customer_mapping.empty:
                raise Exception("Kundmappningen misslyckades (kan vara tom).")
    except Exception as e:
        print(f"FATALT FEL: {e}")
        traceback.print_exc()
        return None, None
    
    # === STEG 3: Datum ===
    true_today = get_last_date_from_source(mssql_engine)
    
    if config.RUN_MODE == 'VALIDATION':
        validation_end_date_str = config.VALIDATION_SETTINGS.get('TRAINING_END_DATE', '2025-09-30 23:59:59')
        today = pd.to_datetime(validation_end_date_str).replace(hour=0, minute=0, second=0, microsecond=0) + relativedelta(days=1)
        print(f"*** VALIDATION MODE AKTIVT (Simulerad dag: {today.date()}) ***")
    else:
        today = true_today
        print(f"*** PRODUCTION MODE AKTIVT (Dag: {today.date()}) ***")

    end_date_dt = today.replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta(seconds=1)
    start_date_dt = (end_date_dt + relativedelta(seconds=1) - relativedelta(months=config.OPERATIONAL_MONTHS_AGO)).replace(day=1)
    start_date = start_date_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_date = end_date_dt.strftime('%Y-%m-%d %H:%M:%S')

    print(f"-> Hämtar data: {start_date} till {end_date}.")

    # === STEG 4: SQL (MOT BRONZE I MSSQL) ===
    exclude_queues_str = ", ".join([f"'{str(qid)}'" for qid in config.EXCLUDE_QUEUE_IDS])
    
    # Använd tabellnamnet för Bronze CDR
    bronze_cdr_table = config.BRONZE_TABLES['cdr']
    
    # OBS: CAST(Created AS TIME) krävs för MSSQL
    query = f"""
        WITH CallData AS (
            SELECT * FROM [{bronze_cdr_table}]
            WHERE Created BETWEEN '{start_date}' AND '{end_date}'
            AND CAST(Created AS TIME) >= '{config.BUSINESS_HOURS_START}'
            AND CAST(Created AS TIME) < '{config.BUSINESS_HOURS_END}'
        ),
        CalculatedMetrics AS (
            SELECT
                CallId, Status, Created, LandingNumber, ChannelType, QueueId,
                TalkTimeInSec, Duration, CaseId,
                SUM(TalkTimeInSec) OVER (PARTITION BY CallId) as TotalTalkTime,
                MAX(Duration) OVER (PARTITION BY CallId) as TotalDuration,
                MAX(CaseId) OVER (PARTITION BY CallId) as CaseId_Fixed,
                FIRST_VALUE(QueueId) OVER (PARTITION BY CallId ORDER BY Created ASC) as First_QueueId,
                FIRST_VALUE(Created) OVER (PARTITION BY CallId ORDER BY Created ASC) as First_Created,
                FIRST_VALUE(LandingNumber) OVER (PARTITION BY CallId ORDER BY Created ASC) as First_LandingNumber,
                FIRST_VALUE(ChannelType) OVER (PARTITION BY CallId ORDER BY Created ASC) as First_ChannelType,
                LAST_VALUE(Status) OVER (PARTITION BY CallId ORDER BY Created ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as Last_Status,
                ROW_NUMBER() OVER (PARTITION BY CallId ORDER BY Created ASC) as rn_first
            FROM CallData
        )
        SELECT
            CallId, First_Created AS Created, First_LandingNumber AS LandingNumber,
            First_ChannelType AS ChannelType, First_QueueId AS QueueId, CaseId_Fixed AS CaseId,
            Last_Status AS Status, TotalDuration AS Duration, TotalTalkTime AS TalkTimeInSec
        FROM CalculatedMetrics
        WHERE rn_first = 1 AND First_QueueId NOT IN ({exclude_queues_str})
    """
    
    # === STEG 5: Hämta data ===
    try:
        print("-> Hämtar data från BRONZE (MSSQL)...")
        df_all_calls = pd.read_sql(query, mssql_engine) # Läser från MSSQL
        print(f"-> Hämtade {len(df_all_calls)} rader.")
        
        if df_all_calls.empty: return None, None
            
        df_all_calls['Created'] = pd.to_datetime(df_all_calls['Created']).dt.tz_localize(None)
        df_all_calls['LandingNumber'] = df_all_calls['LandingNumber'].str.strip()
        
        df_clean = df_all_calls[~df_all_calls['LandingNumber'].isin(nummer_att_exkludera)]
        df_clean = df_clean[df_clean['LandingNumber'] != '']
        
        print("-> Berikar data...")
        df_enriched = pd.merge(df_clean, df_customer_mapping, on='LandingNumber', how='left')
        df_enriched['är_dotterbolag'] = (~df_enriched['ParentId'].isin([0, pd.NA, np.nan, None, ''])).astype(int)
        df_enriched['är_dotterbolag'] = df_enriched['är_dotterbolag'].fillna(0).astype(int)
        
        if hasattr(config, 'EXCLUDE_CUSTOMER_NAMES_LIKE') and config.EXCLUDE_CUSTOMER_NAMES_LIKE:
            for name_to_exclude in config.EXCLUDE_CUSTOMER_NAMES_LIKE:
                clean_name = name_to_exclude.replace('%', '')
                df_enriched = df_enriched[~df_enriched['Name'].str.contains(clean_name, case=False, na=False)]
            
        df_enriched['Name'] = df_enriched['Name'].fillna('Okänd Kund')
        df_enriched['CustomerKey'] = df_enriched['CustomerKey'].fillna('Okänd')
        df_enriched['TjänstTyp'] = df_enriched['QueueId'].apply(map_queue_to_service)

        # === STEG 6: Spara Abandoned Report ===
        df_abandoned = df_enriched[df_enriched['Status'].str.lower() == 'callabandoned'].copy()
        df_abandoned['Datum'] = df_abandoned['Created'].dt.date
        report_start_date = end_date_dt - relativedelta(days=30)
        df_abandoned = df_abandoned[df_abandoned['Created'] >= report_start_date]
        
        report_cols = ['CallId', 'Created', 'Datum', 'LandingNumber', 'Name', 'CustomerKey', 'TjänstTyp', 'ParentId', 'Duration', 'QueueId']
        report_cols_exist = [col for col in report_cols if col in df_abandoned.columns]
        df_abandoned = df_abandoned[report_cols_exist].sort_values(by='Created', ascending=False)
        
        try:
            table_name_abandoned = config.TABLE_NAMES['Abandoned_Calls_Report']
            staging_table_abandoned = f"{table_name_abandoned}_STAGING"
            
            print(f"-> Sparar {len(df_abandoned)} tappade samtal till STAGING...")
            df_abandoned.to_sql(staging_table_abandoned, mssql_engine, if_exists='replace', index=False, chunksize=1000)
            
            print(f"-> Flyttar data till PROD '{table_name_abandoned}'...")
            sql_transaction_abandoned = f"""
            IF OBJECT_ID('{table_name_abandoned}', 'U') IS NOT NULL DROP TABLE [{table_name_abandoned}];
            SELECT * INTO [{table_name_abandoned}] FROM [{staging_table_abandoned}];
            """
            with mssql_engine.connect() as connection:
                connection.execute(text(sql_transaction_abandoned))
                connection.commit() # VIKTIGT
            
        except Exception as e:
            print(f"VARNING: Kunde inte spara rapport: {e}")

        # === STEG 7: Spara Training Data (MED CASEID) ===
        table_name_training = config.TABLE_NAMES['Operative_Training_Data']
        staging_table_training = f"{table_name_training}_STAGING"
        
        final_cols_to_save = ['CallId', 'CaseId', 'Created', 'Status', 'Duration', 'TalkTimeInSec', 'ChannelType', 'LandingNumber', 'QueueId', 'Name', 'CustomerKey', 'är_dotterbolag', 'TjänstTyp']
        final_cols_to_save_exist = [col for col in final_cols_to_save if col in df_enriched.columns]
        df_slim_to_save = df_enriched[final_cols_to_save_exist].copy()
        df_slim_to_save['Datum'] = df_slim_to_save['Created'].dt.date
        
        try:
            print(f"-> Sparar {len(df_slim_to_save)} rader träningsdata till STAGING...")
            df_slim_to_save.to_sql(staging_table_training, mssql_engine, if_exists='replace', index=False, chunksize=5000) 
            
            print(f"-> Flyttar data till PROD '{table_name_training}'...")
            sql_transaction_training = f"""
            IF OBJECT_ID('{table_name_training}', 'U') IS NOT NULL DROP TABLE [{table_name_training}];
            SELECT * INTO [{table_name_training}] FROM [{staging_table_training}];
            """
            with mssql_engine.connect() as connection:
                connection.execute(text(sql_transaction_training))
                connection.commit() # VIKTIGT

            print(f"KLART! Data sparad i: {table_name_training}")
            return df_enriched, mssql_engine
        
        except Exception as e:
            print(f"FATALT FEL vid sparande av huvuddata: {e}")
            traceback.print_exc()
            return None, mssql_engine 

    except Exception as e:
        print(f"\nEtt ohanterat fel uppstod under databearbetningen: {e}")
        traceback.print_exc()
        return None, None

if __name__ == '__main__':
    df_clean_data, engine = clean_and_export_call_data()
    
    if engine:
        if df_clean_data is not None and not df_clean_data.empty:
            update_dim_customer_and_phone(mssql_engine=engine, df_clean_call_data=df_clean_data) 
        else:
            print("VARNING: Ingen data för dimensioner.")
        update_dim_queue(mssql_engine=engine)
    else:
        print("FATALT FEL: Huvudprocessen misslyckades. Stoppar.")
        sys.exit(1)
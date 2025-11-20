"""
================================================================
JOBB 1: Extrahera Data (FRÅN BRONZE)
================================================================
*** UPPDATERAD (MED SJUKANMÄLAN-TAGGNING) ***
- Läser från MSSQL (Bronze).
- Taggar manuellt upp sjukanmälningsnumret till 'INTERNAL_SICK'.
- Detta garanterar att det inte blir 'Okänd' och försvinner.
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
    try:
        bronze_cdr = config.BRONZE_TABLES['cdr']
        query = f"SELECT MAX(Created) as last_date FROM [{bronze_cdr}]"
        df_last_date = pd.read_sql(query, engine)
        if not df_last_date.empty and pd.notna(df_last_date.iloc[0]['last_date']):
            last_date = pd.to_datetime(df_last_date.iloc[0]['last_date']).tz_localize(None)
            pseudo_today = last_date.replace(hour=0, minute=0, second=0, microsecond=0) + relativedelta(days=1)
            return pseudo_today
    except Exception as e:
        print(f"VARNING: Kunde inte dynamiskt hitta sista datum: {e}")
    return datetime(2025, 10, 10) 

def update_dim_queue(mssql_engine):
    print("-> Startar uppdatering av 'Dim_Queue'...")
    try:
        bronze_groups = config.BRONZE_TABLES['groups']
        queue_query = f"SELECT ID, Name FROM [{bronze_groups}]"
        df_queues = pd.read_sql(queue_query, mssql_engine)
        df_queues.rename(columns={"ID": "QueueId", "Name": "QueueName"}, inplace=True)
        
        table_name = config.TABLE_NAMES['Queue_Dimension']
        staging_table_name = f"{table_name}_STAGING"

        df_queues.to_sql(staging_table_name, mssql_engine, if_exists='replace', index=False)
        
        sql_transaction = f"""
        IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE [{table_name}];
        SELECT * INTO [{table_name}] FROM [{staging_table_name}];
        """
        with mssql_engine.connect() as connection:
            connection.execute(text(sql_transaction))
            connection.commit()

        print(f"-> KLART: 'Dim_Queue' har uppdaterats.")
    except Exception as e:
        print(f"FEL: Kunde inte uppdatera 'Dim_Queue': {e}")
        traceback.print_exc()
        sys.exit(1)

def update_dim_customer_and_phone(mssql_engine, df_clean_call_data):
    print("-> Startar uppdatering av 'Dim_Customer' och 'Dim_Phone_Lookup'...")
    if df_clean_call_data is None or df_clean_call_data.empty:
        print("FEL: Ingen ren data mottogs.")
        sys.exit(1)
    
    try:
        # === DEL 1: Dim_Customer ===
        customer_cols_raw = ['CustomerId', 'Name', 'CustomerKey', 'ParentId', 'ParentName', 'BillingType', 'är_dotterbolag', 'LandingNumber']
        customer_cols = [col for col in customer_cols_raw if col in df_clean_call_data.columns]
        
        subset_key = 'CustomerKey' 
        df_dim_customer = df_clean_call_data[customer_cols].drop_duplicates(subset=[subset_key])
        
        customer_table_name = config.TABLE_NAMES['Customer_Dimension']
        customer_staging_table = f"{customer_table_name}_STAGING"
        
        df_dim_customer.to_sql(customer_staging_table, mssql_engine, if_exists='replace', index=False)
        
        sql_transaction_cust = f"""
        IF OBJECT_ID('{customer_table_name}', 'U') IS NOT NULL DROP TABLE [{customer_table_name}];
        SELECT * INTO [{customer_table_name}] FROM [{customer_staging_table}];
        """
        with mssql_engine.connect() as connection:
            connection.execute(text(sql_transaction_cust))
            connection.commit() 
        
        print(f"-> KLART: Sparade {len(df_dim_customer)} kunder.")

        # === DEL 2: Dim_Phone_Lookup ===
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

        df_phone_lookup.to_sql(phone_staging_table, mssql_engine, if_exists='replace', index=False)

        sql_transaction_phone = f"""
        IF OBJECT_ID('{phone_table_name}', 'U') IS NOT NULL DROP TABLE [{phone_table_name}];
        SELECT * INTO [{phone_table_name}] FROM [{phone_staging_table}];
        """
        with mssql_engine.connect() as connection:
            connection.execute(text(sql_transaction_phone))
            connection.commit()
        
        print(f"-> KLART: Sparade {len(df_phone_lookup)} nummer.")

    except Exception as e:
        print(f"FEL vid dimensionsuppdatering: {e}")
        traceback.print_exc()
        sys.exit(1)


def clean_and_export_call_data():
    print(f"Startar skript för datainsamling (BRONZE -> SILVER)...")
    try:
        mssql_engine = create_engine(config.MSSQL_CONN_STR)
        print("-> Ansluten till MSSQL.")
    except Exception as e:
        print(f"FATALT FEL: {e}")
        return None, None

    # === STEG 1: Ladda filter ===
    try:
        exclude_df = pd.read_csv(config.EXCLUDE_NUMBERS_FILE, dtype={'LandingNumber': str})
        nummer_att_exkludera = exclude_df['LandingNumber'].str.strip().tolist()
        print(f"-> Laddade {len(nummer_att_exkludera)} nummer att exkludera.")
    except FileNotFoundError:
        print(f"-> Info: Filen '{config.EXCLUDE_NUMBERS_FILE}' saknas, inga nummer exkluderas.")
        nummer_att_exkludera = []

    # === STEG 2: Hämta Kund-mappning ===
    try:
        df_customer_mapping = get_customer_data(engine=mssql_engine)
        if df_customer_mapping is None: raise Exception("Ingen kunddata.")
    except Exception as e:
        print(f"FATALT FEL: {e}")
        return None, None
    
    # === STEG 3: Datum ===
    true_today = get_last_date_from_source(mssql_engine)
    if config.RUN_MODE == 'VALIDATION':
        today = pd.to_datetime(config.VALIDATION_SETTINGS.get('TRAINING_END_DATE')) + relativedelta(days=1)
    else:
        today = true_today

    end_date_dt = today.replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta(seconds=1)
    start_date_dt = (end_date_dt + relativedelta(seconds=1) - relativedelta(months=config.OPERATIONAL_MONTHS_AGO)).replace(day=1)
    start_date = start_date_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_date = end_date_dt.strftime('%Y-%m-%d %H:%M:%S')

    print(f"-> Bearbetar data: {start_date} till {end_date}.")

    # === STEG 4: SQL (Med SJUKANMÄLAN-logik i WHERE) ===
    exclude_queues_str = ", ".join([f"'{str(qid)}'" for qid in config.EXCLUDE_QUEUE_IDS])
    bronze_cdr = config.BRONZE_TABLES['cdr']
    
    # Hämtar sjukanmälningsnumret säkert
    sick_number = getattr(config, 'SICK_LEAVE_NUMBER', 'XXXXXXXXXX')

    query = f"""
        WITH CallData AS (
            SELECT * FROM [{bronze_cdr}]
            WHERE Created BETWEEN '{start_date}' AND '{end_date}'
            -- TIDSFILTER BORTTAGET HÄR! Vi tar allt dygnet runt.
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
    
    # === STEG 5: Hämta & Koppla ===
    try:
        df_all_calls = pd.read_sql(query, mssql_engine)
        if df_all_calls.empty: return None, None
            
        df_all_calls['Created'] = pd.to_datetime(df_all_calls['Created']).dt.tz_localize(None)
        df_all_calls['LandingNumber'] = df_all_calls['LandingNumber'].str.strip()
        
        # Filter: Ta bort spärrade nummer (OBS: Se till att SICK_NUMBER inte är i csv-filen!)
        df_clean = df_all_calls[~df_all_calls['LandingNumber'].isin(nummer_att_exkludera)]
        df_clean = df_clean[df_clean['LandingNumber'] != '']
        
        # Merge: Koppla på namn
        print("-> Kopplar på kundnamn...")
        df_enriched = pd.merge(df_clean, df_customer_mapping, on='LandingNumber', how='left')
        
        # --- HÄR ÄR KODEN SOM SAKNADES: MANUELL TAGGNING ---
        mask_sick = df_enriched['LandingNumber'] == sick_number
        if mask_sick.any():
            print(f"-> Identifierade {mask_sick.sum()} sjukanmälningar! Taggar som 'INTERNAL_SICK'.")
            df_enriched.loc[mask_sick, 'Name'] = 'Intern Sjukanmälan'
            df_enriched.loc[mask_sick, 'CustomerKey'] = 'INTERNAL_SICK'
            df_enriched.loc[mask_sick, 'TjänstTyp'] = 'Personal'
        # ---------------------------------------------------

        # Analys: Okända (Nu kommer sjukanmälan INTE att synas här)
        print("\n--- ANALYS: OKÄNDA NUMMER ---")
        unknowns = df_enriched[df_enriched['Name'].isna()]
        if not unknowns.empty:
            print(f"Hittade {len(unknowns)} okända samtal.")
            unknowns['LandingNumber'].value_counts().head(20).to_csv("Unknown_Numbers_Audit.csv")
        else:
            print("Alla nummer matchades!")
        print("-------------------------------\n")

        # Filter Namn (Städning)
        if hasattr(config, 'EXCLUDE_CUSTOMER_NAMES_LIKE') and config.EXCLUDE_CUSTOMER_NAMES_LIKE:
            for name_to_exclude in config.EXCLUDE_CUSTOMER_NAMES_LIKE:
                clean_name = name_to_exclude.replace('%', '')
                df_enriched = df_enriched[~df_enriched['Name'].fillna('').str.contains(clean_name, case=False)]
        
        # Fyll i resterande hål
        df_enriched['är_dotterbolag'] = (~df_enriched['ParentId'].isin([0, pd.NA, np.nan, None, ''])).astype(int)
        df_enriched['är_dotterbolag'] = df_enriched['är_dotterbolag'].fillna(0).astype(int)
        df_enriched['Name'] = df_enriched['Name'].fillna('Okänd Kund')
        df_enriched['CustomerKey'] = df_enriched['CustomerKey'].fillna('Okänd')
        df_enriched['TjänstTyp'] = df_enriched['QueueId'].apply(map_queue_to_service)

        # === STEG 6 & 7: Spara ===
        
        # Abandoned
        df_abandoned = df_enriched[df_enriched['Status'].str.lower() == 'callabandoned'].copy()
        df_abandoned['Datum'] = df_abandoned['Created'].dt.date
        report_start_date = end_date_dt - relativedelta(days=30)
        df_abandoned = df_abandoned[df_abandoned['Created'] >= report_start_date]
        
        try:
            tn_ab = config.TABLE_NAMES['Abandoned_Calls_Report']
            st_ab = f"{tn_ab}_STAGING"
            cols_ab = ['CallId', 'Created', 'Datum', 'LandingNumber', 'Name', 'CustomerKey', 'TjänstTyp', 'ParentId', 'Duration', 'QueueId']
            cols_ab_exist = [c for c in cols_ab if c in df_abandoned.columns]
            df_abandoned[cols_ab_exist].to_sql(st_ab, mssql_engine, if_exists='replace', index=False, chunksize=1000)
            with mssql_engine.connect() as conn:
                conn.execute(text(f"IF OBJECT_ID('{tn_ab}', 'U') IS NOT NULL DROP TABLE [{tn_ab}]; SELECT * INTO [{tn_ab}] FROM [{st_ab}];"))
                conn.commit()
        except Exception as e: print(f"Varning Abandoned: {e}")

        # Main Data
        tn_train = config.TABLE_NAMES['Operative_Training_Data']
        st_train = f"{tn_train}_STAGING"
        final_cols = ['CallId', 'CaseId', 'Created', 'Status', 'Duration', 'TalkTimeInSec', 'ChannelType', 'LandingNumber', 'QueueId', 'Name', 'CustomerKey', 'är_dotterbolag', 'TjänstTyp']
        final_cols_exist = [col for col in final_cols if col in df_enriched.columns]
        df_save = df_enriched[final_cols_exist].copy()
        df_save['Datum'] = df_save['Created'].dt.date
        
        print(f"-> Sparar {len(df_save)} rader till STAGING...")
        df_save.to_sql(st_train, mssql_engine, if_exists='replace', index=False, chunksize=5000)
        with mssql_engine.connect() as conn:
            conn.execute(text(f"IF OBJECT_ID('{tn_train}', 'U') IS NOT NULL DROP TABLE [{tn_train}]; SELECT * INTO [{tn_train}] FROM [{st_train}];"))
            conn.commit()

        print(f"KLART! Data sparad i: {tn_train}")
        return df_enriched, mssql_engine

    except Exception as e:
        print(f"Ett fel uppstod: {e}")
        traceback.print_exc()
        return None, None

if __name__ == '__main__':
    df_clean_data, engine = clean_and_export_call_data()
    
    if engine and df_clean_data is not None:
        update_dim_customer_and_phone(mssql_engine=engine, df_clean_call_data=df_clean_data) 
        update_dim_queue(mssql_engine=engine)
    else:
        print("FATALT FEL: Huvudprocessen misslyckades.")
        sys.exit(1)
"""
================================================================
JOBB 1.5: Kör Kundsegmentering (K-Means)
================================================================
*** UPPDATERAD (COMMIT FIX) ***
- Lade till connection.commit() för att säkerställa att tabeller sparas.
- Använder DROP/SELECT INTO för att skapa tabeller automatiskt.
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import config
import sys
import traceback
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from DataDriven_utils import add_all_features

def create_and_save_segments():
    print("--- Startar Jobb 1.5: Dynamisk Kundsegmentering (K-Means) [ROBUST-VERSION] ---")

    try:
        mssql_engine = create_engine(config.MSSQL_CONN_STR)
        print("-> Ansluten till MSSQL Data Warehouse.")
    except Exception as e:
        print(f"FATALT FEL: Kunde inte ansluta till MSSQL: {e}")
        sys.exit(1)

    # === STEG 1: Läs in historik ===
    table_name_training = config.TABLE_NAMES['Operative_Training_Data']
    print(f"-> Läser in historik från '{table_name_training}'...")

    try:
        cols_to_use = ['Created', 'Name', 'QueueId', 'CustomerKey', 'TalkTimeInSec', 'CallId']
        cols_str = ", ".join([f'[{col}]' for col in cols_to_use if col is not None])
        
        sql_query = f'SELECT {cols_str} FROM [{table_name_training}]'
        df_history = pd.read_sql(sql_query, mssql_engine)
        df_history['Created'] = pd.to_datetime(df_history['Created']).dt.tz_localize(None)
        
        if df_history.empty:
            print("VARNING: Ingen historisk data hittades.")
            sys.exit(1)
            
        print(f"-> Läste {len(df_history)} samtalshändelser.")
        
    except Exception as e:
        print(f"FEL: Kunde inte läsa data från '{table_name_training}'.")
        print(f"Tekniskt fel: {e}")
        sys.exit(1)

    # === STEG 2: Aggregera ===
    print("-> Aggregerar per (CustomerKey)...")
    df_history_features = df_history.rename(columns={'Created': 'ds'})
    df_history_features = add_all_features(df_history_features, ds_col='ds')
    
    df_agg = df_history_features.groupby(['CustomerKey']).agg(
        Total_Samtal=('CallId', 'count'),
        Total_Samtalstid_Sek=('TalkTimeInSec', 'sum'),
        Name=('Name', 'first')
    ).reset_index()
    
    df_agg['Genomsnittlig_AHT_Sek'] = (df_agg['Total_Samtalstid_Sek'] / df_agg['Total_Samtal']).fillna(0).astype(int)

    # === STEG 3: Peak Pattern ===
    print("-> Beräknar 'Peak Pattern'...")
    df_peak_pattern = df_history_features.groupby(['CustomerKey', 'veckodag', 'timme']).agg(
        Antal=('CallId', 'count')
    ).reset_index()
    df_peak_pattern = df_peak_pattern.sort_values(by='Antal', ascending=False)
    df_peak = df_peak_pattern.drop_duplicates(subset=['CustomerKey'], keep='first').copy()

    dag_map = {0: 'Mån', 1: 'Tis', 2: 'Ons', 3: 'Tor', 4: 'Fre', 5: 'Lör', 6: 'Sön'}
    df_peak['Peak_Pattern'] = df_peak['veckodag'].map(dag_map) + '-' + df_peak['timme'].astype(str)
    df_segments = pd.merge(df_agg, df_peak[['CustomerKey', 'Peak_Pattern']], on=['CustomerKey'], how='left')
    df_segments['Peak_Pattern'] = df_segments['Peak_Pattern'].fillna('Oklart')

    # === STEG 4: K-Means ===
    print("-> Skapar segment (K-Means)...")
    features_for_clustering = ['Total_Samtal', 'Genomsnittlig_AHT_Sek']
    df_to_cluster = df_segments[df_segments['Total_Samtal'] > 0].copy()
    
    if not df_to_cluster.empty:
        scaler = StandardScaler()
        df_scaled = scaler.fit_transform(df_to_cluster[features_for_clustering])
        kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
        df_to_cluster['Cluster'] = kmeans.fit_predict(df_scaled)
        
        model_inertia = kmeans.inertia_
        print(f"-> K-Means Inertia: {model_inertia:.2f}")
        
        df_centroids = df_to_cluster.groupby('Cluster')[features_for_clustering].mean().reset_index()
        df_centroids = df_centroids.sort_values(by='Total_Samtal')
        low_vol_clusters = df_centroids.iloc[0:2]['Cluster'].tolist()
        high_vol_clusters = df_centroids.iloc[2:4]['Cluster'].tolist()
        cluster_map = {}
        low_vol_df = df_centroids[df_centroids['Cluster'].isin(low_vol_clusters)].sort_values(by='Genomsnittlig_AHT_Sek')
        cluster_map[low_vol_df.iloc[0]['Cluster']] = 'Låg-Volym_Korta-Ärenden'
        cluster_map[low_vol_df.iloc[1]['Cluster']] = 'Låg-Volym_Långa-Ärenden'
        high_vol_df = df_centroids[df_centroids['Cluster'].isin(high_vol_clusters)].sort_values(by='Genomsnittlig_AHT_Sek')
        cluster_map[high_vol_df.iloc[0]['Cluster']] = 'Hög-Volym_Korta-Ärenden'
        cluster_map[high_vol_df.iloc[1]['Cluster']] = 'Hög-Volym_Långa-Ärenden'
        
        df_to_cluster['Behavior_Segment'] = df_to_cluster['Cluster'].map(cluster_map)
        df_segments = pd.merge(df_segments, df_to_cluster[['CustomerKey', 'Behavior_Segment']], on=['CustomerKey'], how='left')
    else:
        df_segments['Behavior_Segment'] = 'Okänt'

    df_segments['Behavior_Segment'] = df_segments['Behavior_Segment'].fillna('Okänt/Ingen Taltid')

    # === STEG 5: Spara Dim_Customer_Behavior (MED COMMIT) ===
    output_table_name = config.TABLE_NAMES['Customer_Behavior_Dimension']
    staging_table_name = f"{output_table_name}_STAGING"
    final_cols = ['CustomerKey', 'Name', 'Total_Samtal', 'Genomsnittlig_AHT_Sek', 'Behavior_Segment']
    df_to_save = df_segments[final_cols]

    try:
        print(f"-> Sparar till STAGING '{staging_table_name}'...")
        df_to_save.to_sql(staging_table_name, mssql_engine, if_exists='replace', index=False, chunksize=1000)

        print(f"-> Flyttar till PROD '{output_table_name}' (Auto-Create)...")
        sql_transaction = f"""
        IF OBJECT_ID('{output_table_name}', 'U') IS NOT NULL DROP TABLE [{output_table_name}];
        SELECT * INTO [{output_table_name}] FROM [{staging_table_name}];
        """
        with mssql_engine.connect() as connection:
            connection.execute(text(sql_transaction))
            connection.commit() # <--- VIKTIG FIX: Bekräfta transaktionen!
            
        print(f"-> KLART: '{output_table_name}' uppdaterad.")

    except Exception as e:
        print(f"FATALT FEL vid sparning av segment: {e}")
        sys.exit(1)

    # === STEG 6: Peak Analysis (MED COMMIT) ===
    print("-> Analyserar månatliga topp-tider...")
    try:
        df_samtal = df_history_features[df_history_features['TalkTimeInSec'] > 0].copy()
        if not df_samtal.empty:
            lower_limit = df_samtal['TalkTimeInSec'].quantile(0.33)
            upper_limit = df_samtal['TalkTimeInSec'].quantile(0.66)
            if lower_limit == 0: lower_limit = 1 
            if upper_limit <= lower_limit: upper_limit = lower_limit + 60 

            df_samtal['Samtalstyp'] = 'Normal'
            df_samtal.loc[df_samtal['TalkTimeInSec'] < lower_limit, 'Samtalstyp'] = 'Kort'
            df_samtal.loc[df_samtal['TalkTimeInSec'] > upper_limit, 'Samtalstyp'] = 'Långt'

            df_peak_monthly = df_samtal.groupby(
                ['CustomerKey', 'Name', 'månad_namn', 'veckodag_namn', 'timme', 'Samtalstyp']
            ).agg(Antal_Samtal_Denna_Timme=('CallId', 'count')).reset_index()

            df_monthly_totals = df_peak_monthly.groupby(
                ['CustomerKey', 'månad_namn', 'Samtalstyp']
            )['Antal_Samtal_Denna_Timme'].sum().to_frame('Totala_Samtal_Manad_Typ').reset_index()

            df_peak_monthly = pd.merge(df_peak_monthly, df_monthly_totals, on=['CustomerKey', 'månad_namn', 'Samtalstyp'], how='left')
            
            mask = df_peak_monthly['Totala_Samtal_Manad_Typ'] > 0
            df_peak_monthly.loc[mask, 'Procent_Av_Manad_Typ'] = (df_peak_monthly['Antal_Samtal_Denna_Timme'] / df_peak_monthly['Totala_Samtal_Manad_Typ'])
            df_peak_monthly['Procent_Av_Manad_Typ'] = df_peak_monthly['Procent_Av_Manad_Typ'].fillna(0)

            df_peak_monthly['Peak_Rank'] = df_peak_monthly.groupby(
                ['CustomerKey', 'månad_namn', 'Samtalstyp']
            )['Antal_Samtal_Denna_Timme'].rank(method='first', ascending=False)

            df_top_peaks = df_peak_monthly[df_peak_monthly['Peak_Rank'] <= 3].copy()
            df_top_peaks = df_top_peaks.sort_values(
                by=['CustomerKey', 'månad_namn', 'Samtalstyp', 'Peak_Rank']
            ).drop(columns=['Totala_Samtal_Manad_Typ'])

            peak_table_name = config.TABLE_NAMES.get('Monthly_Peak_Analysis', 'Dim_Customer_Monthly_Peaks')
            peak_staging_table_name = f"{peak_table_name}_STAGING"

            print(f"-> Sparar peaks till STAGING '{peak_staging_table_name}'...")
            df_top_peaks.to_sql(peak_staging_table_name, mssql_engine, if_exists='replace', index=False, chunksize=1000)
            
            print(f"-> Flyttar till PROD '{peak_table_name}'...")
            peak_sql_transaction = f"""
            IF OBJECT_ID('{peak_table_name}', 'U') IS NOT NULL DROP TABLE [{peak_table_name}];
            SELECT * INTO [{peak_table_name}] FROM [{peak_staging_table_name}];
            """
            with mssql_engine.connect() as connection:
                connection.execute(text(peak_sql_transaction))
                connection.commit() # <--- VIKTIG FIX
            
            print(f"-> KLART: '{peak_table_name}' sparad.")

    except Exception as e:
        print(f"VARNING: Kunde inte spara peak-tabellen: {e}")
        # Ej kritiskt, fortsätt
        
    print("\n--- Kundsegmentering slutförd ---")

if __name__ == '__main__':
    create_and_save_segments()
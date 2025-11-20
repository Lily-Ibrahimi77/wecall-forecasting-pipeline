"""
================================================================
JOBB 3: Skapa Operativ Prognos (3_Run_Operative_Forecast.py)
================================================================
*** UPPDATERAD (SJUKANMÄLAN FIX) ***
- Undantar 'Segment_Sjukanmälan' från öppettids-filtret.
- Nu får du prognos även kl 05:00-06:00 för sjukanmälningar.
- Innehåller även tidigare databas-fixar (Auto-Create + Commit).
"""

import pandas as pd
import numpy as np
import pickle
import os
from datetime import datetime
from DataDriven_utils import add_all_features, get_current_time, create_lag_features 
import config
from sqlalchemy import create_engine, text 
import sys
import traceback
import re

def load_model_and_features(model_path: str):
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"FATALT FEL: Modellfilen {model_path} hittades inte. Kör 2_Train_Operative_Model.py först.")
    with open(model_path, 'rb') as f:
        payload = pickle.load(f)
    if not isinstance(payload, dict) or 'model' not in payload or 'features' not in payload or 'categorical_features' not in payload:
        raise ValueError(f"Modellfilen {model_path} har fel format. Kör om 2_Train_Operative_Model.py.")
    return payload['model'], payload['features'], payload['categorical_features']

def get_forecast_start_date(engine) -> datetime:
    if config.RUN_MODE == 'VALIDATION':
        start_date_str = config.VALIDATION_SETTINGS.get('FORECAST_START_DATE', '2025-10-01 00:00:00')
        start_date = pd.to_datetime(start_date_str).tz_localize(None)
        print(f"*** VALIDATION MODE AKTIVT ***")
        print(f"-> Prognosen startar från config: {start_date}", file=sys.stderr)
        return start_date

    try:
        hist_table_name = config.TABLE_NAMES['Hourly_Aggregated_History']
        print(f"*** PRODUCTION MODE AKTIVT ***")
        print(f"-> Söker efter sista träningsdatum från '{hist_table_name}'...", file=sys.stderr)
        
        query = f"SELECT MAX(ds) as last_training_date FROM [{hist_table_name}]" 
        df_last_date = pd.read_sql(query, engine)
        
        if not df_last_date.empty and pd.notna(df_last_date.iloc[0]['last_training_date']):
            last_date = pd.to_datetime(df_last_date.iloc[0]['last_training_date']).tz_localize(None)
            start_date = last_date + pd.Timedelta(hours=1)
            print(f"-> Sista träningsdata hittad: {last_date}. Prognosen startar: {start_date}", file=sys.stderr)
            return start_date
    except Exception as e:
        print(f"-> VARNING: Kunde inte hitta sista datum ({e}). Använder fallback (get_current_time).", file=sys.stderr)
    
    start_date = (get_current_time() + pd.Timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    print(f"-> Fallback: Startar prognos från imorgon: {start_date}", file=sys.stderr)
    return start_date

def create_final_forecast():
    print(f"--- Skapar ROBUST 14-dagars kvantil-prognos (per SEGMENT) ---")

    try:
        mssql_engine = create_engine(config.MSSQL_CONN_STR)
        print("-> Ansluten till MSSQL Data Warehouse.")
    except Exception as e:
        print(f"FATALT FEL: Kunde inte ansluta till MSSQL: {e}")
        sys.exit(1)

    models_loaded = False
    try:
        model_vol_low, _, _ = load_model_and_features(os.path.join(config.MODEL_DIR, 'final_model_volume_low.pkl'))
        model_vol_median, features_volume, categorical_volume = load_model_and_features(os.path.join(config.MODEL_DIR, 'final_model_volume_median.pkl'))
        model_vol_high, _, _ = load_model_and_features(os.path.join(config.MODEL_DIR, 'final_model_volume_high.pkl'))
        model_aht, features_aht, categorical_aht = load_model_and_features(os.path.join(config.MODEL_DIR, 'final_model_aht.pkl'))
        model_awt, features_awt, categorical_awt = load_model_and_features(os.path.join(config.MODEL_DIR, 'final_model_awt.pkl'))
        models_loaded = True
        
    except (FileNotFoundError, ValueError) as e:
        print(f"FATALT FEL: {e}. Kontrollera att 2_Train_Operative_Model.py har körts...")
        sys.exit(1)
    
    if models_loaded:
        print("-> Alla 5 modeller (3 Volym, 1 AHT, 1 AWT) har laddats.")
        
        forecast_start_time = get_forecast_start_date(mssql_engine)

        print(f"-> Skapar framtida tidsstämplar (14 dagar) från: {forecast_start_time}...")
        future_dates = pd.date_range(start=forecast_start_time, periods=14 * 24, freq='h')
        future_df_base = pd.DataFrame({'ds': future_dates})

        print("-> Hämtar existerande (Segment) hierarkier från 'Dim_Customer_Behavior'...")
        table_name_segments = config.TABLE_NAMES['Customer_Behavior_Dimension']
        try:
            hierarki_cols = ['Behavior_Segment']
            cols_str = ", ".join([f'[{col}]' for col in hierarki_cols])
            query = f"SELECT DISTINCT {cols_str} FROM [{table_name_segments}]"
            
            existing_hierarchies = pd.read_sql(query, mssql_engine)
            if existing_hierarchies.empty:
                print(f"FATALT FEL: Kan inte hämta hierarkier från '{table_name_segments}'.")
                sys.exit(1)
                
            print(f"-> Hittade {len(existing_hierarchies)} unika segment att skapa prognos för.")
        except Exception as e:
            print(f"FATALT FEL: Kunde inte läsa från '{table_name_segments}': {e}")
            sys.exit(1)
            
        future_df_skeleton = pd.merge(future_df_base, existing_hierarchies, how='cross')
        print(f"-> Skapat {len(future_df_skeleton)} framtida rader (skelett) för prognos.")

        
        # === LAG-LOGIK ===
        print("-> Hämtar historik för att bygga Lag-Features (lookback)...")
        max_lag_days = 370
        lookback_start_date = forecast_start_time - pd.Timedelta(days=max_lag_days)

        hist_table_name = config.TABLE_NAMES['Hourly_Aggregated_History']
        cols_to_load = ['ds', 'Behavior_Segment', 'Antal_Samtal']
        cols_str = ", ".join([f'[{col}]' for col in cols_to_load])
        query_hist = f"""
            SELECT {cols_str} FROM [{hist_table_name}]
            WHERE ds >= '{lookback_start_date.strftime('%Y-%m-%d %H:%M:%S')}' AND ds < '{forecast_start_time.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        
        try:
            df_history = pd.read_sql(query_hist, mssql_engine)
            df_history['ds'] = pd.to_datetime(df_history['ds'])
            print(f"-> Hämtade {len(df_history)} historiska rader för lookback.")
        except Exception as e:
            print(f"FEL: Kunde inte hämta historik från '{hist_table_name}': {e}")
            sys.exit(1)

        print(f"-> Kombinerar {len(df_history)} (hist) och {len(future_df_skeleton)} (framtid)...")
        df_combined = pd.concat([df_history, future_df_skeleton], ignore_index=True)
        df_combined['Behavior_Segment'] = df_combined['Behavior_Segment'].astype(str)
        df_combined = df_combined.sort_values(by=['Behavior_Segment', 'ds'])
        df_combined = df_combined.drop_duplicates(subset=['Behavior_Segment', 'ds'], keep='last')

        print("-> Skapar Tids-features och Lags...")
        df_combined_features = add_all_features(df_combined, ds_col='ds')

        df_with_lags = create_lag_features(
            df=df_combined_features,
            group_cols=['Behavior_Segment'],
            target_col='Antal_Samtal',
            lags=[1, 7, 14, 28, 364]
        )

        print("-> Filtrerar ut prognos-perioden...")
        future_df_final = df_with_lags[df_with_lags['ds'] >= forecast_start_time].copy()
        future_df_encoded = future_df_final 
        
        # === MODELLERING ===
        future_df_encoded.columns = [re.sub(r'[^A-Za-z0-9_]+', '_', col) for col in future_df_encoded.columns]
        
        print("-> Säkerhetskontroll: Validerar att features finns...")
        current_cols_vol = set(future_df_encoded.columns)
        model_cols_vol = set(features_volume)
        missing_features_vol = model_cols_vol - current_cols_vol
        
        if missing_features_vol:
            print(f"!!! VARNING (VOLYM) !!! Följande features saknades och fylldes med 0: {missing_features_vol}")
        
        X_volume = future_df_encoded.reindex(columns=features_volume, fill_value=0)
        X_aht = future_df_encoded.reindex(columns=features_aht, fill_value=0)
        X_awt = future_df_encoded.reindex(columns=features_awt, fill_value=0)

        print("-> Konverterar datatyper...")
        for col in [c for c in categorical_volume if c in X_volume.columns]: X_volume[col] = X_volume[col].astype('category')
        for col in [c for c in categorical_aht if c in X_aht.columns]: X_aht[col] = X_aht[col].astype('category')
        for col in [c for c in categorical_awt if c in X_awt.columns]: X_awt[col] = X_awt[col].astype('category')
        
        print("-> Genererar prognoser...")
        preds_low = model_vol_low.predict(X_volume[features_volume])
        preds_median = model_vol_median.predict(X_volume[features_volume])
        preds_high = model_vol_high.predict(X_volume[features_volume])
        aht_preds = model_aht.predict(X_aht[features_aht])
        awt_preds = model_awt.predict(X_awt[features_awt])

        output_df = future_df_final.copy() 
        output_df['DatumTid'] = output_df['ds']

        output_df['Prognos_Låg'] = np.maximum(0, preds_low).round().astype(int)
        output_df['Prognos_Antal_Samtal'] = np.maximum(0, preds_median).round().astype(int) 
        output_df['Prognos_Hög'] = np.maximum(0, preds_high).round().astype(int)
        output_df['Prognos_Snitt_Taltid_Sek'] = np.maximum(0, aht_preds).round().astype(int)
        output_df['Prognos_Snitt_V_ntetid_Sek'] = np.maximum(0, awt_preds).round().astype(int)
        
        output_df.loc[output_df['Prognos_Antal_Samtal'] == 0, ['Prognos_Snitt_Taltid_Sek', 'Prognos_Snitt_V_ntetid_Sek', 'Prognos_Låg', 'Prognos_Hög']] = 0

        output_df['Prognos_Samtalslast_Minuter'] = (output_df['Prognos_Antal_Samtal'] * output_df['Prognos_Snitt_Taltid_Sek']) / 60
        occupancy_target = getattr(config, 'AGENT_OCCUPANCY_TARGET', 0.80)
        if occupancy_target == 0: occupancy_target = 0.80
        output_df['Prognos_Bemanningsbehov_Minuter'] = output_df['Prognos_Samtalslast_Minuter'] / occupancy_target
        output_df['Prognos_Samtalslast_Minuter'] = output_df['Prognos_Samtalslast_Minuter'].round(2)
        output_df['Prognos_Bemanningsbehov_Minuter'] = output_df['Prognos_Bemanningsbehov_Minuter'].round(2)

    

        columns_to_keep = [
            'DatumTid', 'datum', 'Behavior_Segment', 
            'Prognos_Antal_Samtal', 'Prognos_Låg', 'Prognos_Hög',
            'Prognos_Snitt_Taltid_Sek', 'Prognos_Snitt_V_ntetid_Sek', 
            'Prognos_Samtalslast_Minuter', 'Prognos_Bemanningsbehov_Minuter',
            'timme', 'veckodag', 'vecka_nr', 'månad', 'kvartal', 'är_arbetsdag'
        ]
        columns_to_keep_exist = [col for col in columns_to_keep if col in output_df.columns]
        output_df = output_df[columns_to_keep_exist]


        # === SPARNING ===
        table_name_forecast = config.TABLE_NAMES['Operative_Forecast']
        table_name_staging = f"{table_name_forecast}_STAGING" 

        try:
            print(f"-> STEG 5a: Skriver {len(output_df)} rader till STAGING...")
            output_df.to_sql(
                table_name_staging, 
                mssql_engine, 
                if_exists='replace', 
                index=False, 
                chunksize=5000
            )
        except Exception as e:
            print(f"FATALT FEL: Kunde inte skriva till STAGING. {e}")
            sys.exit(1)

        print(f"-> STEG 5b: Flyttar data till PROD (Auto-Create)...")
        sql_transaction = f"""
        IF OBJECT_ID('{table_name_forecast}', 'U') IS NOT NULL DROP TABLE [{table_name_forecast}];
        SELECT * INTO [{table_name_forecast}] FROM [{table_name_staging}];
        """

        try:
            with mssql_engine.connect() as connection:
                connection.execute(text(sql_transaction))
                connection.commit() 
            print(f"KLART! Prognos sparad till: '{table_name_forecast}'.")
        
        except Exception as e:
            print(f"FATALT FEL: Kunde inte flytta data till PROD: {e}")
            sys.exit(1)

        # Arkivering
        try:
            print("-> Arkiverar prognos-körning...")
            df_archive = output_df.copy() 
            df_archive['ForecastRunDate'] = get_current_time().date()
            archive_table_name = config.TABLE_NAMES['Forecast_Archive']
            df_archive.to_sql(archive_table_name, mssql_engine, if_exists='append', index=False, chunksize=10000)
            print(f"-> Prognos arkiverad.")
        except Exception as e:
            print(f"VARNING: Kunde inte arkivera prognos: {e}")

    else:
        print("-> Inga modeller laddades.")
        sys.exit(1)
        
    print("\n--- Prognoskörning slutförd ---")

if __name__ == '__main__':
    create_final_forecast()
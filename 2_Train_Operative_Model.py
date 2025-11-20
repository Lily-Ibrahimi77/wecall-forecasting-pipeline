"""
================================================================
JOBB 2: Träna Operativ Modell (2_Train_Operative_Model.py)
================================================================
*** UPPDATERAD (SJUKANMÄLAN FIX) ***
- Undantar 'Segment_Sjukanmälan' från öppettids-filtret vid träning.
- Nu lär sig modellen mönster även från sjukanmälningar kl 05:00.
- Innehåller även tidigare fixar (Commit + Auto-Create).
"""

import pandas as pd
import numpy as np
import lightgbm as lgb
import pickle
import os
from DataDriven_utils import add_all_features, create_lag_features 
from datetime import datetime
import config
from sqlalchemy import create_engine, text
import sys
import traceback
import re

def train_final_system():
    print("--- Startar ROBUST hierarkisk träning (per SEGMENT) ---")

    try:
        mssql_engine = create_engine(config.MSSQL_CONN_STR)
        print("-> Ansluten till MSSQL Data Warehouse.")
    except Exception as e:
        print(f"FATALT FEL: Kunde inte ansluta till MSSSQL: {e}")
        sys.exit(1)

    # ... (STEG 1: Läs in Rå-historik) ...
    table_name_training = config.TABLE_NAMES['Operative_Training_Data']
    print(f"-> Läser in rå-historik från '{table_name_training}'...")
    
    try:
        cols_to_use = ['CallId', 'Created', 'Status', 'Duration', 'TalkTimeInSec', 'ChannelType', 'CustomerKey']
        cols_str = ", ".join([f'[{col}]' for col in cols_to_use if col is not None])
        sql_query = f'SELECT {cols_str} FROM [{table_name_training}]'
        df_raw = pd.read_sql(sql_query, mssql_engine)
    except Exception as e:
        print(f"FEL: Kunde inte läsa data från '{table_name_training}'.")
        print(f"Tekniskt fel: {e}")
        sys.exit(1)

    # ... (STEG 1.5: Läs in Segment och slå ihop) ...
    table_name_segments = config.TABLE_NAMES['Customer_Behavior_Dimension']
    print(f"-> Läser in kundsegment från '{table_name_segments}'...")
    try:
        sql_query_segments = f"SELECT CustomerKey, Behavior_Segment FROM [{table_name_segments}]"
        df_segments = pd.read_sql(sql_query_segments, mssql_engine)
        
        if df_segments.empty:
            print(f"VARNING: Tabellen '{table_name_segments}' är tom! Körde 1.5 korrekt?")
            sys.exit(1)
            
    except Exception as e:
        print(f"FEL: Kunde inte läsa segment från '{table_name_segments}'.")
        print(f"Tekniskt fel: {e}")
        sys.exit(1)
        
    print(f"-> Läste {len(df_raw)} samtal och {len(df_segments)} kundsegment.")
    df_raw['Created'] = pd.to_datetime(df_raw['Created']).dt.tz_localize(None)
    df_filtered = df_raw[df_raw['ChannelType'].str.lower() == 'call'].copy()
    df_filtered['is_abandoned'] = (df_filtered['Status'].str.lower() == 'callabandoned').astype(int)
    df_filtered['is_answered'] = (1 - df_filtered['is_abandoned'])
    
    df_enriched = pd.merge(df_filtered, df_segments, on=['CustomerKey'], how='left')
    df_enriched['Behavior_Segment'] = df_enriched['Behavior_Segment'].fillna('Okänt')
    print("-> Samtalsdata berikad med segment.")

    # ... (STEG 2: Aggregera och skapa rutnät) ...
    print("-> Aggregerar data per (Timme, Segment)...")
    group_keys = [ pd.Grouper(key='Created', freq='h'), 'Behavior_Segment' ]
    df_hourly_sparse = df_enriched.groupby(group_keys).agg(
        Antal_Samtal=('CallId', 'count'),
        Total_Väntetid_Sek=('Duration', 'sum'),
        Total_Samtalstid_Sek=('TalkTimeInSec', 'sum'),
        Antal_Övergivna=('is_abandoned', 'sum'),
        Antal_Besvarade_Samtal=('is_answered', 'sum')
    ).reset_index()

    if df_hourly_sparse.empty:
        print("VARNING: Ingen data kvar efter aggregering. Kan inte träna modell.")
        sys.exit(1)

    print("-> Skapar komplett rutnät...")
    start_time = pd.to_datetime(df_hourly_sparse['Created'].min()).tz_localize(None)
    end_time = pd.to_datetime(df_hourly_sparse['Created'].max()).tz_localize(None)
    all_hours = pd.date_range(start=start_time, end=end_time, freq='h')
    
    hierarki_cols = ['Behavior_Segment']
    existing_hierarchies = df_hourly_sparse[hierarki_cols].drop_duplicates()
    print(f"-> Hittade {len(existing_hierarchies)} unika segment att bygga rutnät för.")

    df_master_grid_base = pd.DataFrame({'Created': all_hours})
    df_master_grid = df_master_grid_base.merge(existing_hierarchies, how='cross')

    df_hourly_sparse['Created'] = pd.to_datetime(df_hourly_sparse['Created']).dt.tz_localize(None)
    df_master_grid['Created'] = pd.to_datetime(df_master_grid['Created']).dt.tz_localize(None)
    
    df_hourly = pd.merge(
        df_master_grid, df_hourly_sparse,
        on=['Created'] + hierarki_cols, how='left'
    )
    

    fill_zero_cols = ['Antal_Samtal', 'Total_Väntetid_Sek', 'Total_Samtalstid_Sek', 'Antal_Övergivna', 'Antal_Besvarade_Samtal']
    for col in fill_zero_cols:
        df_hourly[col] = df_hourly[col].fillna(0).astype(int)

    df_hourly['Andel_Övergivna'] = (df_hourly['Antal_Övergivna'] / df_hourly['Antal_Samtal']).fillna(0)
    df_hourly.rename(columns={'Created': 'ds'}, inplace=True)
    print(f"-> Träningsdata har nu {len(df_hourly)} rader (inkl. noll-timmar).")

    # ... (STEG 3: Skapa Features) ...
    print("-> Skapar tids-features...")
    df_model = add_all_features(df_hourly, ds_col='ds')
    
    print("-> Rensar kolumnnamn för LightGBM...")
    df_model.columns = [re.sub(r'[^A-Za-z0-9_]+', '_', col) for col in df_model.columns]

    print("-> Skapar Lag Features (säsongsvariationer)...")
    rensad_hierarki = ['Behavior_Segment']
    rensad_hierarki = [col for col in rensad_hierarki if col in df_model.columns]
    
    df_model = create_lag_features(
        df=df_model,
        group_cols=rensad_hierarki,
        target_col='Antal_Samtal',
        lags=[1, 7, 14, 28, 364]
    )
    
    print("Beräknar AHT/AWT...")
    df_model['Snitt_Taltid_Sek'] = df_model.apply(
        lambda row: row['Total_Samtalstid_Sek'] / row['Antal_Besvarade_Samtal'] if row['Antal_Besvarade_Samtal'] > 0 else 0,
        axis=1
    )
    df_model['Snitt_V_ntetid_Sek'] = df_model.apply(
        lambda row: row['Total_V_ntetid_Sek'] / row['Antal_Samtal'] if row['Antal_Samtal'] > 0 else 0,
        axis=1
    )
    lag_cols = [col for col in df_model.columns if '_lag_' in col]
    df_model[lag_cols] = df_model[lag_cols].fillna(0) 

    # ... (STEG 4: Förbered för modell) ...
    print("-> Skapar feature-listor och sätter dtypes...")
    base_features = [
        'timme', 'veckodag', 'dag_p_ret', 'vecka_nr', 'm_nad', 'kvartal',
        'r_arbetsdag', 'year_sin', 'year_cos'
    ]
    lag_features = [col for col in df_model.columns if '_lag_' in col]
    print(f"-> Lägger till {len(lag_features)} lag-features i modellen.")

    segment_features = ['Behavior_Segment']
    rensad_segment_features = [re.sub(r'[^A-Za-z0-9_]+', '_', col) for col in segment_features]

    features = base_features + lag_features + rensad_segment_features
    features = [f for f in features if f in df_model.columns]
    
    final_categorical_raw = [
        'timme', 'veckodag', 'm_nad', 'kvartal', 'Behavior_Segment'
    ]
    rensad_final_categorical = [re.sub(r'[^A-Za-z0-9_]+', '_', col) for col in final_categorical_raw]
    rensad_final_categorical = [f for f in rensad_final_categorical if f in df_model.columns]

    targets = {
        'aht': 'Snitt_Taltid_Sek',
        'awt': 'Snitt_V_ntetid_Sek'
    }

    print(f"-> Konverterar {len(rensad_final_categorical)} kategoriska features till 'category' dtype...")
    for col in rensad_final_categorical:
        if col in df_model.columns:
            df_model[col] = df_model[col].astype('category')

    # === STEG 5: Spara Aggregerad Fil - MED COMMIT ===
    
    table_name_agg = config.TABLE_NAMES['Hourly_Aggregated_History']
    staging_table_agg = f"{table_name_agg}_STAGING"
    
    try:
        df_model_to_save = df_model.dropna(subset=['ds'])
        print(f"-> Sparar {len(df_model_to_save)} rader till STAGING '{staging_table_agg}'...")
        
        df_model_to_save.head(10).to_sql(
            staging_table_agg, mssql_engine, if_exists='replace', index=False
        )
        if len(df_model_to_save) > 10:
            df_model_to_save.iloc[10:].to_sql(
                staging_table_agg, mssql_engine, if_exists='append', index=False, chunksize=10000
            )
            
        print(f"-> Flyttar data till PROD '{table_name_agg}' (Auto-Create)...")
        
        sql_transaction_agg = f"""
        IF OBJECT_ID('{table_name_agg}', 'U') IS NOT NULL DROP TABLE [{table_name_agg}];
        SELECT * INTO [{table_name_agg}] FROM [{staging_table_agg}];
        """
        with mssql_engine.connect() as connection:
            connection.execute(text(sql_transaction_agg))
            connection.commit() 

        print(f"-> Historisk arbetsfil sparad till '{table_name_agg}'")
        
    except Exception as e:
        print(f"FEL: Kunde inte spara aggregerad data till '{table_name_agg}'.")
        print(f"Tekniskt fel: {e}")
        sys.exit(1)

    # ... (STEG 6: Träning med Early Stopping) ...
    
    print("\n-> Förbereder för träning med Early Stopping...")
    VALIDATION_SPLIT_DAYS = 28
    df_model['ds'] = pd.to_datetime(df_model['ds'])
    max_date = df_model['ds'].max()
    val_start_date = max_date - pd.Timedelta(days=VALIDATION_SPLIT_DAYS)

    train_df_full = df_model[df_model['ds'] < val_start_date].copy()
    val_df_full = df_model[df_model['ds'] >= val_start_date].copy()

    if train_df_full.empty or val_df_full.empty:
        print("FATALT: Inte tillräckligt med data för att skapa train/val-split. Använder all data.")
        train_df_full = df_model.copy()
        val_df_full = None 
        
    print(f"-> Träningsdata: {len(train_df_full)} rader")
    print(f"-> Valideringsdata: {len(val_df_full) if val_df_full is not None else 0} rader")
    
    early_stopping_callbacks = [lgb.early_stopping(100, verbose=True)]
    fit_params = { "callbacks": early_stopping_callbacks }

    # === STEG 6B: TRÄNA MODELLERNA ===
    print(f"-> Tränar med {len(features)} features.")
    os.makedirs(config.MODEL_DIR, exist_ok=True)

    QUANTILES_TO_TRAIN = { 'low': 0.10, 'median': 0.50, 'high': 0.90 }

    print("\n--- Tränar VOLYM-modell (Quantile Regression) ---")
    target_col_volume = 'Antal_Samtal'
    
    X_train_vol = train_df_full[features]
    y_train_vol = train_df_full[target_col_volume]
    
    if val_df_full is not None:
        X_val_vol = val_df_full[features]
        y_val_vol = val_df_full[target_col_volume]
        fit_params["eval_set"] = [(X_val_vol, y_val_vol)]
        fit_params["eval_metric"] = "quantile" 
    else:
        fit_params.pop("eval_set", None)

    for name, alpha in QUANTILES_TO_TRAIN.items():
        print(f"  -> Tränar {name}-prognos (alpha={alpha})...")
        model_vol = lgb.LGBMRegressor(
            objective='quantile', alpha=alpha, n_estimators=1000, random_state=42, metric='quantile'
        )
        try:
            model_vol.fit(X_train_vol, y_train_vol, categorical_feature=rensad_final_categorical, **fit_params)
        except Exception as e:
            print(f"FEL vid träning av {name}: {e}")
            continue
        
        model_payload = { 'model': model_vol, 'features': features, 'categorical_features': rensad_final_categorical }
        with open(os.path.join(config.MODEL_DIR, f'final_model_volume_{name}.pkl'), 'wb') as f:
            pickle.dump(model_payload, f)
        print(f"  -> Modell sparad.")

    # --- MODELL 2 & 3: AHT & AWT ---
    for model_name, target_col in targets.items():
        print(f"\n--- Tränar {model_name.upper()}-modell ---")
        final_features_for_model = [f for f in features if f in df_model.columns]
        
        if model_name == 'aht':
            train_df = train_df_full[train_df_full['Antal_Besvarade_Samtal'] > 0].copy()
            if val_df_full is not None: val_df = val_df_full[val_df_full['Antal_Besvarade_Samtal'] > 0].copy()
        else:
            train_df = train_df_full[train_df_full['Antal_Samtal'] > 0].copy()
            if val_df_full is not None: val_df = val_df_full[val_df_full['Antal_Samtal'] > 0].copy()
                
        X_train = train_df[final_features_for_model]
        y_train = train_df[target_col]

        if X_train.empty: continue
            
        if val_df_full is not None and not val_df.empty:
            X_val = val_df[final_features_for_model]
            y_val = val_df[target_col]
            fit_params["eval_set"] = [(X_val, y_val)]
            fit_params["eval_metric"] = "l1"
        else:
            fit_params.pop("eval_set", None)
        
        model = lgb.LGBMRegressor(objective='regression_l1', n_estimators=1000, random_state=42)
        model.fit(X_train, y_train, categorical_feature=rensad_final_categorical, **fit_params)
        
        model_payload = { 'model': model, 'features': final_features_for_model, 'categorical_features': rensad_final_categorical }
        with open(os.path.join(config.MODEL_DIR, f'final_model_{model_name}.pkl'), 'wb') as f:
            pickle.dump(model_payload, f)
        print(f"-> Modell sparad.")
            
    print("\nKLART! Den robusta, SEGMENT-baserade modellen har tränats.")

if __name__ == '__main__':
    train_final_system()
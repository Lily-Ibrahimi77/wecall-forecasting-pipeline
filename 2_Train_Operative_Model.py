"""
================================================================
JOBB 2: Träna Operativ Modell
================================================================
- Sparar 'cat_dtypes' i modellen. Detta är nyckeln för att prognosen
  ska förstå att "Kundtjänst" är samma sak som "Kundtjänst".
"""

import pandas as pd
import numpy as np
import lightgbm as lgb
import pickle
import os
from DataDriven_utils import add_all_features, create_lag_features 
import config
from sqlalchemy import create_engine, text
import sys
import re

def train_final_system():
    print("--- Startar TRÄNING (KATEGORI FIX) ---")
    try:
        mssql_engine = create_engine(config.MSSQL_CONN_STR)
    except Exception as e:
        print(f"FATALT FEL: {e}")
        sys.exit(1)

    # 1. Läs data
    table_name_training = config.TABLE_NAMES['Operative_Training_Data']
    print(f"-> Läser data från {table_name_training}...")
    sql_query = f'''
        SELECT [Created], [CustomerKey], [TjänstTyp], [ChannelType], 
               [TalkTimeInSec], [Duration], [Status]
        FROM [{table_name_training}] 
        WHERE [ChannelType] = 'call'
    '''
    df_raw = pd.read_sql(sql_query, mssql_engine)
    df_raw['Created'] = pd.to_datetime(df_raw['Created']).dt.tz_localize(None)
    
    # Tvätta text
    df_raw['TjänstTyp'] = df_raw['TjänstTyp'].astype(str).str.strip()

    df_raw['is_abandoned'] = (df_raw['Status'].str.lower() == 'callabandoned').astype(int)
    df_raw['is_answered'] = (1 - df_raw['is_abandoned'])
    df_raw['WaitTime'] = (df_raw['Duration'] - df_raw['TalkTimeInSec']).clip(lower=0)

    # Segment
    table_name_segments = config.TABLE_NAMES['Customer_Behavior_Dimension']
    try:
        df_segments = pd.read_sql(f"SELECT CustomerKey, Behavior_Segment FROM [{table_name_segments}]", mssql_engine)
        df_enriched = pd.merge(df_raw, df_segments, on=['CustomerKey'], how='left')
        df_enriched['Behavior_Segment'] = df_enriched['Behavior_Segment'].fillna('Okänt').astype(str).str.strip()
    except:
        df_enriched = df_raw.copy()
        df_enriched['Behavior_Segment'] = 'Okänt'

    # Aggregera
    print("-> Aggregerar till (Timme, TjänstTyp, Segment)...")
    df_hourly_agg = df_enriched.groupby([
        pd.Grouper(key='Created', freq='h'), 'TjänstTyp', 'Behavior_Segment'
    ]).agg(
        Antal_Samtal=('Created', 'count'),
        Total_Samtalstid_Sek=('TalkTimeInSec', 'sum'),
        Total_V_ntetid_Sek=('WaitTime', 'sum'),
        Antal_Besvarade_Samtal=('is_answered', 'sum')
    ).reset_index()

    # Grid
    start_time = df_hourly_agg['Created'].min()
    end_time = df_hourly_agg['Created'].max()
    all_hours = pd.date_range(start=start_time, end=end_time, freq='h')
    unique_combos = df_hourly_agg[['TjänstTyp', 'Behavior_Segment']].drop_duplicates()
    df_master = pd.merge(pd.DataFrame({'Created': all_hours}), unique_combos, how='cross')
    
    df_final = pd.merge(df_master, df_hourly_agg, on=['Created', 'TjänstTyp', 'Behavior_Segment'], how='left')
    fill_cols = ['Antal_Samtal', 'Total_Samtalstid_Sek', 'Total_V_ntetid_Sek', 'Antal_Besvarade_Samtal']
    df_final[fill_cols] = df_final[fill_cols].fillna(0).astype(int)
    
    df_final.rename(columns={'Created': 'ds', 'TjänstTyp': 'Tj_nstTyp'}, inplace=True)
    
    # Features & Lags
    print(f"-> Skapar lags för {len(df_final)} rader...")
    df_final = add_all_features(df_final, ds_col='ds')
    df_final.columns = [re.sub(r'[^A-Za-z0-9_]+', '_', col) for col in df_final.columns]
    df_final = create_lag_features(df_final, group_cols=['Tj_nstTyp', 'Behavior_Segment'], target_col='Antal_Samtal', lags=[1, 7, 14, 28, 364])
    
    # Spara Historik
    tn_hist = config.TABLE_NAMES['Hourly_Aggregated_History']
    df_final.to_sql(f"{tn_hist}_STAGING", mssql_engine, if_exists='replace', index=False, chunksize=50000)
    with mssql_engine.connect() as conn:
        conn.execute(text(f"IF OBJECT_ID('{tn_hist}', 'U') IS NOT NULL DROP TABLE [{tn_hist}]; SELECT * INTO [{tn_hist}] FROM [{tn_hist}_STAGING];"))
        conn.commit()

    # --- 1. VOLYM (DAGLIG) ---
    print("\n--- Tränar Volym (Daglig) ---")
    df_vol_train = df_final.groupby([pd.Grouper(key='ds', freq='D'), 'Tj_nstTyp']).agg({'Antal_Samtal': 'sum'}).reset_index()
    df_vol_train = add_all_features(df_vol_train, ds_col='ds')
    df_vol_train = create_lag_features(df_vol_train, group_cols=['Tj_nstTyp'], target_col='Antal_Samtal', lags=[1, 7, 14, 28, 364])
    df_vol_train = df_vol_train.dropna(subset=['Antal_Samtal_lag_1d'])
    
    raw_base_features = ['veckodag', 'dag_på_året', 'vecka_nr', 'månad', 'kvartal', 'är_arbetsdag']
    vol_features = raw_base_features + [c for c in df_vol_train.columns if '_lag_' in c] + ['Tj_nstTyp']
    
    # ***  Definiera och spara kategorier ***
    cat_features = ['Tj_nstTyp', 'veckodag', 'månad']
    category_dtypes = {} # Här sparar vi kartan
    for c in cat_features:
        df_vol_train[c] = df_vol_train[c].astype('category')
        category_dtypes[c] = df_vol_train[c].dtype # Sparar dtypen

    models_to_train = {
        'low': {'obj': 'quantile', 'alpha': 0.10},
        'median': {'obj': 'quantile', 'alpha': 0.50},
        'high': {'obj': 'quantile', 'alpha': 0.90},
        'operative': {'obj': 'tweedie', 'alpha': None}
    }

    for name, params in models_to_train.items():
        print(f"  -> Tränar Volume_{name}...")
        kw = {'objective': params['obj'], 'n_estimators': 500, 'random_state': 42}
        if params['alpha']: kw['alpha'] = params['alpha']
        
        model = lgb.LGBMRegressor(**kw)
        model.fit(df_vol_train[vol_features], df_vol_train['Antal_Samtal'], categorical_feature=cat_features)
        
        # SPARA MED KATEGORI-KARTA
        path = os.path.join(config.MODEL_DIR, f'final_model_volume_{name}.pkl')
        with open(path, 'wb') as f:
            pickle.dump({
                'model': model, 
                'features': vol_features, 
                'categorical_features': cat_features,
                'cat_dtypes': category_dtypes 
            }, f)

    # --- 2. AHT (SEGMENT) ---
    print("\n--- Tränar AHT (Segment) ---")
    df_aht_train = df_final.groupby([pd.Grouper(key='ds', freq='D'), 'Behavior_Segment']).agg({
        'Antal_Samtal': 'sum', 'Total_Samtalstid_Sek': 'sum', 'Total_V_ntetid_Sek': 'sum', 'Antal_Besvarade_Samtal': 'sum'
    }).reset_index()
    
    df_aht_train['Snitt_Taltid'] = np.where(df_aht_train['Antal_Besvarade_Samtal'] > 0, 
                                            df_aht_train['Total_Samtalstid_Sek'] / df_aht_train['Antal_Besvarade_Samtal'], 0)
    df_aht_train['Snitt_Vantetid'] = np.where(df_aht_train['Antal_Samtal'] > 0, 
                                              df_aht_train['Total_V_ntetid_Sek'] / df_aht_train['Antal_Samtal'], 0)
    
    df_aht_train = add_all_features(df_aht_train, ds_col='ds')
    aht_features = raw_base_features + ['Behavior_Segment']
    cat_aht = ['Behavior_Segment', 'veckodag', 'månad']
    
    aht_dtypes = {}
    for c in cat_aht:
        df_aht_train[c] = df_aht_train[c].astype('category')
        aht_dtypes[c] = df_aht_train[c].dtype # Spara karta

    df_aht_clean = df_aht_train[df_aht_train['Antal_Samtal'] > 0].copy()

    # AHT Model
    print("  -> Tränar AHT...")
    model_aht = lgb.LGBMRegressor(objective='regression', n_estimators=500, random_state=42)
    model_aht.fit(df_aht_clean[aht_features], df_aht_clean['Snitt_Taltid'], categorical_feature=cat_aht)
    with open(os.path.join(config.MODEL_DIR, 'final_model_aht.pkl'), 'wb') as f:
        pickle.dump({'model': model_aht, 'features': aht_features, 'categorical_features': cat_aht, 'cat_dtypes': aht_dtypes}, f)

    # AWT Model
    print("  -> Tränar AWT...")
    model_awt = lgb.LGBMRegressor(objective='regression', n_estimators=500, random_state=42)
    model_awt.fit(df_aht_clean[aht_features], df_aht_clean['Snitt_Vantetid'], categorical_feature=cat_aht)
    with open(os.path.join(config.MODEL_DIR, 'final_model_awt.pkl'), 'wb') as f:
        pickle.dump({'model': model_awt, 'features': aht_features, 'categorical_features': cat_aht, 'cat_dtypes': aht_dtypes}, f)

    print("\n-> ALLA MODELLER TRÄNADE & SPARADE (MED KATEGORI-FIX)!")

if __name__ == '__main__':
    train_final_system()
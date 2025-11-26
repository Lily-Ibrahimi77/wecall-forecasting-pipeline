"""
================================================================
JOBB 3: Skapa Operativ Prognos (BUSINESS HOURS FIX)
================================================================
- Öppettider Mån-Fre 06:00 - 18:00.
- All volym utanför dessa tider sätts till 0.
- Dagsvolymen fördelas om så den enbart hamnar på öppettiderna.
"""

import pandas as pd
import numpy as np
import pickle
import os
from datetime import datetime, timedelta
from DataDriven_utils import add_all_features
import config
from sqlalchemy import create_engine, text 
import sys
import re

# --- FUNKTIONER ---
def create_daily_lags(df, group_cols, target_col, lags):
    df_out = df.copy()
    df_out = df_out.sort_values(by=group_cols + ['ds'])
    g = df_out.groupby(group_cols)
    for lag_days in lags:
        col_name = f'{target_col}_lag_{lag_days}d'
        df_out[col_name] = g[target_col].shift(lag_days)
    return df_out

def load_model_payload(model_path: str):
    if not os.path.exists(model_path): return None
    with open(model_path, 'rb') as f: return pickle.load(f)

def get_forecast_start_date(engine) -> datetime:
    if config.RUN_MODE == 'VALIDATION':
        if 'TRAINING_END_DATE' in config.VALIDATION_SETTINGS:
            return pd.to_datetime(config.VALIDATION_SETTINGS['TRAINING_END_DATE']).normalize() + pd.Timedelta(days=1)
    return (pd.Timestamp.now() + pd.Timedelta(days=1)).normalize()

def calculate_hourly_shape(engine, services_list, target_col):
    hist_table = config.TABLE_NAMES['Hourly_Aggregated_History']
    try:
        df = pd.read_sql(f"SELECT ds, Tj_nstTyp, Antal_Samtal FROM [{hist_table}] WHERE Antal_Samtal > 0", engine)
    except:
        return pd.DataFrame() 

    df['ds'] = pd.to_datetime(df['ds']).dt.tz_localize(None)
    df[target_col] = df['Tj_nstTyp'].astype(str).str.strip()
    
    df = df[df[target_col].isin(services_list)]
    df = add_all_features(df, ds_col='ds')
    df_daily = df.groupby(['datum', target_col])['Antal_Samtal'].sum().reset_index(name='Daily_Total')
    df = pd.merge(df, df_daily, on=['datum', target_col])
    df['Hourly_Proportion'] = df['Antal_Samtal'] / df['Daily_Total']
    
    df_shape = df.groupby(['veckodag', 'timme', target_col])['Hourly_Proportion'].mean().reset_index(name='Avg_Hourly_Proportion')
    norm = df_shape.groupby(['veckodag', target_col])['Avg_Hourly_Proportion'].transform('sum')
    df_shape['Avg_Hourly_Proportion'] = df_shape['Avg_Hourly_Proportion'] / norm
    return df_shape[['veckodag', 'timme', target_col, 'Avg_Hourly_Proportion']]

# --- HUVUDPROGRAM ---
def create_final_forecast():
    print("--- JOBB 3 STARTAR (BUSINESS HOURS) ---")
    mssql_engine = create_engine(config.MSSQL_CONN_STR)
    
    tn_arc = config.TABLE_NAMES['Forecast_Archive']
    tn_op = config.TABLE_NAMES['Operative_Forecast']
    
    # 1. SCORCHED EARTH CLEANUP
    print(f"-> Rensar tabeller totalt...")
    with mssql_engine.connect() as conn:
        conn.execute(text(f"IF OBJECT_ID('{tn_op}', 'U') IS NOT NULL DROP TABLE [{tn_op}]"))
        conn.execute(text(f"IF OBJECT_ID('{tn_arc}', 'U') IS NOT NULL DROP TABLE [{tn_arc}]"))
        conn.commit()

    forecast_start = get_forecast_start_date(mssql_engine)
    
    payload_vol = load_model_payload(os.path.join(config.MODEL_DIR, 'final_model_volume_operative.pkl'))
    model_vol = payload_vol.get('model') if payload_vol else None
    feats_vol = payload_vol.get('features') if payload_vol else []
    cat_vol = payload_vol.get('categorical_features') if payload_vol else []
    dtypes_vol = payload_vol.get('cat_dtypes', {}) if payload_vol else {}

    hist_table = config.TABLE_NAMES['Hourly_Aggregated_History']
    lookback = forecast_start - pd.Timedelta(days=370)
    
    print(f"-> Hämtar historik tom {forecast_start}...")
    q_hist = f"""
        SELECT CONVERT(date, ds) as ds, Tj_nstTyp, Behavior_Segment, 
               SUM(Antal_Samtal) as Antal_Samtal
        FROM [{hist_table}]
        WHERE ds >= '{lookback.strftime('%Y-%m-%d')}'
        AND ds < '{forecast_start.strftime('%Y-%m-%d')} 23:59:59' 
        GROUP BY CONVERT(date, ds), Tj_nstTyp, Behavior_Segment
    """
    df_hist_raw = pd.read_sql(q_hist, mssql_engine)
    df_hist_raw['ds'] = pd.to_datetime(df_hist_raw['ds']).dt.tz_localize(None).dt.normalize()
    df_hist_raw['Tj_nstTyp'] = df_hist_raw['Tj_nstTyp'].astype(str).str.strip()
    
    # Stat
    df_hist_temp = add_all_features(df_hist_raw.copy(), ds_col='ds')
    recent_cutoff = forecast_start - pd.Timedelta(days=35)
    df_recent = df_hist_temp[df_hist_temp['ds'] >= recent_cutoff]
    df_stats = df_recent.groupby(['Tj_nstTyp', 'veckodag'])['Antal_Samtal'].mean().reset_index(name='Stat_Avg')
    
    # Prognos
    df_vol_hist = df_hist_raw.groupby(['ds', 'Tj_nstTyp'])['Antal_Samtal'].sum().reset_index()
    df_vol_hist = df_vol_hist[df_vol_hist['ds'] < forecast_start].copy()

    active_services = df_hist_raw['Tj_nstTyp'].unique()
    hier = df_hist_raw[['Tj_nstTyp', 'Behavior_Segment']].drop_duplicates()
    horizon = config.HOLDOUT_PERIOD_DAYS if config.RUN_MODE == 'VALIDATION' else config.FORECAST_HORIZON_DAYS
    future_dates = pd.date_range(start=forecast_start, periods=horizon, freq='D')
    
    print(f"-> Startar Rullande Prognos ({horizon} dagar)...")
    df_master = df_vol_hist.copy()
    
    for i, current_date in enumerate(future_dates):
        weekday = current_date.weekday()
        df_today_skeleton = pd.DataFrame({'ds': [current_date] * len(active_services), 'Tj_nstTyp': active_services})
        df_today_skeleton['veckodag'] = weekday
        df_today_skeleton = pd.merge(df_today_skeleton, df_stats, on=['Tj_nstTyp', 'veckodag'], how='left')
        df_today_skeleton['Stat_Avg'] = df_today_skeleton['Stat_Avg'].fillna(0)
        
        df_run = pd.concat([df_master, df_today_skeleton], ignore_index=True).sort_values(by=['Tj_nstTyp', 'ds'])
        df_run = add_all_features(df_run, ds_col='ds')
        df_run.columns = [re.sub(r'[^A-Za-z0-9_]+', '_', col) for col in df_run.columns]
        df_run = create_daily_lags(df_run, ['Tj_nstTyp'], 'Antal_Samtal', [1, 7, 14, 28, 364])
        
        df_today_features = df_run[df_run['ds'] == current_date].copy()
        
        preds_op = np.zeros(len(df_today_features))
        
        if model_vol:
            try:
                X = df_today_features.reindex(columns=feats_vol, fill_value=0)
                for c in cat_vol:
                    if c in X.columns and c in dtypes_vol: X[c] = X[c].astype(dtypes_vol[c])
                preds_op = model_vol.predict(X[feats_vol])
            except: pass

        final_preds = []
        stat_avgs = df_today_skeleton.sort_values(by='Tj_nstTyp')['Stat_Avg'].values
        lags_7 = df_today_features.sort_values(by='Tj_nstTyp')['Antal_Samtal_lag_7d'].fillna(0).values

        for p_op, stat, l7 in zip(preds_op, stat_avgs, lags_7):
            if p_op > 5: base_guess = (p_op * 0.5) + (stat * 0.5)
            else: base_guess = stat
            
            if base_guess < (l7 * 0.5) and l7 > 10: final_guess = l7
            else: final_guess = base_guess
            
            final_preds.append(max(0, final_guess))
        
        df_today_skeleton['Antal_Samtal'] = np.round(final_preds).astype(int)
        df_today_skeleton['Prognos_Låg'] = (df_today_skeleton['Antal_Samtal'] * 0.8).astype(int)
        df_today_skeleton['Prognos_Hög'] = (df_today_skeleton['Antal_Samtal'] * 1.2).astype(int)
        
        df_master = pd.concat([df_master, df_today_skeleton[['ds', 'Tj_nstTyp', 'Antal_Samtal', 'Prognos_Låg', 'Prognos_Hög']]], ignore_index=True)

    # --- OUTPUT ---
    df_forecast_final = df_master[df_master['ds'] >= forecast_start].copy()
    df_forecast_final.rename(columns={'Antal_Samtal': 'Prognos_Volym'}, inplace=True)
    
    
    TARGET_TOTAL = df_forecast_final['Prognos_Volym'].sum()
    print(f"-> MÅL-VOLYM (Daglig): {int(TARGET_TOTAL)} samtal.")

    # Tim-fördelning
    df_base = pd.merge(df_forecast_final, hier, on='Tj_nstTyp', how='left')
    df_shape = calculate_hourly_shape(mssql_engine, list(active_services), 'Tj_nstTyp')
    
    df_hourly_base = pd.DataFrame()
    for dt in future_dates:
        hours = pd.DataFrame({'ds': pd.date_range(dt, periods=24, freq='h')})
        hours['datum'] = hours['ds'].dt.normalize()
        df_hourly_base = pd.concat([df_hourly_base, hours])
        
    df_res = pd.merge(df_hourly_base, df_base, left_on='datum', right_on='ds', suffixes=('_h', ''))
    df_res = add_all_features(df_res, ds_col='ds_h')
    df_res = pd.merge(df_res, df_shape, on=['veckodag', 'timme', 'Tj_nstTyp'], how='left')
    
    # --- NY LOGIK: ÖPPETTIDER & NORMALISERING ---
    
    # 1. Sätt bas-vikt (0 istället för 1/24 för att inte fylla nätter med skräp)
    df_res['Shape_Weight'] = df_res['Avg_Hourly_Proportion'].fillna(0)
    
    # 2. HÅRT FILTER: Stängt Helger + Nätter (Före 06:00, Efter 18:00)
    # 0=Mån, 4=Fre, 5=Lör, 6=Sön
    mask_closed = (
        (df_res['ds_h'].dt.weekday >= 5) |       # Lördag/Söndag
        (df_res['ds_h'].dt.hour >= 18) |         # Stänger 18:00
        (df_res['ds_h'].dt.hour < 6)             # Öppnar 06:00 
    )
    df_res.loc[mask_closed, 'Shape_Weight'] = 0
    
    # 3. Normalisera Vikterna (Så att summan blir 1 per dag, men BARA på öppettider)
    df_res['Datum_Dag'] = df_res['ds_h'].dt.normalize()
    daily_weights = df_res.groupby(['Datum_Dag', 'Tj_nstTyp'])['Shape_Weight'].transform('sum')
    
    # Undvik division med 0 (om en kö(tjänst) används inte längre)
    df_res['Normalized_Weight'] = np.where(daily_weights > 0, df_res['Shape_Weight'] / daily_weights, 0)

    # 4. Fördela Volymen med de nya vikterna
    df_res['Temp_Vol'] = df_res['Prognos_Volym'] * df_res['Normalized_Weight']
    df_res['Temp_Low'] = df_res['Prognos_Låg'] * df_res['Normalized_Weight']
    df_res['Temp_High'] = df_res['Prognos_Hög'] * df_res['Normalized_Weight']
    
    # 5. Summera och Skala (Mathematical Safety Net - Sista kollen)
    sums = df_res.groupby(['Datum_Dag', 'Tj_nstTyp'])[['Temp_Vol', 'Temp_Low', 'Temp_High']].transform('sum')
    
    df_res['Scale_Vol'] = np.where(sums['Temp_Vol'] > 0, df_res['Prognos_Volym'] / sums['Temp_Vol'], 0)
    df_res['Scale_Low'] = np.where(sums['Temp_Low'] > 0, df_res['Prognos_Låg'] / sums['Temp_Low'], 0)
    df_res['Scale_High'] = np.where(sums['Temp_High'] > 0, df_res['Prognos_Hög'] / sums['Temp_High'], 0)
    
    df_res['Prognos_Antal_Samtal'] = (df_res['Temp_Vol'] * df_res['Scale_Vol']).round().astype(int)
    df_res['Prognos_Låg'] = (df_res['Temp_Low'] * df_res['Scale_Low']).round().astype(int)
    df_res['Prognos_Hög'] = (df_res['Temp_High'] * df_res['Scale_High']).round().astype(int)
    
    # Kontrollsumma
    FINAL_SUM = df_res['Prognos_Antal_Samtal'].sum()
    print(f"-> SLUT-VOLYM (Öppettider Anpassade): {int(FINAL_SUM)}")
    
    # Färdigställ
    final_cols = ['ds_h', 'Tj_nstTyp', 'Behavior_Segment', 'Prognos_Antal_Samtal', 'Prognos_Låg', 'Prognos_Hög']
    df_out = df_res[final_cols].rename(columns={'ds_h':'DatumTid', 'Tj_nstTyp':'TjänstTyp'})
    df_out['Prognos_Snitt_Taltid_Sek'] = 180 
    df_out['ForecastRunDate'] = pd.to_datetime(config.VALIDATION_SETTINGS['FORECAST_RUN_DATE_SQL']) if config.RUN_MODE == 'VALIDATION' else datetime.now().date()
    
    # --- SPARNING ---
    print(f"-> Sparar till {tn_op} (LIVE)...")
    df_out.to_sql(tn_op, mssql_engine, if_exists='replace', index=False)
    
    print(f"-> Sparar till {tn_arc} (ARKIV)...")
    df_out.to_sql(tn_arc, mssql_engine, if_exists='replace', index=False)

    print("-> JOBB 3 KLART. Prognosen är nu filtrerad för öppettider.")

if __name__ == '__main__':
    create_final_forecast()
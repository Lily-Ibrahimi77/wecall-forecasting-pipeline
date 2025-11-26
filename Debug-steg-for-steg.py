"""
================================================================
DEBUG TOOL: Spåra Volym-Explosionen
================================================================
Detta skript kör logiken från Jobb 3 steg-för-steg och
kontrollräknar volymen efter varje .merge().
"""

import pandas as pd
import numpy as np
import config
from sqlalchemy import create_engine
from DataDriven_utils import add_all_features
import sys

def debug_pipeline():
    print("\n--- STARTAR STEG-FÖR-STEG DEBUG ---")
    mssql_engine = create_engine(config.MSSQL_CONN_STR)
    
    # 1. Hämta Facit (Vad vi borde ha)
    # Vi simulerar df_forecast_final genom att hämta skapade prognos (Jobb 3 output)
    # Men vi summerar den INNAN expansionen teoretiskt sett.
    
    # För att göra detta rätt, låt oss titta på vad Jobb 3 sa:
    target_vol = 9454
    print(f"MÅL-VOLYM (Från Jobb 3 log): {target_vol}")
    
    # 2. Hämta data som behövs för expansionen
    print("-> Hämtar hjälp-data (Hierarki, Shape)...")
    
    hist_table = config.TABLE_NAMES['Hourly_Aggregated_History']
    df_hist_raw = pd.read_sql(f"SELECT Tj_nstTyp, Behavior_Segment, ds, Antal_Samtal FROM [{hist_table}]", mssql_engine)
    df_hist_raw['Tj_nstTyp'] = df_hist_raw['Tj_nstTyp'].astype(str).str.strip()
    
    # Hierarki
    hier = df_hist_raw[['Tj_nstTyp', 'Behavior_Segment']].drop_duplicates()
    print(f"   Hierarki rader: {len(hier)}")
    
    # Shape
    # (Vi gör en förenklad shape-calc här för att matcha Jobb 3)
    df_hist_raw['ds'] = pd.to_datetime(df_hist_raw['ds'])
    df_shape_base = add_all_features(df_hist_raw.copy(), ds_col='ds')
    
    # Räkna ut shape
    daily = df_shape_base.groupby(['datum', 'Tj_nstTyp'])['Antal_Samtal'].sum().reset_index(name='Daily')
    df_shape_base = pd.merge(df_shape_base, daily, on=['datum', 'Tj_nstTyp'])
    df_shape_base['Prop'] = df_shape_base['Antal_Samtal'] / df_shape_base['Daily']
    df_shape = df_shape_base.groupby(['veckodag', 'timme', 'Tj_nstTyp'])['Prop'].mean().reset_index(name='Avg_Hourly_Proportion')
    
    # Normalisera shape
    norm = df_shape.groupby(['veckodag', 'Tj_nstTyp'])['Avg_Hourly_Proportion'].transform('sum')
    df_shape['Avg_Hourly_Proportion'] = df_shape['Avg_Hourly_Proportion'] / norm
    print(f"   Shape rader: {len(df_shape)}")

    # 3. Skapa en Dummy "Daily Forecast" (df_forecast_final)
    # Vi hämtar den sparade prognosen och grupperar ihop den till dags-nivå för att simulera startpunkten.
    tn_op = config.TABLE_NAMES['Operative_Forecast']
    df_out_saved = pd.read_sql(f"SELECT * FROM [{tn_op}]", mssql_engine)
    
    # "Backa bandet" till daglig nivå
    df_forecast_final = df_out_saved.groupby(['DatumTid', 'TjänstTyp'])['Prognos_Antal_Samtal'].sum().reset_index()
    df_forecast_final['ds'] = pd.to_datetime(df_forecast_final['DatumTid']).dt.normalize()
    # Gruppera helt till dag (ta bort timmar)
    df_forecast_final = df_forecast_final.groupby(['ds', 'TjänstTyp'])['Prognos_Antal_Samtal'].sum().reset_index()
    df_forecast_final.rename(columns={'Prognos_Antal_Samtal': 'Prognos_Volym', 'TjänstTyp': 'Tj_nstTyp'}, inplace=True)
    
    current_vol = df_forecast_final['Prognos_Volym'].sum()
    print(f"\n--- STEG 1: STARTLÄGE ---")
    print(f"Volym i df_forecast_final: {int(current_vol)}")
    
    if abs(current_vol - 13694) < 100:
        print("VARNING: Din sparade data är redan 'exploderad' till 13694.") 
        print("Vi måste simulera vad Jobb 3 hade innan sparning.")
        # Vi skalar ner den till 9454 för att testa logiken
        scale = 9454 / 13694
        df_forecast_final['Prognos_Volym'] = df_forecast_final['Prognos_Volym'] * scale
        print(f"--> Skalade ner till {int(df_forecast_final['Prognos_Volym'].sum())} för simulering.")


    # 1. Merge Hierarki
    print(f"\n--- STEG 2: SEGMENT EXPANSION (Hierarki) ---")
    df_base = pd.merge(df_forecast_final, hier, on='Tj_nstTyp', how='left')
    
    # KONTROLL: Har volymen ändrats? (Den borde dubblas/tripplas här pga segment, men vi kollar rader)
    print(f"Rader före: {len(df_forecast_final)}, Rader efter: {len(df_base)}")
    # Notera: Här har vi inte fördelat volymen än, så summan av 'Prognos_Volym' kommer vara duplicerad.
    # Det är OK just nu, så länge vi vet om det.
    dup_factor = len(df_base) / len(df_forecast_final)
    print(f"Duplicerings-faktor: {dup_factor:.2f}x (Pga flera segment per tjänst)")

    # 2. Skapa Timmar (Hourly Base)
    print(f"\n--- STEG 3: TIM-EXPANSION (24h) ---")
    future_dates = df_forecast_final['ds'].unique()
    df_hourly_base = pd.DataFrame()
    for dt in future_dates:
        hours = pd.DataFrame({'ds': pd.date_range(dt, periods=24, freq='h')})
        hours['datum'] = hours['ds'].dt.normalize()
        df_hourly_base = pd.concat([df_hourly_base, hours])
    
    df_res = pd.merge(df_hourly_base, df_base, left_on='datum', right_on='ds', suffixes=('_h', ''))
    
    print(f"Rader i df_res: {len(df_res)}")
    # Här ska volymen vara: Startvolym * 24 * SegmentFaktor. 
    # Vi kollar inte summan än för den är inte fördelad.

    # 3. Merge Shape
    print(f"\n--- STEG 4: SHAPE MERGE ---")
    df_res = add_all_features(df_res, ds_col='ds_h') # Lägg till 'timme', 'veckodag'
    
    rows_before_shape = len(df_res)
    df_res = pd.merge(df_res, df_shape, on=['veckodag', 'timme', 'Tj_nstTyp'], how='left')
    rows_after_shape = len(df_res)
    
    print(f"Rader före Shape: {rows_before_shape}")
    print(f"Rader efter Shape: {rows_after_shape}")
    
    if rows_after_shape > rows_before_shape:
        print("!!! VARNING: SHAPE-MERGEN SKAPADE DUBBLETTER !!!")
        print("Detta är troligen orsaken. Shape-tabellen är inte unik per Timme/Tjänst.")
        diff = rows_after_shape - rows_before_shape
        print(f"Antal extra rader: {diff}")
    else:
        print("Shape-mergen ser OK ut (inga nya rader).")

    # 4. Fördelnings-Matte
    print(f"\n--- STEG 5: BERÄKNING AV SLUTVOLYM ---")
    df_res['Avg_Hourly_Proportion'] = df_res['Avg_Hourly_Proportion'].fillna(1/24)
    df_res['Prognos_AHT'] = 180
    
    # Här är din logik:
    seg_counts = df_res.groupby(['ds_h', 'Tj_nstTyp'])['Behavior_Segment'].transform('size')
    
    # Debugga seg_counts
    avg_seg_count = seg_counts.mean()
    print(f"Snitt antal segment per tjänst (seg_counts): {avg_seg_count:.2f}")

    # Beräkna
    df_res['Final_Volym'] = ((df_res['Prognos_Volym'] / seg_counts) * df_res['Avg_Hourly_Proportion']).fillna(0)
    
    # Slutsumma (Innan avrundning)
    raw_sum = df_res['Final_Volym'].sum()
    print(f"Total Volym (Raw): {raw_sum:.2f}")
    
    # Slutsumma (Efter avrundning)
    df_res['Final_Volym_Round'] = df_res['Final_Volym'].round().astype(int)
    round_sum = df_res['Final_Volym_Round'].sum()
    print(f"Total Volym (Rounded): {round_sum}")
    
    diff = round_sum - target_vol
    print(f"\n--- RESULTAT ---")
    print(f"Start: {target_vol}")
    print(f"Slut:  {round_sum}")
    print(f"Diff:  {diff}")
    
    if abs(diff) > 1000:
        print("\nANALYS:")
        print("Volymen exploderade.")
        if rows_after_shape > rows_before_shape:
            print("ORSAK: Dubbletter i Shape-tabellen.")
        elif raw_sum > target_vol * 1.1:
            print("ORSAK: 'seg_counts' delar inte upp volymen tillräckligt mycket.")
            print("Kanske matchar inte 'ds_h' och 'Tj_nstTyp' exakt i groupby?")
        else:
            print("ORSAK: Avrundningsfel (osannolikt för 4000 samtal) eller annan logik.")

if __name__ == '__main__':
    debug_pipeline()
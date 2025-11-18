"""
================================================================
VERKTYGSLÅDA (DataDriven_utils.py)
================================================================
*** KORRIGERAD VERSION (av Data Scientist) ***
- NY FUNKTION: get_current_time() för centraliserad tid.
- add_all_features: Tvingar 'naive' datetimes (tar bort tidszon).
- get_customer_data: Använder den robusta logiken från 1_Extract.
"""

import pandas as pd
import numpy as np
from workalendar.europe import Sweden
import config
from sqlalchemy import create_engine
import sys
import traceback 
from datetime import datetime
import pytz 
import pandas as pd
import numpy as np

def create_lag_features(df, group_cols, target_col, lags):
    """ 
    Skapar lag-features grupperat (t.ex. per segment).
    Kräver att 'ds' (datetime) finns i df.
    """
    df_out = df.copy()
    if 'ds' not in df_out.columns:
        print("FEL i create_lag_features: 'ds'-kolumn saknas.")
        return df_out
        
    df_out = df_out.sort_values(by=group_cols + ['ds'])
    
    # Säkerställ att group_cols är en lista, även om det bara är en
    if not isinstance(group_cols, list):
        group_cols = [group_cols]
        
    g = df_out.groupby(group_cols)
    
    for lag_days in lags:
        lag_hours = lag_days * 24
        col_name = f'{target_col}_lag_{lag_days}d'
        print(f"  -> Skapar {col_name} (shift {lag_hours}h)...")
        try:
            # Använd shift() inom groupby för att förhindra dataläckage mellan segment
            df_out[col_name] = g[target_col].shift(lag_hours)
        except Exception as e:
            print(f"    VARNING: Kunde inte skapa lag feature {col_name}: {e}")
            df_out[col_name] = np.nan
            
    return df_out


def get_current_time() -> datetime:
    """
    NY FUNKTION:
    Hämtar nuvarande tid, tvingad till projektets tidszon
    (definierad i config.py) och returnerar den som 'naive' (utan tzinfo).
    Detta är den ENDA funktionen som ska användas istället för datetime.now()
    """
    try:
        # Hämta tidszon från config
        tz = pytz.timezone(config.PROJECT_TIMEZONE)
        # Hämta nuvarande tid i den tidszonen
        tz_aware_time = datetime.now(tz)
        # Returnera 'naive' tid (ta bort tzinfo)
        return tz_aware_time.replace(tzinfo=None)
    except Exception:
        # Fallback om pytz eller config saknas
        print(f"VARNING: Kunde inte ladda tidszon '{config.PROJECT_TIMEZONE}'. Använder serverns lokala tid.", file=sys.stderr)
        return datetime.now().replace(tzinfo=None)

def map_queue_to_service(queue_id):
    """ Mappar ett QueueId till en TjänstTyp baserat på config. """
    return config.QUEUE_TO_SERVICETYPE_MAP.get(queue_id, 'Okänd Kö')

def get_customer_data(engine=None) -> pd.DataFrame:
    """
    Hämtar kunddata. NU FRÅN BRONZE (MSSQL) ISTÄLLET FÖR BILLING DB.
    """
    print("-> Hämtar central kundmappning från BRONZE (MSSQL)...", file=sys.stderr)
    
    try:
        # Om ingen motor skickas med, skapa en MSSQL-motor (inte Billing!)
        if engine is None:
            engine = create_engine(config.MSSQL_CONN_STR)
        
        # Använd tabellnamnet från config.BRONZE_TABLES
        customer_table = config.BRONZE_TABLES['customers']

        if hasattr(config, 'EXCLUDE_CUSTOMER_IDS') and config.EXCLUDE_CUSTOMER_IDS:
            exclude_ids_str = ", ".join([f"'{str(cid)}'" for cid in config.EXCLUDE_CUSTOMER_IDS])
            exclude_sql = f"AND t1.CustomerId NOT IN ({exclude_ids_str})"
        else:
            exclude_sql = ""

        # SQL-frågan (anpassad för Bronze i MSSQL)
        customer_query = f"""
            SELECT 
                t1.LandingNumber, t1.Name, t1.OrganisationNumber, t1.ParentId,
                t1.CustomerId, t1.BillingType, t2.Name AS ParentName,
                t2.OrganisationNumber AS ParentOrganisationNumber 
            FROM [{customer_table}] AS t1
            LEFT JOIN [{customer_table}] AS t2 ON t1.ParentId = t2.CustomerId
            WHERE t1.LandingNumber IS NOT NULL AND t1.LandingNumber != '' {exclude_sql}
        """
        
        df_customer_mapping_raw = pd.read_sql(customer_query, engine)
        
        # ... (Resten av funktionen med list-split och städning är IDENTISK med förut) ...
        # (Förkortar här för att spara plats, kopiera logiken från din gamla fil)
        
        df_customer_mapping_raw['LandingNumber_list'] = df_customer_mapping_raw['LandingNumber'].astype(str).str.split(',')
        df_customer_mapping_exploded = df_customer_mapping_raw.explode('LandingNumber_list')
        df_customer_mapping_exploded['LandingNumber_clean'] = df_customer_mapping_exploded['LandingNumber_list'].str.strip()
        df_customer_mapping = df_customer_mapping_exploded.drop(['LandingNumber', 'LandingNumber_list'], axis=1)
        df_customer_mapping = df_customer_mapping.rename(columns={'LandingNumber_clean': 'LandingNumber'})
        df_customer_mapping = df_customer_mapping[df_customer_mapping['LandingNumber'] != '']

        def clean_org_nr(series):
            clean = series.astype(str).str.extract(r'^([\d-]+)', expand=False)
            clean = clean.str.replace('-', '', regex=False).str.strip()
            clean.replace(['', 'None', 'nan', 'NULL', 'Okänt'], pd.NA, inplace=True)
            return clean

        df_customer_mapping['OrgNr_Clean'] = clean_org_nr(df_customer_mapping['OrganisationNumber'])
        df_customer_mapping['ParentOrgNr_Clean'] = clean_org_nr(df_customer_mapping['ParentOrganisationNumber'])
        df_customer_mapping['OrgNr_Clean'] = df_customer_mapping['OrgNr_Clean'].fillna(df_customer_mapping['ParentOrgNr_Clean'])
        
        df_customer_mapping['CustomerId_str'] = df_customer_mapping['CustomerId'].astype(str).str.strip()
        df_customer_mapping['OrgNr_Clean'] = df_customer_mapping['OrgNr_Clean'].fillna('CUSTID_' + df_customer_mapping['CustomerId_str'])
        
        df_customer_mapping['Name_clean_key'] = df_customer_mapping['Name'].astype(str).str.replace(r'[^A-Za-z0-9]+', '', regex=True)
        df_customer_mapping['OrgNr_Clean'] = df_customer_mapping['OrgNr_Clean'].fillna('NAME_' + df_customer_mapping['Name_clean_key'])
            
        df_customer_mapping.rename(columns={"OrgNr_Clean": "CustomerKey"}, inplace=True)
        
        cols_to_drop = ['OrganisationNumber', 'ParentOrganisationNumber', 'ParentOrgNr_Clean', 'CustomerId_str', 'Name_clean_key']
        cols_to_drop = [col for col in cols_to_drop if col in df_customer_mapping.columns]
        df_final_customers = df_customer_mapping.drop(columns=cols_to_drop)
        
        return df_final_customers

    except Exception as e:
        print(f"FATALT FEL i get_customer_data: {e}", file=sys.stderr)
        return None

def get_holidays(years: list) -> pd.DataFrame:
    cal = Sweden()
    all_holidays = []
    for year in years:
        all_holidays.extend(cal.holidays(year))
    if not all_holidays:
        return pd.DataFrame(columns=['ds', 'holiday'])
    holidays_df = pd.DataFrame(all_holidays, columns=['ds', 'holiday'])
    holidays_df['ds'] = pd.to_datetime(holidays_df['ds'])
    return holidays_df

def categorize_customer(df: pd.DataFrame) -> pd.DataFrame:
    if 'Name' not in df.columns:
        print("VARNING: 'Name'-kolumn saknas, kan inte kategorisera kunder.", file=sys.stderr)
        df['kategori'] = 'Okänd'
        df['är_dotterbolag'] = 0
        return df
    df['Name'] = df['Name'].astype(str)
    df['kategori'] = 'Övrigt' 
    for category, keywords in config.CUSTOMER_CATEGORIES.items():
        keyword_regex = '|'.join(keywords)
        if keyword_regex:
            df.loc[df['Name'].str.contains(keyword_regex, case=False, na=False), 'kategori'] = category
    if 'ParentId' not in df.columns:
         df['är_dotterbolag'] = 0
    else:
        df['är_dotterbolag'] = (~df['ParentId'].isin([0, np.nan, None, ''])).astype(int)
    return df

def add_all_features(df: pd.DataFrame, ds_col: str = 'ds') -> pd.DataFrame:
    """
    Skapar alla nödvändiga tids-features från en datumkolumn ('ds').
    *** KORRIGERAD: Tvingar 'naive' datetime och lägger till namn. ***
    """
    # KORRIGERING: Tvinga bort all tidszons-information
    df[ds_col] = pd.to_datetime(df[ds_col]).dt.tz_localize(None)
    
    # Grundläggande tidsattribut
    df['timme'] = df[ds_col].dt.hour
    df['minut'] = df[ds_col].dt.minute
    df['veckodag'] = df[ds_col].dt.weekday  # Måndag=0, Söndag=6
    df['dag_på_året'] = df[ds_col].dt.dayofyear
    df['vecka_nr'] = df[ds_col].dt.isocalendar().week.astype(int)
    df['månad'] = df[ds_col].dt.month
    df['kvartal'] = df[ds_col].dt.quarter
    df['år'] = df[ds_col].dt.year
    df['datum'] = df[ds_col].dt.date

    # === KORRIGERING: Lade till namn-mappningar ===
    dag_map = {0: 'Mån', 1: 'Tis', 2: 'Ons', 3: 'Tor', 4: 'Fre', 5: 'Lör', 6: 'Sön'}
    month_map = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'Maj', 6: 'Jun', 
                 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Okt', 11: 'Nov', 12: 'Dec'}

    df['veckodag_namn'] = df['veckodag'].map(dag_map)
    df['månad_namn'] = df['månad'].map(month_map)
    # === SLUT PÅ KORRIGERING ===

    # Hämta helgdagar
    unique_years = df[ds_col].dt.year.unique().tolist()
    if unique_years:
        holidays_df = get_holidays(unique_years)
        holidays_dates = holidays_df['ds'].dt.date
    else:
        holidays_dates = pd.Series(dtype='datetime64[ns]').dt.date

    is_weekday = df['veckodag'] < 5
    is_not_holiday = ~df[ds_col].dt.date.isin(holidays_dates)
    df['är_arbetsdag'] = (is_weekday & is_not_holiday).astype(int)
    
    previous_day_is_closed = ((df['veckodag'] == 0) | df[ds_col].dt.date.isin(holidays_dates + pd.Timedelta(days=1)))
    df['är_dagen_efter_stängt'] = (previous_day_is_closed & (df['är_arbetsdag'] == 1)).astype(int)
    
    time_as_float = df['timme'] + df['minut'] / 60.0
    
    df['är_tidig_morgon'] = ((time_as_float >= 6.5) & (time_as_float < 8)).astype(int)
    df['är_förmiddag'] = ((time_as_float >= 8) & (time_as_float < 11)).astype(int)
    df['är_lunchtid'] = ((time_as_float >= 11) & (time_as_float < 13)).astype(int)
    df['är_eftermiddag'] = ((time_as_float >= 13) & (time_as_float < 17.5)).astype(int)

    df['year_sin'] = np.sin(2 * np.pi * df['dag_på_året'] / 365.25)
    df['year_cos'] = np.cos(2 * np.pi * df['dag_på_året'] / 365.25)
    
    return df
"""
================================================================
KONFIGURATIONSFIL (config.py)
================================================================
Detta är den centrala hjärnan i systemet.
ALLA databasanslutningar, filvägar och affärsregler
(som schablonvärden och kundkategorier) ska ställas in här.

*** MODIFIERAD FÖR ATT TA BORT UPPREPNINGAR (DRY) OCH DUBBLETTER ***
"""

import os
import urllib 

# --- 0. Global Tids- & Läges-styrning (NYTT) ---

# Sätt denna till 'PRODUCTION' för vanlig drift.
# Sätt denna till 'VALIDATION' för att köra din "hold-out"-test (oktober)
RUN_MODE = 'PRODUCTION' # <-- ÄNDRA DENNA ENDA RAD

# Alla 'datetime.now()'-anrop och tidsomvandlingar ska använda detta.
PROJECT_TIMEZONE = 'Europe/Stockholm'

# --- 1. Databasanslutningar (KÄLLOR) ---

# --- A. MariaDB / MySQL (KÄLLOR) ---
# Central konfig för alla MariaDB-anslutningar (per användarens önskemål)
# Vi antar att alla MariaDB-källor har samma inloggningsuppgifter.
MARIADB_BASE_CONFIG = {
    "host": "172.21.2.49", 
    "port": "3306",
    "user": "root",
    "pass": "X40t6jRq7msOABEG"
}

# Definiera endast de unika databasnamnen
QUEUE_DB_NAME = "queues"
BILLING_DB_NAME = "billing"
CASE_DB_NAME = "casemanagement"

# Tabellnamn för varje databas
QUEUE_TABLES = {
    "cdr": "queue_cdr",
    "channels": "queuechannels",
    "membership": "queuechannelmembership",
    "groups": "queuegroups",
    "current": "current_queue",
    "estimates": "estimated_wait_times"
}

BILLING_TABLES = {
    "customers": "customers"
}

# Döpte om denna från CASEMANAGE_TABLES för tydlighet
CASE_TABLES = {
    "users": "users",
    "cases": "cases" 
}

# --- B. MSSQL (MÅL) ---
# Här sparas all färdiga data.
MSSQL_DB = {
    "server": "172.21.2.49",
    "database": "IpPbxCDR",     
    "user": "sa",
    "pass": "X40t6jRq7msOABEG",
    "driver": "ODBC Driver 18 for SQL Server" 
}

# --- 3. SQL Alchemy Connection Strings ---
# Skapar anslutningssträngar automatiskt

def get_mariadb_conn_string(base_config, db_name):
    """ 
    MODIFIERAD: Skapar en anslutningssträng för MariaDB/MySQL.
    Tar nu emot en bas-konfig och ett databasnamn.
    """
    return (
        f"mysql+pymysql://{base_config['user']}:{base_config['pass']}"
        f"@{base_config['host']}:{base_config['port']}/{db_name}"
    )

def get_mssql_conn_string(db_config):
    """ Skapar en anslutningssträng för MSSQL. (Oförändrad) """
    # Bygger din exakta sträng
    params = urllib.parse.quote_plus(
        f"DRIVER={{{db_config['driver']}}};"
        f"SERVER={db_config['server']};"
        f"DATABASE={db_config['database']};"
        f"UID={db_config['user']};"
        f"PWD={db_config['pass']};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
    return f"mssql+pyodbc:///?odbc_connect={params}"

# Käll-anslutningar (Nu mycket renare!)
QUEUE_DB_CONN_STR = get_mariadb_conn_string(MARIADB_BASE_CONFIG, QUEUE_DB_NAME)
BILLING_DB_CONN_STR = get_mariadb_conn_string(MARIADB_BASE_CONFIG, BILLING_DB_NAME)
CASE_DB_CONN_STR = get_mariadb_conn_string(MARIADB_BASE_CONFIG, CASE_DB_NAME)
# CASEMANAGE_DB_CONN_STR är borttagen, använd CASE_DB_CONN_STR istället.

# Mål-anslutning
MSSQL_CONN_STR = get_mssql_conn_string(MSSQL_DB)


# --- 4. Affärsregler & Filtrering ---
OPERATIONAL_MONTHS_AGO = 16
STRATEGIC_MONTHS_AGO = 16
BUSINESS_HOURS_START = "06:30:00"
BUSINESS_HOURS_END = "18:00:00"
CALL_CHANNEL_NAME = 'call'

SICK_LEAVE_NUMBER = '+46607890220'
# axeon, wecall, test etc
EXCLUDE_QUEUE_IDS = [ 148, 150, 151, 156, 157, 168, 166, 171, 172, 173, 174, 175, 176]

# wecal, axeon, interna linjer, lrf, eltel, upplevelse
EXCLUDE_CUSTOMER_IDS = [
    74727,
    123113,
    125303,
    170378,
    132810, 
    20613,
    119050,
    121345,
    119278,
    154358,
    154787,
    154832,
    20724,
    20984,
    20813,
    20681,
    20509,
    187111,
    20520,
    20524,
    20411 # telecenter i Umeå
]

CUSTOMER_CATEGORIES = {
    'Fastighet': ['fastighet', 'bygg', 'boende', 'förvaltning', 'Fastighetsbyrå'],
    'Advokat': ['advokat', 'juridik', 'Advåkatbyrå'],
    'Vård': ['vård', 'hälsa', 'tandläkare', 'klinik']
}

QUEUE_TO_SERVICETYPE_MAP = {
    # Generella Tjänster
    141: "Telefonpassning",
    142: "TP Komplicerad/Växel Enkel",
    143: "Växel Komplicerad/Externa",
    144: "Kundtjänst Samtal",
    146: "Cherry Picking",
    147: "Kundtjänst Omni", # Samtal/Mail/Chatt
    154: "Kö 11",
    163: "Ingen Väntetid",
    # Specifika Kunder
    145: "Kund: Advokat",
    152: "Kund: Prezero IT",
    161: "Kund: Polygon",
    177: "Kund: Flexmassage",
    178: "Kund: Kävlinge", 
    # Lägg till fler specifika kund-IDn här
}

# --- 5. Ekonomiska Schablonvärden ---

COST_AGENT_PER_HOUR = 0.1
REVENUE_PER_MINUTE = 10.0
REVENUE_PER_CALL = 10.0

# Hur många procent av en timme en agent kan vara samtals-belagd
AGENT_OCCUPANCY_TARGET = 0.80

# --- 6. Tabellnamn i MSSQL db ---

BRONZE_TABLES = {
    "cdr": "Bronze_Queue_CDR",
    "groups": "Bronze_Queue_Groups",
    "customers": "Bronze_Billing_Customers",
    "cases": "Bronze_Cases",
    "users": "Bronze_Case_Users"
}
# Definierar vad tabellerna ska heta i din MSSQL-databas
TABLE_NAMES = {
    # --- Dimensioner (VEM, VAD, VAR, NÄR) ---
    "Date_Dimension": "Dim_Date",
    "Queue_Dimension": "Dim_Queue",
    "Customer_Dimension": "Dim_Customer",               
    "Customer_Behavior_Dimension": "Dim_Customer_Behavior", 
    "Phone_Lookup_Dimension": "Dim_Phone_Lookup",
    
    # --- Fakta-tabeller (HÄNDELSER, MÄTVÄRDEN) ---
    "Raw_Cases": "Fact_Cases",
    "Agent_Performance": "Fact_Agent_Performance",
    "Hourly_Aggregated_History": "Fact_Hourly_Aggregated_History",
    
    # Detta är den stora käll-tabellen, en fakta-tabell med transaktioner
    "Operative_Training_Data": "Fact_Operative_Training_Calls",
    
    # --- Prognoser (Framtid) ---
    "Operative_Forecast": "Frcast_Operative_Calls_By_Service",
    
    # --- Färdiga Rapporter (Specifika vyer) ---
    "Abandoned_Calls_Report": "Rpt_Abandoned_Calls",     
    "Strategic_KPI_Report": "Rpt_Strategic_KPI_Hourly_By_Service",
    
    # --- NYA TABELLER FÖR FEEDBACK-LOOP ---
    "Forecast_Archive": "Fcast_Archive_Log",
    "Forecast_Performance": "Fcast_Performance_Log",
    
    "Monthly_Peak_Analysis": "Dim_Customer_Monthly_Peaks",
    
    "Bronze_Call_Data": "Bronze_Queue_CDR",
}

# --- 7. Filvägar ---
# Fil med nummer som ska exkluderas (läses från fil)
EXCLUDE_NUMBERS_FILE = 'incomingnumber.csv' 

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MODEL_DIR = os.path.join(BASE_DIR, 'models')
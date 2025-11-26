@ECHO OFF
TITLE (DAGLIG KORNING - Uppdatera & Prognostisera)

ECHO ==========================================================
ECHO KOR DAGLIG DATALADDNING OCH PROGNOS
ECHO ==========================================================
ECHO.

REM Aktiverar miljön
CALL C:\Users\Lily.ibrahimi\AppData\Local\miniconda3\Scripts\activate.bat DataDrivetSysV2
IF %ERRORLEVEL% NEQ 0 (
    ECHO FEL: Kunde inte aktivera Conda-miljon.
    PAUSE
    GOTO :EOF
)

ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 1: Hamtar NY data (Bronze -> Silver)...
ECHO ----------------------------------------------------------
REM  gårdagens samtal för att prognosen ska veta nuläget
python "0_Load_Bronze_Data.py"
python "1_Extract_Operative_Data.py"

ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 2: Uppdaterar Segment & Historik...
ECHO ----------------------------------------------------------
REM Detta uppdaterar 'Hourly_Aggregated_History' som prognosen behover
python "1.5_Run_Customer_Segmentation.py"
python "2_Train_Operative_Model.py"


ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 3: rensar ...
ECHO ----------------------------------------------------------
python "nuke_all_archive.py"


ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 4: Skapar Prognos (14 dagar)...
ECHO ----------------------------------------------------------
python "3_Run_Operative_Forecast.py"


ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 4: Synkar Agent-data (Raw Cases)...
ECHO ----------------------------------------------------------
python "C_Sync_Raw_Cases.py"

ECHO.
ECHO ==========================================================
ECHO DAGLIG KORNING KLAR!
ECHO ==========================================================
PAUSE
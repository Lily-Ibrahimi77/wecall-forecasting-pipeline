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
REM Vi måste hämta gårdagens samtal för att prognosen ska veta nuläget
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
ECHO Steg 3: Skapar Prognos (14 dagar)...
ECHO ----------------------------------------------------------
python "3_Run_Operative_Forecast.py"

REM Vi hoppar över 4_Evaluate just nu då den kräver 'VALIDATION' mode
REM python "4_evaluate_forcast.py"

ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 4: Synkar Agent-data (Raw Cases)...
ECHO ----------------------------------------------------------
REM Bra att ha färskt i Power BI för analys av gårdagen
python "C_Sync_Raw_Cases.py"

ECHO.
ECHO ==========================================================
ECHO DAGLIG KORNING KLAR!
ECHO Data uppdaterad och ny prognos publicerad till Power BI.
ECHO ==========================================================
PAUSE
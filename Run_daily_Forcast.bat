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
ECHO Steg 1: Hamtar & Tvattar Data (Bronze -> Silver)
ECHO ----------------------------------------------------------
%PYTHON_EXE% "0_Load_Bronze_Data.py"
%PYTHON_EXE% "1_Extract_Operative_Data.py"

ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 2: Analys & Traning (Gold Layer)
ECHO ----------------------------------------------------------
%PYTHON_EXE% "1.5_Run_Customer_Segmentation.py"
%PYTHON_EXE% "2_Train_Operative_Model.py"

ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 3: Skapar Prognos (14 dagar)
ECHO ----------------------------------------------------------
REM Detta steg rensar automatiskt gamla dubbletter (Self-Healing)
%PYTHON_EXE% "3_Run_Operative_Forecast.py"

ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 4: Synkar Agent-data till BI
ECHO ----------------------------------------------------------
%PYTHON_EXE% "C_Sync_Raw_Cases.py"

ECHO.
ECHO ==========================================================
ECHO DAGLIG KORNING KLAR!
ECHO ==========================================================
PAUSE



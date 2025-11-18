@ECHO OFF
TITLE Operativ Prognos-pipeline (ROBUST VERSION)

ECHO ==========================================================
ECHO KORNING AV ROBUST OPERATIV PROGNOS-PIPELINE
ECHO (INKL. KUNDSEGMENTERING, TRANING, PROGNOS OCH UTVARDERING)
ECHO ==========================================================
ECHO.
ECHO Hittar ratt Python-miljo...

REM ** ANPASSA DENNA SOKVAG **
CALL C:\Users\Lily.ibrahimi\AppData\Local\miniconda3\Scripts\activate.bat DataDrivetSysV2
IF %ERRORLEVEL% NEQ 0 (
    ECHO FEL: Kunde inte aktivera Conda-miljon 'DataDrivetSysV2'.
    GOTO :ERROR
)
ECHO.
ECHO Python-miljo 'DataDrivetSysV2' aktiverad.
ECHO.

ECHO ----------------------------------------------------------
ECHO Steg 0: Kor 0_Load_Bronze_Data.py...
ECHO (Kopierar radata fran MariaDB till MSSQL Bronze-lager)
ECHO ----------------------------------------------------------
python "0_Load_Bronze_Data.py"
IF %ERRORLEVEL% NEQ 0 (
    ECHO FEL: 0_Load_Bronze_Data.py misslyckades!
    GOTO :ERROR
)

ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 1: Kor 1_Extract_Operative_Data.py...
ECHO (Hamtar 16 manader historik fran Bronze till Silver)
ECHO ----------------------------------------------------------
python "1_Extract_Operative_Data.py"
IF %ERRORLEVEL% NEQ 0 (
    ECHO FEL: 1_Extract_Operative_Data.py misslyckades!
    GOTO :ERROR
)

ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 1.5: Kor 1.5_Run_Customer_Segmentation.py (K-Means)... 
ECHO (Bygger dynamiska beteendesegment och 'Peak Patterns')
ECHO ----------------------------------------------------------
python "1.5_Run_Customer_Segmentation.py"
IF %ERRORLEVEL% NEQ 0 (
    ECHO FEL: 1.5_Run_Customer_Segmentation.py misslyckades!
    GOTO :ERROR
)

ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 2: Kor 2_Train_Operative_Model.py...
ECHO (Tranar hierarkisk modell pa (Ko, Kund, Segment))
ECHO ----------------------------------------------------------
python "2_Train_Operative_Model.py"
IF %ERRORLEVEL% NEQ 0 (
    ECHO FEL: 2_Train_Operative_Model.py misslyckades!
    GOTO :ERROR
)

ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 3: Kor 3_Run_Operative_Forecast.py (med arkivering)...
ECHO (Skapar 14-dagars prognos per Kund/Segment)
ECHO ----------------------------------------------------------
python "3_Run_Operative_Forecast.py"
IF %ERRORLEVEL% NEQ 0 (
    ECHO FEL: 3_Run_Operative_Forecast.py misslyckades!
    GOTO :ERROR
)

ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 4: Kor C_Sync_Raw_Cases.py...
ECHO (Synkroniserar agent- och arendedata fran Bronze)
ECHO ----------------------------------------------------------
python "C_Sync_Raw_Cases.py"
IF %ERRORLEVEL% NEQ 0 (
    ECHO FEL: C_Sync_Raw_Cases.py misslyckades!
    GOTO :ERROR
)

ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 5: Kor 4_Evaluate_Forecast.py (Feedback-loop)...
ECHO (Utvarderar foregaende prognos jamfort med verklighet)
ECHO ----------------------------------------------------------
python "4_evaluate_forcast.py"
IF %ERRORLEVEL% NEQ 0 (
    ECHO FEL: 4_Evaluate_Forecast.py misslyckades!
    GOTO :ERROR
)

ECHO.
ECHO ==========================================================
ECHO KLART! Hela den robusta, operativa pipelinen lyckades.
ECHO ==========================================================
ECHO.
PAUSE
GOTO :EOF

:ERROR
ECHO.
ECHO ==========================================================
ECHO ETT FEL UPPSTOD. Avbryter korningen.
ECHO ==========================================================
ECHO.
PAUSE
:EOF
@ECHO OFF
TITLE (TEST-KORNING) Operativ Prognos-pipeline (MED FEEDBACK)

ECHO ==========================================================
ECHO KOR EN SNABB TEST-PIPELINE (ANVANDER _TEST_config.py)
ECHO ==========================================================
ECHO.
ECHO --- STEG A: SAKRAR PRODUKTIONS-CONFIG ---
REM Byter namn pa den riktiga config-filen sa den ar saker
REN config.py config_PROD.py
IF %ERRORLEVEL% NEQ 0 (
    ECHO VARNING: Kunde inte byta namn pa config.py. 
    ECHO Forsoker aterstalla om _TEST_config.py ar aktiv...
    REN config.py _TEST_config.py
    REN config_PROD.py config.py
)

ECHO.
ECHO --- STEG B: AKTIVERAR TEST-CONFIG ---
REM Aktiverar var test-config
REN _TEST_config.py config.py
IF %ERRORLEVEL% NEQ 0 (
    ECHO FATALT FEL: Kunde inte hitta din _TEST_config.py!
    GOTO :CLEANUP_AND_ERROR
)

ECHO.
ECHO Test-config ar nu aktiv.
ECHO Startar Python-miljo...
ECHO.
REM Aktiverar din Conda-miljo
CALL C:\Users\Lily.ibrahimi\AppData\Local\miniconda3\Scripts\activate.bat DataDrivetSysV2
IF %ERRORLEVEL% NEQ 0 (
    ECHO FEL: Kunde inte aktivera Conda-miljon 'DataDrivetSysV2'.
    GOTO :CLEANUP_AND_ERROR
)
ECHO.
ECHO Python-miljo 'DataDrivetSysV2' aktiverad.
ECHO.
ECHO ==========================================================
ECHO KOR PIPELINE...
ECHO ==========================================================



ECHO.
ECHO ----------------------------------------------------------
ECHO ...
ECHO ----------------------------------------------------------

python "Debug-steg-for-steg.py"

IF %ERRORLEVEL% NEQ 0 (

    ECHO  misslyckades!

    GOTO :CLEANUP_AND_ERROR

)


ECHO.
ECHO ----------------------------------------------------------
ECHO Steg 3: Kor 3_Run_Operative_Forecast.py (med arkivering)...
ECHO ----------------------------------------------------------

python "3_Run_Operative_Forecast.py"

IF %ERRORLEVEL% NEQ 0 (

    ECHO FEL: 3_Run_Operative_Forecast.py misslyckades!

    GOTO :CLEANUP_AND_ERROR

)



ECHO ----------------------------------------------------------
ECHO Steg 5: Kor 4_evaluate_forcast.py (Feedback-loop)...
ECHO ----------------------------------------------------------
python "4_evaluate_forcast.py"
IF %ERRORLEVEL% NEQ 0 (
    ECHO FEL: 4_evaluate_forcast.py misslyckades!
    GOTO :CLEANUP_AND_ERROR
)

ECHO.
ECHO ==========================================================
ECHO TEST-KORNING LYCKADES!
ECHO ==========================================================
GOTO :CLEANUP_AND_EXIT


:CLEANUP_AND_ERROR
ECHO.
ECHO ==========================================================
ECHO ETT FEL UPPSTOD UNDER TEST-KORNINGEN.
ECHO ==========================================================
ECHO.

:CLEANUP_AND_EXIT
ECHO.
ECHO --- STEG Z: ATERSTALLER CONFIG-FILER... ---
REM Byter tillbaka sa att den riktiga config-filen ar aktiv
REN config.py _TEST_config.py
REN config_PROD.py config.py
ECHO.
ECHO Produktions-config (16 manader) ar aterstalld.
ECHO Klar.
ECHO.
PAUSE
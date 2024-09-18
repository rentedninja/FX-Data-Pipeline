@echo off
REM Change the working directory
cd /d C:\Users\allan\Videos\360T

REM Set the path to the Python interpreter and script
set PYTHON_PATH=C:\Users\allan\AppData\Local\Microsoft\WindowsApps\python.exe
set SCRIPT_PATH=C:\Users\allan\Videos\360T\Fx_1min.py

REM Set the path to the log file
set LOG_FILE=C:\Users\allan\Videos\360T\update_exchange_rates_results.log

REM Run the Python script and redirect output and errors to the log file
"%PYTHON_PATH%" "%SCRIPT_PATH%" > "%LOG_FILE%" 2>&1

REM Display the contents of the log file in the Command Prompt window
type "%LOG_FILE%"

REM Pause to keep the Command Prompt window open for review
pause

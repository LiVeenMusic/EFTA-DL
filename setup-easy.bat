@echo off
setlocal

echo.
echo =====================================
echo EFTA-DL setup: creating virtual environment and installing dependencies
echo =====================================
echo.

python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not available. Install Python and make sure it is on your PATH.
    goto :error
)

if exist venv (
    echo Using existing virtual environment in venv\
) else (
    echo Creating virtual environment...
    python -m venv venv
    if errorlevel 1 goto :error
)

echo Activating virtual environment...
call venv\Scripts\activate.bat
if errorlevel 1 goto :error

echo Upgrading pip...
python -m pip install --upgrade pip
if errorlevel 1 goto :error

echo Installing dependencies from requirements.txt...
python -m pip install -r requirements.txt
if errorlevel 1 goto :error

echo Installing Playwright browsers...
python -m playwright install
if errorlevel 1 goto :error

echo Installing Playwright system dependencies...
python -m playwright install-deps
if errorlevel 1 goto :error

echo.
echo Setup complete. You can now run the project using the activated virtual environment.

goto :end

:error
echo.
echo Setup failed. Please review the error output above and rerun this script after fixing the issue.

:end
pause
endlocal
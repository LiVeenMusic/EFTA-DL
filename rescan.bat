@echo off
setlocal

echo.
echo =====================================
echo EFTA-DL rescan: activating virtual environment and rescanning
echo =====================================
echo.

if not exist venv\Scripts\activate.bat (
    echo ERROR: Virtual environment not found. Run setup-easy.bat first.
    goto :error
)

call venv\Scripts\activate.bat
if errorlevel 1 goto :error

echo Running rescan...
python download.py --rescan %*
if errorlevel 1 goto :error

echo.
echo Rescan completed successfully.

goto :end

:error
echo.
echo Rescan failed. See the output above for details.

:end
pause
endlocal
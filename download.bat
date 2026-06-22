@echo off
setlocal

echo.
echo =====================================
echo EFTA-DL download: activating virtual environment and starting download
echo =====================================
echo.

if not exist venv\Scripts\activate.bat (
    echo ERROR: Virtual environment not found. Run setup-easy.bat first.
    goto :error
)

call venv\Scripts\activate.bat
if errorlevel 1 goto :error

echo Running data download...
python download.py --max-workers 10 --request-delay 1 %*
if errorlevel 1 goto :error

echo.
echo Download completed successfully.

goto :end

:error
echo.
echo Download failed. See the output above for details.

:end
pause
endlocal
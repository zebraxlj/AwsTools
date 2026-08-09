@echo off
setlocal

cd /d "%~dp0.."

:: Setup build environment
echo INFO - Checking uv...
uv --version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ERROR - uv is not installed or not on PATH.
    goto :fail
)

echo INFO - Ensuring managed Python is available...
uv python install
if %ERRORLEVEL% neq 0 (
    echo ERROR - Failed to install managed Python.
    goto :fail
)

echo INFO - Syncing project dependencies...
uv sync --python-preference only-managed
if %ERRORLEVEL% neq 0 (
    echo ERROR - Failed to sync dependencies.
    goto :fail
)
echo INFO - Build environment ready.

:: Clean previous build artifacts
echo INFO - Cleaning previous build...
if exist build rmdir /s /q build
if exist AwsTools.spec del /q AwsTools.spec
if exist dist\AwsTools.exe del /q dist\AwsTools.exe

:: Build
echo INFO - Building AwsTools.exe...
uv run --python-preference only-managed --with pyinstaller pyinstaller ^
    --onefile ^
    --noconsole ^
    --name "AwsTools" ^
    --distpath dist ^
    --workpath build ^
    --specpath . ^
    --collect-data botocore ^
    --collect-data boto3 ^
    --exclude-module pytest ^
    --exclude-module tkinter ^
    --icon "UI\assets\aws_tools_icon.ico" ^
    --add-data "UI\theme.qss;UI" ^
    --add-data "UI\assets\aws_tools_icon_310x310.png;UI\assets" ^
    run_app.py

if %ERRORLEVEL% neq 0 goto :fail

echo.
echo INFO - Build succeeded: dist\AwsTools.exe
powershell -NoProfile -Command ^
    "$f=(Resolve-Path dist).Path; $n='AwsTools.exe';" ^
    "$s=New-Object -Com Shell.Application;" ^
    "$w=$s.Windows()|Where-Object{$_.Document.Folder.Self.Path -eq $f}|Select-Object -First 1;" ^
    "if($w){" ^
    "  Add-Type -Name U -Namespace Win32 -MemberDefinition '[DllImport(\"user32.dll\")]public static extern bool SetForegroundWindow(IntPtr h);';" ^
    "  [Win32.U]::SetForegroundWindow([IntPtr]$w.HWND)|Out-Null;" ^
    "  $i=$w.Document.Folder.ParseName($n);" ^
    "  $w.Document.SelectItem($i,29)" ^
    "}else{" ^
    "  explorer /select,(Join-Path $f $n)" ^
    "}"
goto :end

:fail
echo.
echo ERROR - Build FAILED.

:end
endlocal
pause

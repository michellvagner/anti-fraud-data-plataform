@echo off
cd /d "%~dp0"

uv sync
uv run src\main.py

echo.
echo.

set /p DELETE_INFRA="Deseja deletar a infraestrutura? (yes/no): "

if /I "%DELETE_INFRA%"=="yes" (
    uv run src\destroy_snowflake.py
) else (
    echo.
    echo infraestrutura mantida
)
pause
@echo off
cd /d "%~dp0"
call venv\Scripts\activate

REM Upgrade pip and install dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt

REM Start the FastAPI application
uvicorn main:app --host 0.0.0.0 --port 8001

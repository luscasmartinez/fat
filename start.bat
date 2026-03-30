@echo off
echo ============================================
echo   Faturamento Platform - Iniciando servidor
echo   http://localhost:8001
echo ============================================
cd backend
C:\Users\Lusca\AppData\Local\Programs\Python\Python314\python.exe -m uvicorn main:app --host 0.0.0.0 --port 8001 --reload

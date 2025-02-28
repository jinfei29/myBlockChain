@echo off
setlocal enabledelayedexpansion

REM 循环发送100条消息
for /L %%i in (1,1,100) do (
    curl "http://localhost:8080/req?message=%%i"
)

endlocal
@echo off
echo Build AISStore-Go

cd %~dp0

go build -ldflags "-s" -o aisstore-lw.exe .
:: go build -ldflags "-s -H windowsgui" .
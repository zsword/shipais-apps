@echo off
echo Build AISMerge-Go

cd %~dp0

go build -ldflags "-s" -o aismerge-lw.exe .
:: go build -ldflags "-s -H windowsgui" .
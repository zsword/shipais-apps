@echo off
echo Build AISMerge-Go

cd %~dp0

go build -ldflags "-s -w -H=windowsgui" -o aismerge-lw.exe .
:: go build -ldflags "-s -H windowsgui" .
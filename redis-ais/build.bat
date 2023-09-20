@echo off
echo Build Redis-AIS

cd %~dp0

go build -ldflags "-s" .
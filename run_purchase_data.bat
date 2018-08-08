@echo off

if [%1]==[] goto :blank
if [%2]==[] goto :blank

:start_execute
set newest_date=%2
set start_of_newest_date=%newest_date:~0,6%01

::echo %newest_date%
::echo %start_of_newest_date%
::goto :end

:kudil
@echo on
ekko.exe -s %1 -e %2
ekpo.exe -s %1 -e %2
mara -s %1 -e %2 -t %start_of_newest_date% -d %2
t024e.exe
lfa1.exe -s %1 -e %2
tcurr.exe -s %start_of_newest_date% -e %2
t024.exe
zstxl.exe -s %1 -e %2
@echo off

goto :end

:blank
echo You need to specify 2 arguments!
echo Sample: %~nx0% 20180101 20180331

:end
exit /b
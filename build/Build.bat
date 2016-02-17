@echo off

set params="%1"
if %params% == "" set params=/t:package /p:BUILD_NUMBER=0 /p:PackageVersion=3.0.2.2
%WINDIR%\Microsoft.NET\Framework\v4.0.30319\MSBuild.exe build.proj /v:m %params%

REM package
REM %WINDIR%\Microsoft.NET\Framework\v4.0.30319\MSBuild.exe build.proj /v:m /t:package /p:BUILD_NUMBER=YYY
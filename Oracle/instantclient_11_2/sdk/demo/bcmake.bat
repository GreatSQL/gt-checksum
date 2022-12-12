@echo off
REM
REM Copyright (c) 1999, 2004, Oracle. All rights reserved.  
REM $
REM
if (%BORLAND_HOME%) == () goto nobchome

set BINC=%BORLAND_HOME%\include 
set BLIB=%BORLAND_HOME%\lib
set ICINCHOME=..\include
set ICLIBHOME=..\lib\bc
if (%1) == () goto usage

if (%1) == (cdemo81) goto ocimake
if (%1) == (CDEMO81) goto ocimake
if (%1) == (occidml) goto occimake
if (%1) == (OCCIDML) goto occimake

:ocimake
%BORLAND_HOME%\bin\bcc32 -w-pro -c -a4 -DOCI_BORLAND -I. -I%BINC% -I%ICINCHOME% %1.c
%BORLAND_HOME%\bin\bcc32 -L%BLIB% -L%ICLIBHOME% %1.obj oci.lib bidsfi.lib 
goto end
:occimake
%BORLAND_HOME%\bin\bcc32 -w-pro -c -a4 -DOCI_BORLAND -I. -I%BINC% -I.\ -I%ICINCHOME% %1.cpp
%BORLAND_HOME%\bin\bcc32 -L%BLIB% -L%ICLIBHOME% %1.obj oci.lib bidsfi.lib 
goto end

:nobchome
echo .
echo Please set environment variable BORLAND_HOME
echo .
goto end

:usage
echo.
echo Usage: make filename [i.e. bcmake oci01]
echo.
:end
set BINC=
set BLIB=
set ICINCHOME=
set ICLIBHOME=

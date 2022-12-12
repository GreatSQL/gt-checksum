REM
REM Copyright (c) 1999, 2009, Oracle and/or its affiliates. 
REM All rights reserved. 
REM
@echo off
set ICINCHOME=..\include
set ICLIBHOME=..\lib\msvc
set OTT=..\ott

if (%1) == () goto usage
if (%1) == (cdemo81) goto ocimake
if (%1) == ("cdemo81") goto ocimake
if (%1) == (CDEMO81) goto ocimake
if (%1) == ("CDEMO81") goto ocimake

if (%1) == (occidml) goto occidml
if (%1) == ("occidml") goto occidml
if (%1) == (OCCIDML) goto occidml
if (%1) == ("OCCIDML") goto occidml

if (%1) == (occiobj) goto occiobj
if (%1) == ("occiobj") goto occiobj
if (%1) == (OCCIOBJ) goto occiobj
if (%1) == ("OCCIOBJ") goto occiobj

if defined AMD64BLD set BUFFEROVERFLOWLIB=bufferoverflowu.lib

cl -I%ICINCHOME% -I. -D_CRT_SECURE_NO_DEPRECATE -D_DLL -D_MT %1.c /link /LIBPATH:%ICLIBHOME% oci.lib kernel32.lib msvcrt.lib /nod:libc %BUFFEROVERFLOWLIB%
if exist %1.exe.manifest call %MTBIN% -manifest %1.exe.manifest -outputresource:%1.exe;#1 & call %SRCHOME%/buildtools/bin/ntrmln.bat rmf %1.exe.manifest
goto end

:ocimake
set filename=%1
cl -I%ICINCHOME% -I. -D_CRT_SECURE_NO_DEPRECATE -D_DLL -D_MT %1.c /link /LIBPATH:%ICLIBHOME% oci.lib msvcrt.lib /nod:libc %BUFFEROVERFLOWLIB%
if exist %filename%.exe.manifest call %MTBIN% -manifest %filename%.exe.manifest -outputresource:%filename%.exe;#1 & %SRCHOME%/buildtools/bin/ntrmln.bat rmf %filename%.exe.manifest
goto end

:occidml
set filename=%1
cl -GX -DWIN32COMMON -I. -I%ICINCHOME% -I. -D_CRT_SECURE_NO_DEPRECATE -D_DLL -D_MT %1.cpp /link /LIBPATH:%ICLIBHOME% oci.lib msvcrt.lib msvcprt.lib oraocci11.lib /nod:libc %BUFFEROVERFLOWLIB%
if exist %filename%.exe.manifest call %MTBIN% -manifest %filename%.exe.manifest -outputresource:%filename%.exe;#1 & %SRCHOME%/buildtools/bin/ntrmln.bat rmf %filename%.exe.manifest
goto end

:occiobj
set filename=%1
CALL %OTT% userid=hr/hr intype=%1.typ outtype=%1out.typ code=cpp hfile=%1.h cppfile=%1o.cpp mapfile=%1m.cpp attraccess=private
cl -GX -DWIN32COMMON -I. -I%ICINCHOME% -I. -D_CRT_SECURE_NO_DEPRECATE -D_DLL -D_MT %1.cpp %1o.cpp %1m.cpp /link /LIBPATH:%ICLIBHOME% oci.lib msvcrt.lib msvcprt.lib oraocci11.lib /nod:libc %BUFFEROVERFLOWLIB%
if exist %filename%.exe.manifest call %MTBIN% -manifest %filename%.exe.manifest -outputresource:%filename%.exe;#1 & %SRCHOME%/buildtools/bin/ntrmln.bat rmf %filename%.exe.manifest
goto end

:usage
echo.
echo Usage: make filename [i.e. make cdemo81]
echo.
:end
set ICINCHOME=
set ICLIBHOME=
set BUFFEROVERFLOWLIB=

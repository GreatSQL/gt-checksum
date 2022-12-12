@echo off
Rem OTT script for instant client.
Rem Before running this script
Rem   set jdk1.5/bin to PATH
Rem   set PATH to point to the instant client dlls
Rem   Include ottclasses.zip, ojdbc5.jar, orai18n.jar in the CLASSPATH
Rem
set NLSLANG=
if defined NLS_LANG set NLSLANG=NLS_LANG
java oracle.ott.c.CMain nlslang=%NLSLANG% %*

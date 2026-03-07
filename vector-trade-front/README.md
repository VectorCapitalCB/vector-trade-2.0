# App


https://vectortrade.cl/updatevt/VectorTrade2.0.9.exe
https://www.vectortrade.cl/updatevt-qa/VectorTradeQA3.1.2.exe


/usr/share/nginx/html/updatevt/
/usr/share/nginx/html/updatevt-qa/


##### NOTE VICTOR

"C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" x64
set LIB=C:\Program Files (x86)\Windows Kits\10\Lib\10.0.26100.0\ucrt\x64;C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Tools\MSVC\14.43.32007\lib\x64;%LIB%

mvn -f E:\VC-github\vector-trade-front clean gluonfx:build gluonfx:nativerun




mvn -f E:\VC-github\vector-trade-front gluonfx:runagent

##### PROFILE WINDOWS PRODUCCION

mvn -f E:\VC-github\vector-trade-front mvn -Pwindows clean gluonfx:build gluonfx:nativerun

##### PROFILE WINDOWS RICCI

mvn -Pricci clean gluonfx:build gluonfx:nativerun

#### PROFILE MAC

mvn -f /Users/user280230/Documents/vector-trade-front clean gluonfx:compile gluonfx:link gluonfx:package -Pmac gluonfx:nativerun

#### Graamvl-gluonfx-maven-plugin

<version>1.0.27</version>
<nativeImageArgs>
<arg>-Dsvm.platform=org.graalvm.nativeimage.Platform$MACOS_AMD64</arg>
<arg>-H:+UnlockExperimentalVMOptions</arg>
</nativeImageArgs>

Your Mac username is: user280230
Your Mac password is: phn65738uxv

# comprimir en mac
cd /Users/user280230/Documents/vector-trade-front/target/gluonfx/aarch64-darwin/
zip -r --symlinks -X VectorTrade3.1.4.macos.zip VectorTrade.app

scp /Users/user280230/Documents/vector-trade-front/target/gluonfx/aarch64-darwin/VectorTrade3.1.5.macos.zip azureadmin@4.203.105.71:/usr/share/nginx/html/updatevt/

Xevl4Vi5S37qJglY


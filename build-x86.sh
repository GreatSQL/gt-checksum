set -x

export PATH=$PATH:/usr/local/go/bin
export GO111MODULE=on
export GOPROXY=https://goproxy.cn
export CXXFLAGS="-stdlib=libstdc++" CC=/usr/bin/gcc CXX=/usr/bin/g++

vs=`cat ./inputArg/flagHelp.go| grep "app.Version"|awk -F "=" '{print $2}'|sed 's/\"//g'|sed 's/\/\/版本//g'|sed 's/ //g'`
OracleDrive="instantclient_11_2"
if [ ! -d "/usr/lcoal/${OracleDrive}" ];then
  cp -rpf Oracle/${OracleDrive} /usr/lcoal/
fi
export LD_LIBRARY_PATH=/usr/local/${OracleDrive}:$LD_LIBRARY_PATH
 
go build -o gt-checksum greatdbCheck.go
chmod +x gt-checksum
mkdir gt-checksum-${vs}-linux-x86-64
cp -rpf Oracle/${OracleDrive} gt-checksum gc.conf gc.conf-simple relnotes docs README.md gt-checksum-${vs}-linux-x86-64
tar zcf gt-checksum-${vs}-linux-x86-64.tar.gz gt-checksum-${vs}-linux-x86-64
mkdir binary
mv gt-checksum-${vs}-linux-x86-64.tar.gz binary
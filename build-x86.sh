set -x

export PATH=$PATH:/usr/local/go/bin
export GO111MODULE=on
export GOPROXY=https://goproxy.cn
export CXXFLAGS="-stdlib=libstdc++" CC=/usr/bin/gcc CXX=/usr/bin/g++

vs=`cat ./inputArg/flagHelp.go| grep "app.Version"|awk -F "=" '{print $2}'|sed 's/\"//g'|sed 's/\/\/版本//g'|sed 's/ //g'`
if [ ! -d "/usr/lcoal/instantclient_19_17" ];then
  cp -rpf Oracle/instantclient_19_17 /usr/lcoal/
fi
export LD_LIBRARY_PATH=/usr/local/instantclient_11_2:$LD_LIBRARY_PATH

go build -o gt-checkOut greatdbCheck.go
mkdir gt-checkOut-${vs}-linux-x86-64
cp -rpf gt-checkOut gc.conf gt-checkOut-${vs}-linux-x86-64
tar zcf gt-checkOut-${vs}-linux-x86-64.tar.gz gt-checkOut-${vs}-linux-x86-64
mkdir binary
mv gt-checkOut-${vs}-linux-x86-64.tar.gz binary
#
# build gt-checksum
# Requires go version 1.17 or higher
#
# run as:
# sh ./build.sh
#

export PATH=$PATH:/usr/local/go/bin
export GO111MODULE=on
export GOPROXY=https://goproxy.cn
export CXXFLAGS="-stdlib=libstdc++" CC=/usr/bin/gcc CXX=/usr/bin/g++

vs=`cat ./inputArg/flagHelp.go| grep "app.Version"|awk -F "=" '{print $2}'|sed 's/\"//g'|sed 's/\/\/版本//g'|sed 's/ //g'`
OracleDrive="instantclient_11_2"
HASH="a5bdc19"

# 自动适配CPU架构类型
if [ ! -z "`which uname > /dev/null 2>&1`" ] ; then
  arch=`uname -m`
elif [ ! -z "`echo $MACHTYPE`" ] ; then
  arch=`echo $MACHTYPE|awk -F '-' '{print $1}'`
else
  arch=x86_64
fi

rm -fr gt-checksum-${vs}-${HASH}-linux-${arch} release
mkdir -p gt-checksum-${vs}-${HASH}-linux-${arch} release

echo -n "1. "
go version

echo "2. Setting Oracle Library PATH"
if [ ! -d "/tmp/${OracleDrive}" ];then
  tar xf Oracle/${OracleDrive}.tar.xz -C /tmp/
fi
export LD_LIBRARY_PATH=/tmp/${OracleDrive}:$LD_LIBRARY_PATH

echo "3. Compiling gt-checksum"
go build -o gt-checksum gt-checksum.go && \
CGO_ENABLED=0 go build -o repairDB repairDB.go && \
go build -o oracle_random_data_load oracle_random_data_load.go && \
chmod +x gt-checksum repairDB oracle_random_data_load

if [ $? -ne 0 ] ; then
 echo "build gt-checksum failed, exit!"
 exit
fi

echo "4. Packaging gt-checksum"
cp -rpf CHANGELOG.md gc-sample.conf gt-checksum gt-checksum-manual.md Oracle/${OracleDrive}.tar.xz README.md repairDB oracle_random_data_load testcase gt-checksum-${vs}-${HASH}-linux-${arch} && \
tar cf gt-checksum-${vs}-${HASH}-linux-${arch}.tar gt-checksum-${vs}-${HASH}-linux-${arch} && \
tar cf gt-checksum-${vs}-${HASH}-linux-${arch}-minimal.tar --exclude=gt-checksum-${vs}-${HASH}-linux-${arch}/${OracleDrive}.tar.xz gt-checksum-${vs}-${HASH}-linux-${arch} && \
xz -9 -f gt-checksum-${vs}-${HASH}-linux-${arch}.tar && \
xz -9 -f gt-checksum-${vs}-${HASH}-linux-${arch}-minimal.tar && \
echo "5. The gt-checksum binary package is: gt-checksum-${vs}-${HASH}-linux-${arch}.tar.gz under directory release" && \
mv gt-checksum-${vs}-${HASH}-linux-${arch}.tar.xz release && \
mv gt-checksum-${vs}-${HASH}-linux-${arch}-minimal.tar.xz release && \
ls -la release && \
rm -fr gt-checksum-${vs}-${HASH}-linux-${arch}

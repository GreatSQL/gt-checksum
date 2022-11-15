vs=`cat ./inputArg/flagHelp.go| grep "app.Version"|awk -F "=" '{print $2}'|sed 's/\"//g'|sed 's/\/\/版本//g'|sed 's/ //g'`
go build -o gt-checkOut greatdbCheck.go
mkdir gt-checkOut-${vs}-linux-x86-64
cp -rpf gt-checkOut gc.conf gt-checkOut-${vs}-linux-x86-64
tar zcf gt-checkOut-${vs}-linux-x86-64.tar.gz gt-checkOut-${vs}-linux-x86-64
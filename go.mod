module gt-checksum

go 1.17

require (
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/fatih/color v1.13.0
	github.com/go-mysql-org/go-mysql v1.4.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/godror/godror v0.33.3
	github.com/gosuri/uitable v0.0.4
	github.com/lestrrat-go/file-rotatelogs v2.4.0+incompatible
	github.com/rifflock/lfshook v0.0.0-20180920164130-b9218ef580f5
	github.com/satori/go.uuid v1.2.0
	github.com/siddontang/go-log v0.0.0-20180807004314-8d05993dda07
	github.com/sirupsen/logrus v1.8.1
	github.com/urfave/cli v1.22.9
	gopkg.in/ini.v1 v1.66.6
)

require (
	github.com/cpuguy83/go-md2man/v2 v2.0.0-20190314233015-f79a8a8ca69d // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/godror/knownpb v0.1.0 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/lestrrat-go/strftime v1.0.5 // indirect
	github.com/mattn/go-colorable v0.1.9 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/russross/blackfriday/v2 v2.0.1 // indirect
	github.com/shopspring/decimal v0.0.0-20180709203117-cd690d0c9e24 // indirect
	github.com/shurcooL/sanitized_anchor_name v1.0.0 // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726 // indirect
	github.com/stretchr/testify v1.8.4 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	golang.org/x/sys v0.10.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)

replace github.com/go-mysql-org/go-mysql => github.com/siddontang/go-mysql v1.4.0 // indirect

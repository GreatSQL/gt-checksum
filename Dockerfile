## Build
#
# 本Dockerfile适用于Docker 17.05及以上版本，如果你的Docker版本较低，请先升级你的Docker
# 如果是podman则最后可能无法正常运行，因为podman不支持-o选项，可以试着改用buildah(4.x以上)实现，例如
# DOCKER_BUILDKIT=1 buildah build-using-dockerfile -t greatsql/gt-checksum:1.2.1 -f Dockerfile .
#

FROM golang:latest AS builder

LABEL gt-checksum="greatsql"
MAINTAINER  greatsql <greatsql@greatdb.com>

ENV GO111MODULE=on \
    GOOS=linux \
    GOPROXY="https://goproxy.io" \
    GOPRIVATE="github.com/marvinhosea/*"

WORKDIR /go/release

COPY  . .

ARG VERSION

RUN go mod tidy
RUN go build -o gt-checksum gt-checksum.go
RUN mkdir -p ./gt-checksum-${VERSION} && cp -rf README.md CHANGELOG.md gt-checksum-manual.md gc-sample.conf gt-checksum Oracle/instantclient_11_2.tar.xz gt-checksum-${VERSION}

FROM scratch AS exporter

ARG VERSION

COPY --from=builder /go/release/gt-checksum-${VERSION} ./gt-checksum-${VERSION}

# DOCKER_BUILDKIT=1 docker build --build-arg VERSION=v1.2.3 -f Dockerfile -o ./ .
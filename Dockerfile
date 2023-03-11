## Build
# This dockerfile uses the linux image
# VERSION 1.2.0
# Author: greatsql
# Command format: Instruction [arguments / command] …

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
RUN go build -o gt-checksum greatdbCheck.go
RUN mkdir -p ./gt-checksum-${VERSION} && cp -rf docs gc.conf gc.conf-simple gt-checksum Oracle/instantclient_11_2 README.md relnotes gt-checksum-${VERSION}

FROM scratch AS exporter

ARG VERSION

COPY --from=builder /go/release/gt-checksum-${VERSION} ./gt-checksum-${VERSION}

# DOCKER_BUILDKIT=1 docker build --build-arg VERSION=v1.2.0 -f Dockerfile -o ./ .

## Build
# This dockerfile uses the linux image
# VERSION 1
# Author: greatsql
# Command format: Instruction [arguments / command] …

FROM golang:latest
LABEL gt-checksum="greatsql"
MAINTAINER  greatsql <greatsql@greatdb.com>
ENV  GO111MODULE=on GOOS=linux GOPROXY=https://goproxy.io
RUN go env -w GOPRIVATE=github.com/marvinhosea/*
WORKDIR /go/release
COPY go.mod ./
COPY go.sum ./
COPY  . .
RUN go mod download
RUN go build -o gt-checksum greatdbCheck.go
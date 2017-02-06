#!/bin/sh
git submodule update --init
/usr/local/opt/protobuf@3.1/bin/protoc -Iprotobuf -Ikvproto/proto --java_out=src/main/java protobuf/gogoproto/gogo.proto kvproto/proto/*.proto


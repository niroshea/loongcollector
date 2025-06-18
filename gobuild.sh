#!/bin/bash

rm -rf "license_coverage.txt"
rm -rf "output" "dist"
rm -rf behavior-test
rm -rf performance-test
rm -rf core-test
rm -rf e2e-engine-coverage.txt
rm -rf find_licenses
rm -rf "generated_files"
rm -rf .testCoverage.txt
rm -rf .coretestCoverage.txt
rm -rf core/build
rm -rf plugin_main/*.dll
rm -rf plugin_main/*.so
rm -rf plugins/all/all.go
rm -rf plugins/all/all_debug.go
rm -rf plugins/all/all_windows.go
rm -rf plugins/all/all_linux.go

go mod tidy

make plugin_local

cp tihuan.dockerfile output/

cd output/ || exit

pwd

imageTag=hpc-sgp-prod-jcr-aliyun.hik-proconnect.com/docker-ipsc/usta/middleware/loongcollector:3.0.11_t$1

docker build -t $imageTag  -f tihuan.dockerfile .

docker push $imageTag

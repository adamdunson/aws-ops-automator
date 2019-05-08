#!/usr/bin/env bash

echo 'Staring to build distribution'
echo 'make build'
echo "$1"
cd ../source/code || { echo 'Path ../source/code does not exist!' && exit 1 ; }
make "bucket=$1"
cd ../../deployment || { echo 'Path ../../deployment does not exist!' && exit 1 ; }

echo 'mkdir -p dist'
mkdir -p dist
cp ops-automator-latest.template dist/ops-automator.template
cp "ops-automator-$(cat ../source/code/version.txt).zip" dist
cp "cloudwatch-handler-$(cat ../source/code/version.txt).zip" dist
rm "ops-automator-$(cat ../source/code/version.txt).template"
rm "ops-automator-$(cat ../source/code/version.txt).zip"
rm "cloudwatch-handler-$(cat ../source/code/version.txt).zip"

echo 'Completed building distribution'

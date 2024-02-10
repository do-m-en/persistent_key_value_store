#!/bin/bash

rm -rf pkvs_data

./pkvs -c1 --port 8080 -t 1 &
pid=$!
sleep 1 # TODO wait for certain output instead of sleep
trap "kill -9 $pid" EXIT

output=`curl -i -H "Accept: application/json" -H "Content-Type: application/json" -X POST localhost:8080/post -d "{\"key\":\"abcd\",\"value\":\"efg\"}"`

if ! [[ "$output" =~ "{\\\"result\\\":\\\"ok\\\"}" ]]
then
  exit 1
fi

output=`curl -i -H "Accept: application/json" -H "Content-Type: application/json" -X GET localhost:8080/get -d "{\"key\":\"abcd\"}"`

if ! [[ "$output" =~ "{\\\"value\\\":\\\"efg\\\"}" ]]
then
  exit 1
fi

sleep 2 # sleep so that the file gets persisted - TODO look for file on filesystem instead
kill -9 $pid

./pkvs -c8 --port 8080 -t 1 &
pid=$!
sleep 1 # TODO wait for certain output instead of sleep
trap "kill -9 $pid" EXIT

output=`curl -i -H "Accept: application/json" -H "Content-Type: application/json" -X GET localhost:8080/get -d "{\"key\":\"abcd\"}"`

if ! [[ "$output" =~ "{\\\"value\\\":\\\"efg\\\"}" ]]
then
  echo "error: "
  echo ${output}
  exit 1
fi

exit 0
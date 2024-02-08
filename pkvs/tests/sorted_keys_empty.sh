#!/bin/bash

rm -rf pkvs_data

./pkvs -c1 --port 8080 &
pid=$!
sleep 1 # TODO wait for certain output instead of sleep
trap "kill -9 $pid" EXIT

output=`curl -i -H "Accept: application/json" -H "Content-Type: application/json" -X GET localhost:8080/sorted_keys`

if ! [[ "$output" =~ "{\\\"keys\\\":[]}" ]]
then
  exit 1
fi

exit 0
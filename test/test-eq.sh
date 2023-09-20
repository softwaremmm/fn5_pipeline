#!/bin/bash
set -e

# This is a first pass. Ideally we'd test every permutation rather than just the end product

get_dist(){
    local species=$1
    local run_id=$2
    curl -SsL --fail --show-error --header "Content-Type: application/json"  --request GET  http://0.0.0.0:8000/api/v1/relatedness/$species/neighbours\?run_id\=$run_id -H "Authorization: Basic test-api-key"
}

json_eq(){
    local expected=$(cat $1 | jq ".")
    local actual=$(cat $2 | jq ".")
    if [[ "$expected" == "$actual" ]]; then
        #Correct
        echo "PASS: $2"
    else
        #Mismatch so complain
        echo "FAIL: $2"
        echo
        echo "Expected:"
        echo $expected
        echo
        echo
        echo "Actual:"
        echo $actual
        exit 1
    fi
}

#Check that our synchronous runs have expected values first
mkdir -p test/actual_distances/synchronous

for i in {1..6}; do
    get_dist test $i > test/actual_distances/synchronous/$i.json
    echo $i
    json_eq test/expected/$i.json test/actual_distances/synchronous/$i.json
    echo
done




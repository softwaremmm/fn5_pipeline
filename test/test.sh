#!/bin/bash
set -e

API_KEY=$1
SAMPLE_ID=$2

add_run() {
    #Create a run
    curl -SsL --fail-with-body --show-error \
    --header "Content-Type: application/json"  \
    --request POST  https://portal-dev.sp4.world/api/v1/samples/$SAMPLE_ID/runs \
    -H "Authorization: Basic $API_KEY" > run.json

    RUN_ID=$(cat run.json | jq ".id")

    #Mark the run as complete so we can create more
    curl -SsL --fail-with-body --show-error \
    --header "Content-Type: application/json"  \
    --request PATCH  https://portal-dev.sp4.world/api/v1/samples/$SAMPLE_ID/runs/$RUN_ID \
    -H "Authorization: Basic $API_KEY" -d '{"status": "Complete"}' > /dev/null

    echo $RUN_ID
}

get_dist(){
    local species=$1
    local run_id=$2
    curl -SsL --fail --show-error --header "Content-Type: application/json"  --request GET  https://portal-dev.sp4.world/api/v1/relatedness/$species/neighbours\?run_id\=$run_id -H "Authorization: Basic $API_KEY"
}


TEST_IDS=(
    1
    2
    3
    4
    5
    6
    7
)
RUN_IDS=()


#This runs all of the test cases in a sequential manner.
#This allows testing of FN6's distances easily, but won't test the queuing etc
for i in {1..7}; do
    echo Synchronous $i
    RUN_ID=$(add_run)
    RUN_IDS+=($RUN_ID)
    nextflow run . -profile docker --api_url https://portal-dev.sp4.world --sample $(pwd)/test/s$i.fasta --api_token $API_KEY --species test --run_id $RUN_ID --testing true --ref_fasta $(pwd)/test/NC_000962.3.fasta --mask $(pwd)/test/tb-exclude.txt --cutoff 20
done


#This runs all of the test cases in a non-sequential manner
#This should force use of the queuing system to ensure this works
#It makes use of a separate species so we should be able to compare side-by-side

#Note that this is non-deterministic so isn't the easiest thing to test
#   but this should give a good approximation of real use

for i in {0..6}; do
    echo Asynchronous ${TEST_IDS[$i]}
    nextflow run . -profile docker --api_url https://portal-dev.sp4.world --sample $(pwd)/test/s${TEST_IDS[$i]}.fasta --api_token $API_KEY --species test2 --run_id ${RUN_IDS[$i]} --testing true --ref_fasta $(pwd)/test/NC_000962.3.fasta --mask $(pwd)/test/tb-exclude.txt --cutoff 20 > ${TEST_IDS[$i]}.log &
    sleep 2
done

#Give it enough time to actually start
#Without this, we don't pick up all of the runs with `jobs -p`
sleep 10

for job in $(jobs -p); do
    if wait -n -p exitc ;then
        return_code="$?"
    else
        return_code="$?"
        #There was an error so dump the logs and exit loudly
        for f in $(ls | grep log); do
            echo $f
            cat $f
            echo
            echo
        done
        exit 1
    fi
done


#Check that our synchronous runs have expected values first
mkdir -p test/actual_distances/synchronous

for i in {0..6}; do
    get_dist test ${RUN_IDS[$i]} > test/actual_distances/synchronous/${TEST_IDS[$i]}.json
    echo Synchronous ${TEST_IDS[$i]}
    python3 test/check-distances.py --expected test/expected/${TEST_IDS[$i]}.json --actual test/actual_distances/synchronous/${TEST_IDS[$i]}.json --run-ids ${RUN_IDS[@]} --test-ids ${TEST_IDS[@]}
    echo
done

#Should be identical for async
mkdir -p test/actual_distances/asynchronous

for i in {0..6}; do
    get_dist test2 ${RUN_IDS[$i]} > test/actual_distances/asynchronous/${TEST_IDS[$i]}.json
    echo Asynchronous ${TEST_IDS[$i]}
    python3 test/check-distances.py --expected test/expected/${TEST_IDS[$i]}.json --actual test/actual_distances/asynchronous/${TEST_IDS[$i]}.json --run-ids ${RUN_IDS[@]} --test-ids ${TEST_IDS[@]}
    echo
done


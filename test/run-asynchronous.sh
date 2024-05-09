#!/bin/bash
set -xe

#This runs all of the test cases in a non-sequential manner
#This should force use of the queuing system to ensure this works
#It makes use of a separate species so we should be able to compare side-by-side

#Note that this is non-deterministic so isn't the easiest thing to test
#   but this should give a good approximation of real use

for i in {1..7}; do
    echo $i
    nextflow run . -profile docker --api_url http://127.0.0.1:8000 --sample $(pwd)/test/s$i.fasta --api_token test-api-key --species test2 --run_id $i --testing true --ref_fasta $(pwd)/test/NC_000962.3.fasta --mask $(pwd)/test/tb-exclude.txt --cutoff 20 > $i.log &
    sleep 1
done

#Give it enough time to actually start
#Without this, we don't pick up all of the runs with `jobs -p`
sleep 3

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
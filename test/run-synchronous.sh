#!/bin/bash
set -xe

#This runs all of the test cases in a sequential manner.
#This allows testing of FN6's distances easily, but won't test the queuing etc

for i in {1..7}; do
    echo $i
    sudo nextflow run . -profile docker --api_url http://127.0.0.1:8000 --sample $(pwd)/test/s$i.fasta --api_token test-api-key --species test --run_id $i --testing true --ref_fasta $(pwd)/test/NC_000962.3.fasta --mask $(pwd)/test/tb-exclude.txt --cutoff 20
done


#!/bin/bash
set -xe

#Insert the minimal data required to allow FN5 runs
#This is somewhat hacky - all of the runs here are the same sample, 
#   but as FN5 is the only part we care about, it's fine

#Choosing to use the API via curl for this rather than direct DB access as it's probably a little more stable

#All of the creation endpoints return the batch/sample IDs, but where req in payloads, they are difficult to variable interpolate because of JSON and bash both requiring double quotes
#Also, we functionally don't care what most of these values are, just that they exist
#Inserting a record should ensure that an ID of 1 works in these cases

#Create species
curl -SsL \
    --header "Content-Type: application/json"  \
    --request GET  http://0.0.0.0:8000/api/v1/species \
    -H "Authorization: Basic test-api-key" \
    -X POST \
    -d '{"species_name": "test"}'

#Create a batch
curl -SsL --fail --show-error \
    --header "Content-Type: application/json"  \
    --request POST  http://127.0.0.1:8000/api/v1/batches \
    -d '{"name": "test-batch", "status": "Created", "telemetry_data": {}, "quality": null, "is_approved": false, "is_shared": true}' \
    -H "Authorization: Basic test-api-key"

#Create a sample
touch dummy-sample-file.fastq.gz
curl -SsL --fail --show-error \
    --header "Content-Type: application/json"  \
    --request POST  http://127.0.0.1:8000/api/v1/samples \
    -d '{"batch_id": 1, "status": "Created", "collection_date": "2023-09-08", "control": false, "country": "GBR", "client_decontamination_reads_removed_proportion": 0, "client_decontamination_reads_in": 0, "client_decontamination_reads_out": 0, "district": "test", "instrument_platform": "illumina", "subdivision": "na", "specimen_organism": "tb", "checksum": "not-a-checksum", "is_shared": true}' \
    -H "Authorization: Basic test-api-key" > sample.json

SAMPLE_ID=$(cat sample.json | jq ".id")

curl -SsL --fail --show-error \
    -X 'POST' \
    "http://127.0.0.1:8000/api/v1/samples/$SAMPLE_ID/files" \
    -H 'accept: application/json' \
    -H 'Content-Type: multipart/form-data' \
    -F "file=@dummy-sample-file.fastq.gz;type=text/plain" \
    -H "Authorization: Basic test-api-key"

add_run(){
    #Create a run
    curl -SsL --fail --show-error \
    --header "Content-Type: application/json"  \
    --request POST  http://127.0.0.1:8000/api/v1/samples/$SAMPLE_ID/runs \
    -H "Authorization: Basic test-api-key" > run.json

    RUN_ID=$(cat run.json | jq ".id")

    #Mark the run as complete so we can create more
    curl -SsL --fail --show-error \
    --header "Content-Type: application/json"  \
    --request PATCH  http://127.0.0.1:8000/api/v1/samples/$SAMPLE_ID/runs/$RUN_ID \
    -H "Authorization: Basic test-api-key" -d '{"status": "Complete"}'
}

#Add some runs
for i in {1..20}; do
    echo Adding $i
    add_run
done

#Check we actually have runs
curl -SsL --header "Content-Type: application/json"  --request GET  http://127.0.0.1:8000/api/v1/samples/$SAMPLE_ID/runs -H "Authorization: Basic test-api-key" 





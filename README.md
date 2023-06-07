# fn5_pipeline
Nextflow wrapper for FN5

Currently functional, but far from optimal

## Running locally with docker
```
sudo nextflow run . -profile docker -latest --db_path <db connection string> --bucket <bucket PAR> --sample <fasta path>
```
Where:
* `<db connection string>` is the connection URL. Of the format `mysql://<user>:<password>@<url>:<port>/<db name>`
* `<bucket PAR>` is the pre-authenticated request URL for the save bucket
* `<fasta path>` is the full (absolute) path to a sample's FASTA file  

## Deleting from buckets
As deleting from a bucket is not supported by a bucket PAR, to enable this, nextflow secrets must be added.

1. Setup OCI CLI locally - generating a config file and an API key. This assumes your oci config is at `~/.oci/config` and your key is `~/.oci/oci_api_key.pem`
2. Store these as nextflow secrets:
    ```
    nextflow secrets set OCI_CONFIG "$(cat ~/.oci/config | base64 -w 0)"
    nextflow secrets set OCI_KEY "$(cat ~/.oci/oci_api_key.pem | base64 -w 0)"
    ```

## Process
![Sequential processing flowchart](fn5-sequential-processing-idea.png)
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

## Process
![Sequential processing flowchart](fn5-sequential-processing-idea.png)
# fn5_pipeline
Nextflow wrapper for FN5. Enables auto-queuing and auto-batching of samples for performance gains.

## Running locally with docker
Requires an API to be running to handle database and bucket operations
```
sudo nextflow run . -profile docker -latest --api_url <api URL> --sample <fasta path>
```
Where:
* `<api URL>` is the URL for the API
* `<fasta path>` is the full (absolute) path to a sample's FASTA file  

## Process
![Sequential processing flowchart](fn5-sequential-processing-idea.png)

## Error handling
As we are using a distributed locking mechanism, it is important to ensure that the lock is released upon failure.
Nextflow does not support this kind of try/catch behaviour natively, so use of `trap` to catch errors within processing steps allows an error log to track all errors - skipping processes as appropriate. This also allows the overarching pipeline to figure out which step failed and report it as such.

## GDPR data removal
To be GDPR compliant, we need to be able to delete user's saves upon request. This is currently not implemented, but the process would need to be something like:
1. Stop other processing. Probably through acquiring the lock, but could also be during planned downtime
2. Take a list of GUIDs to delete
3. Pull the saves tarball && decompress
4. Delete each of the GUIDS from the saves:
    ```
    for guid in to_delete;
    do
        rm saves/$guid*
    done
    ```
5. Recompress && upload
6. Release the lock (if applicable)



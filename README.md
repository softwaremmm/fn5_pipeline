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

## Testing
For the sake of your sanity, don't run the unit tests locally. The github actions is setup to install and run everything required, as well as pre-populate required records, and build the test dataset. This **can** be done locally, but it's up to you to ensure everything is populated!
See `.github/workflows/test.yaml` for an example of how this could be done locally.

As this test suite (and the pipeline) rely on API calls for running, as well as retriving results, `nf-test` was inappropriate.

## Conventional Commits
Use conventional commits when developing for this repo. 
You should install the pre-commits to check your commit messages.

You can install `pre-commit` using pip or conda and run
```bash
pre-commit install --hook-type commit-msg
```

If you have `npm` installed then you may be able to use `npx`, which is bundled with it, to avoid installing:
```bash
npx pre-commit install --hook-type commit-msg
``` 

Commitizen can help you write commits. 
Install commitizen and run `cz c` or use `npx`:
```bash
npx cz c
```

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

## Adding a new species
To add a new species, there are a few things which need to be done to avoid (sometimes) non-descript errors.
1. Add a row to the species table `insert into species(species_name) values("<species name>");`
    * Without this, you'll get a 404 with a message `species not found!`
2. Add a folder to the relatedness bucket `mkdir -p <relatedness bucket>/<species name>` or use the cloud interface
    * Without this, you'll get a 404 with no message
3. Add a subfolder to the relatedness bucket `mkdir -p <relatedness bucket>/<species name>/to_process` or use the cloud interface
    * Without this, you'll get a 500 with no message
4. Add a starting `all.tar.gz`. Either copy in saves (assuming run_id values are valid), or  `touch <relatedness bucket>/<species name>/all.tar.gz`.
    * Without this, you'll get a 404 with no message


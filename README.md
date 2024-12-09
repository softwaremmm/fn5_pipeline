# fn5_pipeline
Nextflow wrapper for FN5. Enables auto-queuing and auto-batching of samples for performance gains.

## Running locally with docker
Requires an API to be running to handle database and bucket operations. Run `bash local_setup.sh` on first run to ensure the expected bucket structure exists.
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

## Tags, Releases, and Committing
Use conventional commits. This is enforced with commitizen validate action and pre-commit hooks:
```bash
pre-commit install
```

This repo uses a standard gitflow approach, so changes should be first merged into develop and then released to main.
- In the develop branch semantic versioning is not used. Instead you can reference the commit hash to use it in a workflow.
- In a release branch you can create a release candidate with `cz bump a.b.c-rcX`. This also creates a tag.
- When release branch is ready for main run `cz bump a.b.c --files-only`. Manually write a human descriptive changelog. Then push these changes to main and make a release/tag there.

## Process
![Sequential processing flowchart](fn5-sequential-processing.drawio.png)

## Error handling
As we are using a distributed locking mechanism, it is important to ensure that the lock is released upon failure.
Nextflow does not support this kind of try/catch behaviour natively, so use of `trap` to catch errors within processing steps allows an error log to track all errors - skipping processes as appropriate. This also allows the overarching pipeline to figure out which step failed and report it as such.

## GDPR data removal
To be GDPR compliant, we need to be able to delete user's saves upon request. This is currently not implemented, but the process would need to be something like:
1. Stop other processing. Probably through acquiring the lock, but could also be during planned downtime
2. Take a list of GUIDs to delete
3. Delete each of the GUIDS from the saves:
    ```
    for guid in to_delete;
    do
        rm saves/$guid*
    done
    ```
4. Release the lock (if applicable)

## Adding a new species
To add a new species, there are a few things which need to be done to avoid (sometimes) non-descript errors.
1. Add a row to the species table `insert into species(species_name) values("<species name>");`
    * Without this, you'll get a 404 with a message `species not found!`
2. Add a folder to the relatedness bucket `mkdir -p <relatedness bucket>/<species name>` or use the cloud interface
    * Without this, you'll get a 404 with no message
3. Add a subfolder to the relatedness bucket `mkdir -p <relatedness bucket>/<species name>/to_process` or use the cloud interface
    * Without this, you'll get a 500 with no message
4. Add a subfolder to the relatedness bucket `mkdir -p <relatedness bucket>/<species name>/saves` or use the cloud interface
    * Without this, you'll get a 500 with no message

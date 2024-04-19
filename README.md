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

## Conventional Commits
Use conventional commits when developing for this repo. 
You should install the pre-commit hooks to check your commit messages.
You can also install `commitizen` to help with writing conventional commits.
You can install both through pip/conda. Or see [wiki for other options](https://github.com/GlobalPathogenAnalysisService/Wiki/blob/main/Commitizen.md#installing-commitizenpre-commit)

To install hooks run
```bash
pre-commit install --hook-type commit-msg
```

To make commit with commitizen run
```bash
cz c
```

## Tags and Releases

[Commitizen](https://commitizen-tools.github.io/commitizen/) is used to manage versioning of releases. This tool
can be used to make commits to this repository. Regardless, [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/) 
are required to ensure correct version numbering and changelog population.

**Do not add tags by hand.**

On merging a Pull Request a [GitHub action will run](.github/workflows/bump.yaml), causing Commitizen to:
* Determine the new [semver](https://semver.org/) based on conventional commits.
* Replace the previous semver in [.cz.toml](.cz.toml) and other files as specified therein.
* Update the [CHANGELOG](CHANGELOG.md) based on commit messages.
* Commit these changes to the `main` branch.
* Create a tag for this commit with the tag name of the newly determined semver.
* Create a new release from this tag.

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


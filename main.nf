#!/usr/bin/env nextflow

//Set DSL2 syntax
nextflow.enable.dsl=2

//Define ANSI colours for ease
ANSI_GREEN = "\033[1;32m"
ANSI_RESET = "\033[0m"


//Setup so --help triggers the help message
if (params.help) {
    exit(0)
}

if (params.sample == '') {
    log.info 'No sample given, aborting!'
}

///Add to FN5
process compute {
    input:
        path sample
    script:
        """
        echo \$(pwd)
        sample_path=\$(pwd)/$sample
        echo $sample
        echo "path to the fasta: \$sample_path"
        cd /FN5
        echo "DB_PATH=$params.db_path" >> .db
        echo "bucket=$params.bucket" >> .env
        python3 run.py --sample \$sample_path
        """
}

workflow {
    main:
        compute(params.sample)
}


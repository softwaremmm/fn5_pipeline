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

///Add to FN5
process compute {

    input:
        path sample
        path reference
        path mask
        path saves
    output:
        path "comparisons.txt"
    script:
        """
        echo Running FN5!
        echo "guid1 guid2 7" > comparisons.txt
        """
}

workflow {
    main:
        compute(params.sample, params.reference, params.mask, params.saves);
}

#!/usr/bin/env nextflow

//Set DSL2 syntax
nextflow.enable.dsl=2

//Define ANSI colours for ease
ANSI_GREEN = "\033[1;32m"
ANSI_RESET = "\033[0m"

//Ref compress sample & push to bucket
process reference_compress{
    input:
        path sample
    output:
        path "guid"
    script:
        """
        original_path=\$(pwd)
        sample_path=\$(pwd)/$sample

        cd /FN5

        mkdir -p sample-out

        guid=\$(./fn5 --reference_compress \$sample_path --saves_dir sample-out)

        #Check if this was a QC fail or not
        if [[ \$(echo \$guid | grep -E "\\|\\|QC_FAIL: .+\\|\\|" | wc -l) -eq 1 ]]; then
            #QC fail should pass onto check_lock
            #Then it should be recorded in the distances table, and no lock kept
            echo "\$guid" > \$original_path/guid
            exit 0
        fi

        cd sample-out
        tar --use-compress-program=pigz -cf \$(echo \$guid).tar.gz ./*

        echo \$guid.tar.gz
        ls -lhat
        
        curl -SsL --fail --show-error -X 'POST' \
            "$params.api_url/api/relatedness/$params.species/upload?path=to_process/\$(echo \$guid).tar.gz" \
            -H 'accept: application/json' \
            -H 'Content-Type: multipart/form-data' \
            -F "file=@\$(echo \$guid).tar.gz;type=application/gzip"

        echo \$guid > \$original_path/guid
        """ 
    stub:
        """
        touch guid
        """
}

//Check lock
process check_lock{
    input:
        path guid
    output:
        path "lock"
    script:
        """
        original_path=\$(pwd)
        guid=\$(cat $guid)

        cd /FN5

        #If this sample failed QC, mark it as failed in the distances table
        #And provide an empty lock to skip rest of computation
        if [[ \$(echo \$guid | grep -E "\\|\\|QC_FAIL: .+\\|\\|" | wc -l) -eq 1 ]]; then
            g=\$(echo "\$guid" | tail -n 1)
            echo \$g 
            echo "\$g ||QC_FAIL|| -1" > qc_fail_comparison.txt

            curl -SsL --fail --show-error -X 'POST' \
                '$params.api_url/api/relatedness/$params.species/db/add_distances' \
                -H 'accept: application/json' \
                -H 'Content-Type: multipart/form-data' \
                -F 'file=@qc_fail_comparison.txt;type=text/plain'
            
            touch \$original_path/lock
            exit 0
        fi

        #Add the lock
        curl -SsL --fail --show-error -X 'GET' \
            '$params.api_url/api/relatedness/$params.species/db/test10/check_lock' \
            -H 'accept: application/json' | jq ".lock" | tr -d \\" > \$original_path/lock

        #Because strings are null byte terminated, this will give a file containing 1 null byte if added to batch
        #Catch this and make it empty
        echo -e "" > null_byte.txt
        #This needs the `||` clause or it exits with an error 
        cmp --silent \$original_path/lock null_byte.txt && \$(rm \$original_path/lock && touch \$original_path/lock) || cat \$original_path/lock

        """
    stub:
        """
        touch lock
        """
}

//Wait for lock
process wait_for_lock{
    input:
        path lock
    output:
        path "ok"
    script:
        //Using the Nextflow `when` guard didn't seem to work for checking if $lock is empty...
        """
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            touch ok
            exit 0
        fi

        original_path=\$(pwd)
        lock=\$(cat $lock)

        waiting=1
        while [ \$waiting -eq 1 ];
        do
            #Use the API to get the next lock in the table
            curl -SsL --fail --show-error -X 'GET' \
                '$params.api_url/api/relatedness/$params.species/db/next_lock' \
                -H 'accept: application/json' | jq ".lock" > next_lock.txt

            #Compare the outputs, if equal, break from the loop, else sleep and try again
            cmp --silent $lock next_lock.txt && waiting=2 || sleep 1
        done

        #Wait for the lock
        touch \$original_path/ok
        """
    stub:
        """
        touch ok
        """
}

//Get batch
process get_batch{
    input:
        path guid
        path lock
        path unlocked
    output:
        path "batch_guids.txt"
    script:
        """
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            touch batch_guids.txt
            exit 0
        fi

        original_path=\$(pwd)
        guid=\$(cat $guid)

        #Get guids for this batch
        curl -SsL --fail --show-error -X 'GET' \
            '$params.api_url/api/relatedness/$params.species/db/get_batch' \
            -H 'accept: application/json' | jq ".batch[]" | tr -d \\" > batch_guids.txt

        #Add own guid too
        echo \$guid >> batch_guids.txt
        """
    stub:
        """
        touch batch_guids.txt
        """

}

//Pull saves from bucket
process get_saves{
    input:
        path lock
        path batch
        path unlocked
    output:
        path "all.tar.gz"
        path "to_process/*", emit: to_process
    script:
        """
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            touch all.tar.gz
            mkdir -p to_process
            touch to_process/no
            exit 0
        fi
        curl -SsL --fail --show-error -X 'GET' \
            '$params.api_url/api/relatedness/$params.species/download?path=all.tar.gz' \
            -H 'accept: application/gzip' > all.tar.gz

        mkdir -p to_process

        #Fetch the batch
        for f in \$(cat $batch); do
            curl -SsL --fail --show-error -X 'GET' \
                "$params.api_url/api/relatedness/$params.species/download?path=to_process/\$f.tar.gz" \
                -H 'accept: application/gzip' > to_process/\$f.tar.gz
        done
        """
    stub:
        """
        touch all.tar.gz
        mkdir -p to_process
        touch to_process/filename.tar.gz
        """
}

//Do comparisons
process process_batch{
    input:
        path lock
        path all
        path to_process
    output:
        path "comparisons.txt"
        path "all.tar.gz"
    script:
        """
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            touch comparisons.txt
            exit 0
        fi

        original_path=\$(pwd)

        mkdir -p /FN5/batch
        mkdir -p /FN5/saves

        #Extract existing saves
        tar --use-compress-program=pigz -xf all.tar.gz -C /FN5

        #Decompress all of the samples in this batch
        to_process=\$(echo $to_process)
        for f in \$(echo \${to_process});
        do
            tar --use-compress-program=pigz -xf \$f -C /FN5/batch
        done

        cd /FN5

        ./fn5 --add_batch batch --cutoff 20 > \$original_path/comparisons.txt

        mv batch/* saves

        tar --use-compress-program=pigz -cf \$original_path/all.tar.gz saves
        """
    stub:
        """
        touch comparisons.txt
        touch all.tar.gz
        """
}

//Add to DB
process add_to_db{
    input:
        path to_process
        path comparisons
        path lock
    output:
        path done
    script:
        """
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            touch done
            exit 0
        fi

        original_path=\$(pwd)

        #Checking for orphan nodes. Add a -1 record for these
        to_process=\$(echo $to_process)
        for f in \$(echo \${to_process});
        do
            guid=\$(python3 -c "print('\$f'.replace('.tar.gz', ''))")
            if [ \$(cat $comparisons | grep \$guid | wc -l) -eq 0  ]; then
                echo \$guid \$guid -1 >> $comparisons
            fi
        done

        #Add to DB

        curl --fail --show-error -X 'POST' \
            '$params.api_url/api/relatedness/$params.species/db/add_distances' \
            -H 'accept: application/json' \
            -H 'Content-Type: multipart/form-data' \
            -F "file=@$comparisons;type=text/plain"

        #Add dummy output
        touch \$original_path/done
        """
    stub:
        """
        touch done
        """
}

//Update bucket
process clean_up{
    input:
        path lock
        path batch
        path all
        path done_processing
    output:
        path cleaned_up
    script:
        """
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            touch cleaned_up
            exit 0
        fi


        time curl -SsL --fail --show-error -X 'POST' \
            '$params.api_url/api/relatedness/$params.species/db/clear_batch' \
            -H 'accept: application/json' \
            -H 'Content-Type: multipart/form-data' \
            -F 'file=@$batch;type=text/plain'

        #Update the saves tarball
        time curl -SsL --fail --show-error -X 'POST' \
            "$params.api_url/api/relatedness/$params.species/upload?path=all.tar.gz" \
            -H 'accept: application/json' \
            -H 'Content-Type: multipart/form-data' \
            -F "file=@$all;type=application/gzip"        
            
        touch cleaned_up
        """
    stub:
        """
        touch cleaned_up
        """
}

process remove_batch{
    input:
        path lock
        path batch
        path done_processing
    output:
        path batch_removed
    script:
        """
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            touch batch_removed
            exit 0
        fi

        #Rows of \$batch are <guid>, we need to_process/<guid>/tar.gz for deletion
        touch fixed_batch.txt
        for line in \$(cat $batch);
        do
            echo -e "to_process/\$line.tar.gz" >> fixed_batch.txt
        done

        curl -SsL --fail --show-error -X 'POST' \
            "$params.api_url/api/relatedness/$params.species/delete" \
            -H 'accept: application/json' \
            -H 'Content-Type: multipart/form-data' \
            -F "file=@fixed_batch.txt;type=text/plain"

        touch batch_removed

        """
    stub:
        """
        touch batch_removed
        """
}

//Release lock
process release_lock{
    input:
        path lock
        path cleared_batch
        path removed_batch
    script:
        """
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            exit 0
        fi

        curl --fail --show-error -X 'GET' \
            "$params.api_url/api/relatedness/$params.species/db/clear_lock?lock=\$(cat lock)" \
            -H 'accept: application/json'
        """
    stub:
        """
        echo lock released
        """
}

//Split into separate workflow to enable importing
workflow find_neighbour_5{
    main:

        //Setup so --help triggers the help message
        if (params.help) {
            log.info """
            ========================================================================
            Find Neighbour 5

            Fast SNP distance calculation from disk.

            Parameters:
            ------------------------------------------------------------------------
            --db_path   DB connection string of the format mysql://<user>:<password>@<host>:<port>/<db name>
            --bucket    Pre authenticated request URL for a given bucket
            --sample    Path to the sample's FASTA file
            --species   Name of the species this belongs to. Default = 'tb'
            """
            .stripIndent()
            exit(0)
        }

        if (params.sample == '') {
            log.info 'No sample given, aborting!'
            exit(1)
        }
        log.info """
        ========================================================================
        Find Neighbour 5

        Parameters used:
        ------------------------------------------------------------------------
        --db_path   $params.db_path
        --bucket    $params.bucket
        --sample    $params.sample
        --species   $params.species

        Runtime data:
        ------------------------------------------------------------------------
        Running with profile  ${ANSI_GREEN}${workflow.profile}${ANSI_RESET}
        Running as user       ${ANSI_GREEN}${workflow.userName}${ANSI_RESET}
        Launch directory      ${ANSI_GREEN}${workflow.launchDir}${ANSI_RESET}
        """
        .stripIndent()

        guid = reference_compress(params.sample)
        lock = check_lock(guid)

        //To stop Nextflow running this out of order, we need to use a dummy output fed into downstream processes
        check = wait_for_lock(lock)

        batch = get_batch(guid, lock, check)
        (all, to_process) = get_saves(lock, batch, check)

        (comparisons, all2) = process_batch(lock, all, to_process)

        done = add_to_db(to_process, comparisons, lock)

        cleaned_up = clean_up(lock, batch, all2, done)
        batch_removed = remove_batch(lock, batch, done)
        
        release_lock(lock, cleaned_up, batch_removed)
}

workflow{
    main:
        //TODO: Add API token once integrated into GPAS API
        find_neighbour_5()
}


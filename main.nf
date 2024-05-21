#!/usr/bin/env nextflow

//Set DSL2 syntax
nextflow.enable.dsl=2

//Define ANSI colours for ease
ANSI_GREEN = "\033[1;32m"
ANSI_RESET = "\033[0m"

if ("${workflow.profile}" != 'kubernetes') {
    params.relatedness_bucket = "$projectDir/data/relatedness"
} else {
    params.relatedness_bucket = "/workspace/buckets/relatedness_bucket"
}

//Ref compress sample & push to bucket
process reference_compress{
    container = "lhr.ocir.io/lrbvkel2wjot/oxfordmmm/fn5:v2.0.2"
    cpus = 1
    memory = {
        params.testing=="" ? "2GB" : "1GB"
    }

    debug true
    pod label: "name", value: "fn5_pipeline:reference_compress"
    pod label: "sample_id", value: "${params.sample_id}"
    pod label: "run_id", value: "${params.run_id}"

    input:
        path sample
        val species
        val api_url
        val api_token
        path relatedness_bucket
    output:
        path "guid"
    script:
        """
        if [ ${workflow.profile} == 'kubernetes' ]
        then
            #Use the secret if running via k8s
            API_KEY=\$(cat /etc/nextflow-api-key/nextflow_api_key)
        else
            API_KEY="$api_token"
        fi

        original_path=\$(pwd)
        sample_path=\$(pwd)/$sample

        cd /FN5

        mkdir -p sample-out

        guid=\$(./fn5 --reference_compress \$sample_path --guid $params.run_id --saves_dir sample-out)

        #Check if this was a QC fail or not
        if [[ \$(echo \$guid | grep -E "\\|\\|QC_FAIL: .+\\|\\|" | wc -l) -eq 1 ]]; then
            #QC fail should pass onto check_lock
            #Then it should be recorded in the distances table, and no lock kept
            echo "\$guid" > \$original_path/guid
            exit 0
        fi

        cd sample-out
        tar --use-compress-program=pigz -cf \$(echo \$guid).tar.gz ./*

        cp \$(echo \$guid).tar.gz \$original_path/$relatedness_bucket/$species/to_process/

        echo \$guid > \$original_path/guid
        """ 
    stub:
        """
        touch guid
        echo Reference compressed
        """
}

//Check lock
process check_lock{
    container = "lhr.ocir.io/lrbvkel2wjot/oxfordmmm/fn5:v2.0.2"
    cpus = 1
    memory = {
        params.testing=="" ? "2GB" : "1GB"
    }

    debug true
    pod label: "name", value: "fn5_pipeline:check_lock"
    pod label: "sample_id", value: "${params.sample_id}"
    pod label: "run_id", value: "${params.run_id}"

    input:
        path guid
        val species
        val api_url
        val api_token
    output:
        path "lock"
    script:
        """
        if [ ${workflow.profile} == 'kubernetes' ]
        then
            #Use the secret if running via k8s
            API_KEY=\$(cat /etc/nextflow-api-key/nextflow_api_key)
        else
            API_KEY="$api_token"
        fi

        original_path=\$(pwd)
        guid=\$(cat $guid)

        cd /FN5

        #If this sample failed QC, mark it as failed in the distances table
        #And provide an empty lock to skip rest of computation
        if [[ \$(echo \$guid | grep -E "\\|\\|QC_FAIL: .+\\|\\|" | wc -l) -eq 1 ]]; then
            g=\$(echo "\$guid" | tail -n 1)
            echo "\$g ||QC_FAIL|| -1" > qc_fail_comparison.txt

            curl -SsL --fail --show-error --retry-all-errors --retry 5 --retry-delay 20 -X 'POST' \
                '$api_url/api/v1/relatedness/$species/db/add_distances' \
                -H 'accept: application/json' \
                -H 'Content-Type: multipart/form-data' \
                -F 'file=@qc_fail_comparison.txt;type=text/plain' \
                -H "Authorization: Basic \$API_KEY"
            
            touch \$original_path/lock
            touch \$original_path/error_log
            exit 0
        fi

        #Add the lock
        curl -SsL --fail --show-error --retry-all-errors --retry 5 --retry-delay 20 -X 'GET' \
            "$api_url/api/v1/relatedness/$species/db/\$guid/check_lock" \
            -H 'accept: application/json' \
            -H "Authorization: Basic \$API_KEY" > lock.json
        cat lock.json | jq ".lock" | tr -d \\" > \$original_path/lock

        #The lock is the literal string 'null' if added to batch
        echo null > trial_lock
        cmp --silent \$original_path/lock trial_lock && (echo lock was null && rm \$original_path/lock && touch \$original_path/lock) || (echo lock was not null && cat \$original_path/lock)

        #Because strings are null byte terminated, this will give a file containing 1 null byte if added to batch
        #Catch this and make it empty
        #echo -e "" > null_byte.txt
        #This needs the `||` clause or it exits with an error 
        #cmp --silent \$original_path/lock null_byte.txt && \$(rm \$original_path/lock && touch \$original_path/lock) || cat \$original_path/lock

        """
    stub:
        """
        touch lock
        echo Added lock
        """
}

//Wait for lock
process wait_for_lock{
    container = "lhr.ocir.io/lrbvkel2wjot/oxfordmmm/fn5:v2.0.2"
    cpus = 1
    memory = {
        params.testing=="" ? "2GB" : "1GB"
    }

    debug true
    pod label: "name", value: "fn5_pipeline:wait_for_lock"
    pod label: "sample_id", value: "${params.sample_id}"
    pod label: "run_id", value: "${params.run_id}"

    input:
        path lock
        val species
        val api_url
        val api_token
    output:
        path error_log
    script:
        //Using the Nextflow `when` guard didn't seem to work for checking if $lock is empty...
        """
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            touch error_log
            exit 0
        fi

        if [ ${workflow.profile} == 'kubernetes' ]
        then
            #Use the secret if running via k8s
            API_KEY=\$(cat /etc/nextflow-api-key/nextflow_api_key)
        else
            API_KEY="$api_token"
        fi

        original_path=\$(pwd)
        lock=\$(cat $lock)
        trap "echo -e 'Failed to get lock\n' >> \$original_path/error_log && exit 0" SIGINT SIGTERM ERR 

        waiting=1
        while [ \$waiting -eq 1 ];
        do
            #Use the API to get the next lock in the table
            curl -SsL --fail --show-error --retry-all-errors --retry 5 --retry-delay 20 -X 'GET' \
                '$api_url/api/v1/relatedness/$species/db/next_lock' \
                -H 'accept: application/json' \
                -H "Authorization: Basic \$API_KEY" > lock.json
            cat lock.json | jq ".lock" > next_lock.txt

            #Compare the outputs, if equal, break from the loop, else sleep and try again
            cmp --silent $lock next_lock.txt && waiting=2 || sleep 1
        done

        #Wait for the lock
        touch \$original_path/error_log
        """
    stub:
        """
        touch error_log
        echo Got lock
        """
}

//Get batch
process get_batch{
    container = "lhr.ocir.io/lrbvkel2wjot/oxfordmmm/fn5:v2.0.2"
    cpus = 1
    memory = {
        params.testing=="" ? "2GB" : "1GB"
    }

    debug true
    pod label: "name", value: "fn5_pipeline:get_batch"
    pod label: "sample_id", value: "${params.sample_id}"
    pod label: "run_id", value: "${params.run_id}"

    input:
        path guid
        path lock
        path error_log
        val species
        val api_url
        val api_token
    output:
        path "batch_guids.txt"
        path error_log
    script:
        """
        set +e
        trap "echo -e 'Failed to add to get batch\n' >> $error_log && touch batch_guids.txt && exit 0" SIGINT SIGTERM ERR
        if [ -s $error_log ]; then
            #Error occured upstream so skip this step
            echo 'Skipped get_batch' >> $error_log
            touch batch_guids.txt
            exit 0
        fi
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            touch batch_guids.txt
            exit 0
        fi

        if [ ${workflow.profile} == 'kubernetes' ]
        then
            #Use the secret if running via k8s
            API_KEY=\$(cat /etc/nextflow-api-key/nextflow_api_key)
        else
            API_KEY="$api_token"
        fi

        original_path=\$(pwd)
        guid=\$(cat $guid)

        #Get guids for this batch
        #Split into two commands as errors are not percolated through the pipe
        curl -SsL --fail --show-error --retry-all-errors --retry 5 --retry-delay 20 -X 'GET' \
            '$api_url/api/v1/relatedness/$species/db/get_batch' \
            -H 'accept: application/json' \
            -H "Authorization: Basic \$API_KEY" > batch.json
        
        cat batch.json | jq ".batch[]" | tr -d \\" > batch_guids.txt

        #Add own guid too
        echo \$guid >> batch_guids.txt
        """
    stub:
        """
        touch batch_guids.txt
        echo Got batch
        """

}

//Pull saves from bucket
process get_saves{
    container = "lhr.ocir.io/lrbvkel2wjot/oxfordmmm/fn5:v2.0.2"
    cpus = 1
    memory = {
        params.testing=="" ? "3GB" : "1GB"
    }

    debug true
    pod label: "name", value: "fn5_pipeline:get_saves"
    pod label: "sample_id", value: "${params.sample_id}"
    pod label: "run_id", value: "${params.run_id}"

    input:
        path lock
        path batch
        path error_log
        val species
        val api_url
        val api_token
        path relatedness_bucket
    output:
        path "to_process/*", emit: to_process
        path error_log
    script:
        """
        trap "echo -e 'Failed to add to get saves\n' >> $error_log && mkdir -p to_process && touch to_process/no && exit 0" SIGINT SIGTERM ERR
        if [ -s $error_log ]; then
            #Error occured upstream so skip this step
            echo 'Skipped get_saves' >> $error_log
            mkdir -p to_process
            touch to_process/no
            exit 0
        fi        
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            mkdir -p to_process
            touch to_process/no
            exit 0
        fi

        if [ ${workflow.profile} == 'kubernetes' ]
        then
            #Use the secret if running via k8s
            API_KEY=\$(cat /etc/nextflow-api-key/nextflow_api_key)
        else
            API_KEY="$api_token"
        fi

        mkdir -p to_process

        #Fetch the batch
        for f in \$(cat $batch); do
            cp $relatedness_bucket/$species/to_process/\$f.tar.gz to_process/\$f.tar.gz
        done
        """
    stub:
        """
        mkdir -p to_process
        touch to_process/filename.tar.gz
        echo Got saves
        """
}

//Do comparisons
process process_batch{
    container = "lhr.ocir.io/lrbvkel2wjot/oxfordmmm/fn5:v2.0.2"
    cpus = {
        params.testing=="" ? 6 : 1
    }
    memory = {
        params.testing=="" ? "32GB" : "1GB"
    }

    debug true
    pod label: "name", value: "fn5_pipeline:process_batch"
    pod label: "sample_id", value: "${params.sample_id}"
    pod label: "run_id", value: "${params.run_id}"

    input:
        path lock
        path to_process
        path error_log
        path relatedness_bucket
        val species
    output:
        path "comparisons.txt"
        path error_log
    script:
        """
        original_path=\$(pwd)
        trap add_to_error_log SIGINT SIGTERM ERR

        function add_to_error_log(){
            echo -e 'Failed to process batch' >> \$original_path/$error_log
            echo -e '$to_process \n' >> \$original_path/$error_log
            touch \$original_path/comparisons.txt
            exit 0
        }

        if [ -s $error_log ]; then
            #Error occured upstream so skip this step
            echo 'Skipped process_batch' >> $error_log
            touch comparisons.txt
            exit 0
        fi
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            touch comparisons.txt
            exit 0
        fi

        mkdir -p /FN5/batch
        mkdir -p /FN5/saves


        # Check if we have up to date saves in both the bucket and PVC
        # Merging the saves as required to ensure both are up to date
        # Use the PVC for actual computation though for speed
        
        # Ideally, this shouldn't need to do anything, but check anyway
        mkdir -p /workspace/relatedness-saves/$species
    
        # This could take ~20s but worth it for the check
        ls \$original_path/$relatedness_bucket/$species/saves > bucket-saves.txt
        ls /workspace/relatedness-saves/$species > pvc-saves.txt
        
        # Check if there's any bucket saves we haven't got yet
        # This is a neat way to get set difference of files https://stackoverflow.com/a/13038235
        sort bucket-saves.txt pvc-saves.txt pvc-saves.txt | uniq -u > not-in-pvc.txt
        sort pvc-saves.txt bucket-saves.txt bucket-saves.txt | uniq -u > not-in-bucket.txt

        # Sync the PVC with the bucket
        for filename in \$(cat not-in-pvc.txt); do
            cp \$original_path/$relatedness_bucket/$species/saves/\$filename /workspace/relatedness-saves/$species
        done

        # Sync the bucket with the PVC - this should only do stuff if there was an error
        for filename in \$(cat not-in-bucket.txt); do
            cp /workspace/relatedness-saves/$species/\$filename \$original_path/$relatedness_bucket/$species/saves/
        done

        #Decompress all of the samples in this batch
        to_process=\$(echo $to_process)
        for f in \$(echo \${to_process});
        do
            tar --use-compress-program=pigz -xf \$f -C /FN5/batch
        done

        cd /FN5

        ./fn5 --add_batch batch --cutoff 20 --saves_dir /workspace/relatedness-saves/$species > \$original_path/comparisons.txt

        cp -f batch/* \$original_path/$relatedness_bucket/$species/saves
        cp -f batch/* /workspace/relatedness-saves/$species

        #TODO: REMOVE ONCE DEPLOYED TO ALL ENVS
        # At this point, everything on the PVC should be new-style saves
        # So clear old-style saves from the bucket (if existing)
        # This shouldn't add much (significant) overhead if there's no old-style saves
        for save in \$(cat \$original_path/bucket-saves.txt); do
            if [[ \$save == *.fn5 ]]; then
                continue
            fi
            rm \$original_path/$relatedness_bucket/$species/saves/\$save
        done
        """
    stub:
        """
        touch comparisons.txt
        echo Processed batch
        """
}

//Add to DB
process add_to_db{
    container = "lhr.ocir.io/lrbvkel2wjot/oxfordmmm/fn5:v2.0.2"
    cpus = 1
    memory = {
        params.testing=="" ? "2GB" : "1GB"
    }

    debug true
    pod label: "name", value: "fn5_pipeline:add_to_db"
    pod label: "sample_id", value: "${params.sample_id}"
    pod label: "run_id", value: "${params.run_id}"

    input:
        path to_process
        path comparisons
        path lock
        path error_log
        val species
        val api_url
        val api_token
    output:
        path error_log
    script:
        """
        set +e
        
        trap "echo 'Failed to add to DB: ' >> $error_log && cat $comparisons >> $error_log && echo "" >> $error_log && exit 0" SIGINT SIGTERM ERR

        if [ -s $error_log ]; then
            #Error occured upstream so skip this step
            echo 'Skipped add_to_db' >> $error_log
            echo "Skipping add_to_db"
            exit 0
        fi        
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            exit 0
        fi

        if [ ${workflow.profile} == 'kubernetes' ]
        then
            #Use the secret if running via k8s
            API_KEY=\$(cat /etc/nextflow-api-key/nextflow_api_key)
        else
            API_KEY="$api_token"
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

        curl --fail --show-error --retry-all-errors --retry 5 --retry-delay 20 -X 'POST' \
            '$api_url/api/v1/relatedness/$species/db/add_distances' \
            -H 'accept: application/json' \
            -H 'Content-Type: multipart/form-data' \
            -F "file=@$comparisons;type=text/plain" \
            -H "Authorization: Basic \$API_KEY"
        """
    stub:
        """
        echo "Added to DB"
        """
}

//Update bucket
process clean_up{
    container = "lhr.ocir.io/lrbvkel2wjot/oxfordmmm/fn5:v2.0.2"
    cpus = 1
    memory = {
        params.testing=="" ? "2GB" : "1GB"
    }

    debug true
    pod label: "name", value: "fn5_pipeline:clean_up"
    pod label: "sample_id", value: "${params.sample_id}"
    pod label: "run_id", value: "${params.run_id}"

    input:
        path lock
        path batch
        path error_log
        val species
        val api_url
        val api_token
        path relatedness_bucket
    output:
        path error_log
    script:
        """
        set +e
        trap add_to_error_log SIGINT SIGTERM ERR

        function add_to_error_log(){
            echo 'Failed to clean up the batch table: ' >> $error_log
            cat $batch >> $error_log
            echo -e 'Saves have not been updated!\n' >> $error_log
            exit 0
        }

        if [ -s $error_log ]; then
            #Error occured upstream so skip this step
            echo 'Skipped clean_up' >> $error_log
            exit 0
        fi
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            touch cleaned_up
            exit 0
        fi

        if [ ${workflow.profile} == 'kubernetes' ]
        then
            #Use the secret if running via k8s
            API_KEY=\$(cat /etc/nextflow-api-key/nextflow_api_key)
        else
            API_KEY="$api_token"
        fi


        curl -SsL --fail --show-error --retry-all-errors --retry 5 --retry-delay 20 -X 'POST' \
            '$api_url/api/v1/relatedness/$species/db/clear_batch' \
            -H 'accept: application/json' \
            -H 'Content-Type: multipart/form-data' \
            -F 'file=@$batch;type=text/plain' \
            -H "Authorization: Basic \$API_KEY"

        """
    stub:
        """
        echo Cleared up batch + updated saves
        """
}

process remove_batch{
    container = "lhr.ocir.io/lrbvkel2wjot/oxfordmmm/fn5:v2.0.2"
    cpus = 1
    memory = {
        params.testing=="" ? "2GB" : "1GB"
    }

    debug true
    pod label: "name", value: "fn5_pipeline:remove_batch"
    pod label: "sample_id", value: "${params.sample_id}"
    pod label: "run_id", value: "${params.run_id}"

    input:
        path lock
        path batch
        path error_log
        val species
        val api_url
        val api_token
        path relatedness_bucket
    output:
        path error_log
    script:
        """
        set +e
        trap "echo 'Failed to add to clean up bucket: ' >> $error_log && cat $batch >> $error_log && echo "" >> $error_log && exit 0" SIGINT SIGTERM ERR
        if [ -s $error_log ]; then
            #Error occured upstream so skip this step
            echo 'Skipped remove_batch' >> $error_log
            exit 0
        fi        
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            exit 0
        fi

        if [ ${workflow.profile} == 'kubernetes' ]
        then
            #Use the secret if running via k8s
            API_KEY=\$(cat /etc/nextflow-api-key/nextflow_api_key)
        else
            API_KEY="$api_token"
        fi

        for line in \$(cat $batch);
        do
            rm $relatedness_bucket/$species/to_process/\$line.tar.gz
        done


        """
    stub:
        """
        echo Cleaned up bucket
        """
}

//Release lock
process release_lock{
    container = "lhr.ocir.io/lrbvkel2wjot/oxfordmmm/fn5:v2.0.2"
    cpus = 1
    memory = {
        params.testing=="" ? "2GB" : "1GB"
    }

    debug true
    pod label: "name", value: "fn5_pipeline:release_lock"
    pod label: "sample_id", value: "${params.sample_id}"
    pod label: "run_id", value: "${params.run_id}"

    input:
        path lock
        path error_log
        val species
        val api_url
        val api_token
    script:
        """
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            exit 0
        fi

        if [ ${workflow.profile} == 'kubernetes' ]
        then
            #Use the secret if running via k8s
            API_KEY=\$(cat /etc/nextflow-api-key/nextflow_api_key)
        else
            API_KEY="$api_token"
        fi

        curl --fail --show-error --retry-all-errors --retry 5 --retry-delay 20 -X 'GET' \
            "$api_url/api/v1/relatedness/$species/db/clear_lock?lock=\$(cat lock)" \
            -H 'accept: application/json' \
            -H "Authorization: Basic \$API_KEY" \

        #if [ -s $error_log ]; then
        #    #Error occured upstream so now we have released the lock, throw it
        #    cat $error_log > /dev/stderr
        #    exit 1
        #fi  
        """
    stub:
        """
        echo lock released
        """
}

//Split into separate workflow to enable importing
workflow find_neighbour_5{
    take:
        sample
        species
        api_url
        api_token
        relatedness_bucket

    main:
        /**
        Error handling here is obviously not as neat I'd like it,
        but Nextflow doesn't support try/catch to call another process
        so in absence of a neat solution, use of `trap` and percolating an error log
        works, but definitely isn't ideal.
        */

        guid = reference_compress(sample, species, api_url, api_token, relatedness_bucket)
        lock = check_lock(guid, species, api_url, api_token)

        error_log = wait_for_lock(lock, species, api_url, api_token)

        (batch, error_log) = get_batch(guid, lock, error_log, species, api_url, api_token)
        (to_process, error_log) = get_saves(lock, batch, error_log, species, api_url, api_token, relatedness_bucket)

        (comparisons, error_log) = process_batch(lock, to_process, error_log, relatedness_bucket, species)

        error_log = add_to_db(to_process, comparisons, lock, error_log, species, api_url, api_token)

        error_log = clean_up(lock, batch, error_log, species, api_url, api_token, relatedness_bucket)
        error_log = remove_batch(lock, batch, error_log, species, api_url, api_token, relatedness_bucket)

        release_lock(lock, error_log, species, api_url, api_token)

    emit:
        error_log
}

workflow{
    main:
        // Setup so --help triggers the help message
        if (params.help) {
            log.info """
            ========================================================================
            Find Neighbour 5

            Fast SNP distance calculation from disk.

            Parameters:
            ------------------------------------------------------------------------
            --sample             Path to the sample's FASTA file
            --species            Name of the species this belongs to. Default = 'tb'
            --api_url            URL for the GPAS API
            --api_token          Access token for the API
            --relatedness_bucket Path to the relatedness bucket. Default = '$projectDir/data/relatedness' for local runnning
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
        --sample             $params.sample
        --species            $params.species
        --api_url            $params.api_url
        --relatedness_bucket $params.relatedness_bucket

        Runtime data:
        ------------------------------------------------------------------------
        Running with profile  ${ANSI_GREEN}${workflow.profile}${ANSI_RESET}
        Running as user       ${ANSI_GREEN}${workflow.userName}${ANSI_RESET}
        Launch directory      ${ANSI_GREEN}${workflow.launchDir}${ANSI_RESET}
        """
        .stripIndent()
        
        find_neighbour_5(params.sample, params.species, params.api_url, params.api_token, params.relatedness_bucket)
}


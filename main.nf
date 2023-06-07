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
        echo \$guid
        echo This is for $params.species

        cd sample-out
        tar --use-compress-program=pigz -cf \$(echo \$guid).tar.gz ./*
        curl -X PUT --data-binary "@\$(pwd)/\$(echo \$guid).tar.gz" $params.bucket/$params.species/to_process/\$(echo \$guid).tar.gz

        echo \$guid > \$original_path/guid
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

        #Make sure the DB is setup
        echo "DB_PATH=$params.db_path" >> .db
        
        #Add the lock
        python3 add_lock.py --guid \$guid > \$original_path/lock
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

        echo Running wait for lock
        original_path=\$(pwd)
        lock=\$(cat $lock)

        cd /FN5

        #Make sure the DB is setup
        echo "DB_PATH=$params.db_path" >> .db

        #Wait for the lock
        echo Lock were waiting for is: \$lock
        python3 wait-for-lock.py --lock \$lock
        touch \$original_path/ok
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

        cd /FN5

        #Make sure the DB is setup
        echo "DB_PATH=$params.db_path" >> .db

        #Get the batch details
        python3 batch-process.py --get --id \$guid
        #Add own guid too
        echo \$guid >> \$(echo \$guid)_batch_guids.txt

        #Move to original dir for output
        mv \$(echo \$guid)_batch_guids.txt \$original_path/batch_guids.txt
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

        original_path=\$(pwd)

        echo Getting all
        time curl -SsL $params.bucket/$params.species/all.tar.gz > all.tar.gz
        echo

        mkdir -p to_process

        #Fetch the batch
        for f in \$(cat $batch); do
            echo Getting to_process/\$f.tar.gz
            time curl -SsL $params.bucket/$params.species/to_process/\$f.tar.gz > to_process/\$f.tar.gz
        done

        ls to_process
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
    script:
        """
        if ! [ -s $lock ]; then
            #Sample in batch rather than lock table, so exit
            touch comparisons.txt
            exit 0
        fi

        original_path=\$(pwd)
        ls -lhat
        echo
        echo "Started in \$original_path"

        mkdir -p /FN5/batch
        mkdir -p /FN5/saves

        #Extract existing saves
        tar --use-compress-program=pigz -xf all.tar.gz -C /FN5
        ls /FN5/saves | wc -l

        #Decompress all of the samples in this batch
        to_process=\$(echo $to_process)
        for f in \$(echo \${to_process});
        do
            tar --use-compress-program=pigz -xf \$f -C /FN5/batch
            find /FN5/batch
        done

        cd /FN5

        ./fn5 --add_batch batch --cutoff 20 > \$original_path/comparisons.txt

        echo From file
        cat \$original_path/comparisons.txt
        cat \$original_path/comparisons.txt | wc -l
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
        cd /FN5
        #Make sure the DB is setup
        echo "DB_PATH=$params.db_path" >> .db
        python3 add-to-db.py --comparisons \$original_path/comparisons.txt

        #Add dummy output
        touch \$original_path/done
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

        original_path=\$(pwd)


        cd /FN5
        #Make sure the DB is setup
        echo "DB_PATH=$params.db_path" >> .db

        cat \$original_path/$batch

        python3 batch-process.py --guids_to_clear \$original_path/$batch

        #Update the saves tarball
        curl -X PUT --data-binary "@\$original_path/$all" $params.bucket/$params.species/all.tar.gz

        touch \$original_path/cleaned_up
        """
}

//Remove the batch's save files separately due to needing extra auth
//Using a bucket PAR like other operations doesn't work here as DELETE is not supported with PARs
//So this requires Nextflow secrets setup for OCI CLI auth (~/oci/config and ~/.oci/oci_api_key.pem) 
process remove_batch{
    secret 'OCI_CONFIG'
    secret 'OCI_KEY'
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

        echo \$OCI_CONFIG | base64 -d > ~/.oci/config
        echo \$OCI_KEY | base64 -d > ~/.oci/oci_api_key.pem
        export OCI_CLI_SUPPRESS_FILE_PERMISSIONS_WARNING=True

        #Use the PAR to pull out the namespace and the bucket
        #PAR should be of form https://<url>/p/<token>/n/<namespace>/b/<bucket name>/o
        namespace=\$(python3 -c "print('$params.bucket'.split('/')[6])")
        bucket=\$(python3 -c "print('$params.bucket'.split('/')[8])")

        #Build a string of `--include <path in bucket> --include <another path> --include ...` to enable a single `bulk_delete` call
        #This is siginificantly faster than calling `bulk_delete` on individual samples
        python3 -c "print(' --include', ' --include '.join(['$params.species/to_process/'+line.replace('\\n','')+'.tar.gz' for line in open('$batch')]))" > to_delete.txt     

        oci os object bulk-delete -ns \$namespace -bn \$bucket \$(cat to_delete.txt) --force 

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

        original_path=\$(pwd)

        cd /FN5
        #Make sure the DB is setup
        echo "DB_PATH=$params.db_path" >> .db

        python3 release-lock.py --lock \$(cat \$original_path/$lock)
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

        comparisons = process_batch(lock, all, to_process)

        done = add_to_db(to_process, comparisons, lock)

        cleaned_up = clean_up(lock, batch, all, done)
        batch_removed = remove_batch(lock, batch, done)

        release_lock(lock, cleaned_up, batch_removed)
}

workflow{
    main:
        find_neighbour_5()
}


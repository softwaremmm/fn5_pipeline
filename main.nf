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
    exit(1)
}

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

        #TODO: Upload this to bucket...
        tar --use-compress-program=pigz -cf \$(echo \$guid).tar.gz sample-out/*
        curl -X PUT --data-binary "@\$(pwd)/sample-out/\$(echo \$guid).tar.gz" $params.bucket/$params.species/to_process/\$(echo \$guid).tar.gz

        echo \$guid > \$original_path/guid
        cat \$original_path/guid
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
            echo Getting \$f
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
        echo "Started in \$original_path"
        cd /FN5
        mkdir -p batch
        cd batch

        to_process=\$(echo $to_process)
        
        for f in \$(echo \${to_process});
        do
            #TODO: Figure out why the filepath doesn't join here...
            echo \$f
            echo \$original_path
            echo -e \$original_path/\$f
            echo -e \$original_path /\$f
            echo -e \$original_path/ \$f
            echo -e "aa \${original_path}/\$f"
            tar --use-compress-program=pigz -xf \$original_path/\$f
            ls
        done

        exit 1

        """
}

//Add to DB

//Update bucket

//Release lock




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
        guid = reference_compress(params.sample)
        lock = check_lock(guid)

        //To stop Nextflow running this out of order, we need to use a dummy output fed into downstream processes
        check = wait_for_lock(lock)

        batch = get_batch(guid, lock, check)
        (all, to_process) = get_saves(lock, batch, check)

        process_batch(lock, all, to_process)

}


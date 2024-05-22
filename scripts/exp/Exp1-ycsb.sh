#!/bin/bash


. /etc/profile
export BACKUP_MODE="remote"
export SCHEME="depart" # horse or depart
export CLUSTER_NAME="1x"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

initConf
EXP_NAME="Exp1"
SCHEMES=("cassandraWS")
WORKLOADS=("workloada")
REQUEST_DISTRIBUTIONS=("zipfian" "uniform")
REPLICAS=(1)
THREAD_NUMBER=(32)
MEMTABLE_SIZE=(256)
SSTABLE_SIZE_IN_MB=16
rebuild="false"
OPERATION_NUMBER=10000000
KV_NUMBER=30000000
FIELD_LENGTH=1000
KEY_LENGTH=24
KEY_LENGTHMin=24
KEY_LENGTHMax=24
ROUND_NUMBER=5





function main {

    for rf in "${REPLICAS[@]}"; do
        # Load data
        load "CassandraWS" 64 "${SSTABLE_SIZE_IN_MB}" 2048 "${rf}" "workload_template" ${KV_NUMBER} ${rebuild} ${FIELD_LENGTH}
        # Wait for flush or compaction ready
        dataSizeOnEachNode=$(dataSizeEstimation ${KV_NUMBER} ${KEY_LENGTH} ${FIELD_LENGTH} ${rf})
        waitFlushCompactionTime=$(waitFlushCompactionTimeEstimation ${dataSizeOnEachNode})
        echo "Wait for flush and compaction of ${targetScheme}, waiting ${waitFlushCompactionTime} seconds"
        flush "LoadDB" "CassandraWS" 3600
        # Backup the DB and the logs
        backup "LoadDB" "CassandraWS" ${KV_NUMBER} ${KEY_LENGTH} ${FIELD_LENGTH} ${rf}

    done


    # Run experiments
    for scheme in "${SCHEMES[@]}"; do
        echo "Start experiment to ${scheme}"
        # write a for loop based on ROUND_NUMBER 
        for round in $(seq 1 $ROUND_NUMBER); do
            for dist in "${REQUEST_DISTRIBUTIONS[@]}"; do
                for workload in "${WORKLOADS[@]}"; do
                    for threadsNum in "${THREAD_NUMBER[@]}"; do
                        for memtableSize in "${MEMTABLE_SIZE[@]}"; do
                            for rf in "${REPLICAS[@]}"; do
                                echo "RunDB: Start round ${round}, the threads number is ${threadsNum}, sstable size is ${SSTABLE_SIZE_IN_MB}, memtable size is ${memtableSize}, rf is ${rf}, workload is ${workload}, request distribution is ${dist}"

                                # startup from preload dataset
                                startFromBackup "LoadDB" "CassandraWS" ${KV_NUMBER} ${KEY_LENGTH} ${FIELD_LENGTH} ${rf} ${memtableSize}
                                run "CassandraWS" ${dist} ${workload} ${threadsNum} ${KV_NUMBER} ${OPERATION_NUMBER} ${KEY_LENGTH} ${FIELD_LENGTH}
                                # Collect load results
                                resultsDir="/home/ymren/Results-${CLUSTER_NAME}/Run-threads_${threadsNum}-sstSize_${SSTABLE_SIZE_IN_MB}-memSize_${memtableSize}-rf_${rf}-workload_${workload}-dist_${dist}-round_${round}"
                                echo "Collect results to ${resultsDir}"
                                collectResults ${resultsDir}
                            done
                        done
                    done
                done
            done
        done
    done
}

main

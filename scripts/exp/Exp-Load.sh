#!/bin/bash


. /etc/profile
export BACKUP_MODE="remote"
export SCHEME="Horse" # Horse or depart
export CLUSTER_NAME="4x"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

initConf

SCHEMES=("mlsm") # mlsm, cassandra or depart
REPLICAS=(3)
SSTABLE_SIZE_IN_MB=16
KV_NUMBER=1000000
FIELD_LENGTH=1000
KEY_LENGTH=24
MODE="mlsm" # mlsm or cassandra
REBUILD="false"


function main {

    for scheme in "${SCHEMES[@]}"; do
        echo "Load data for ${scheme}"
        for rf in "${REPLICAS[@]}"; do
            # Load data
            load $scheme 64 "${SSTABLE_SIZE_IN_MB}" 2048 "${rf}" "workload_template" ${KV_NUMBER} ${REBUILD} ${FIELD_LENGTH} ${KEY_LENGTH} ${MODE}
            # Wait for flush or compaction ready
            dataSizeOnEachNode=$(dataSizeEstimation ${KV_NUMBER} ${KEY_LENGTH} ${FIELD_LENGTH} ${rf})
            waitFlushCompactionTime=$(waitFlushCompactionTimeEstimation ${dataSizeOnEachNode})
            echo "Wait for flush and compaction of ${targetScheme}, waiting ${waitFlushCompactionTime} seconds"
            flush "LoadDB" $scheme 600
            # Backup the DB and the logs
            backup "LoadDB" $scheme ${KV_NUMBER} ${KEY_LENGTH} ${FIELD_LENGTH} ${rf}

        done
        
    done
}

main

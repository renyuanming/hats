#!/bin/bash


. /etc/profile

SCHEMES=("mlsm" "depart-5.0")
REPLICAS=(3)
SSTABLE_SIZE_IN_MB=160
KV_NUMBER=100000000
FIELD_LENGTH=1000
KEY_LENGTH=24
REBUILD_SERVER="true"
WAIT_TIME=7200
COMPACTION_STRATEGY=("LCS")

JDK_VERSION="17"
LOG_LEVEL="error"
BRANCH="main"


function exportEnv {
    
    scheme=$1

    export BACKUP_MODE="local"
    export SCHEME=$scheme # horse or depart
    export CLUSTER_NAME="1x"
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
    source "${SCRIPT_DIR}/../common.sh"
    initConf
}


function main {

    for scheme in "${SCHEMES[@]}"; do
        echo "Load data for ${scheme}"
        exportEnv $scheme
        perpareJavaEnvironment "${scheme}" "${JDK_VERSION}"
        
        if [ "${REBUILD_SERVER}" == "true" ]; then
            echo "Rebuild the server"
            rebuildServer "${BRANCH}" "${scheme}"
        fi

        for rf in "${REPLICAS[@]}"; do
            for compaction_strategy in "${COMPACTION_STRATEGY[@]}"; do
                # Load data
                load $scheme 4 "${SSTABLE_SIZE_IN_MB}" 2048 "${rf}" "workload_template" ${KV_NUMBER} ${FIELD_LENGTH} ${KEY_LENGTH} ${compaction_strategy} ${LOG_LEVEL} ${BRANCH}
                # Wait for flush or compaction ready
                flush "LoadDB" $scheme $WAIT_TIME
                # Backup the DB and the logs
                backup "LoadDB" $scheme ${KV_NUMBER} ${KEY_LENGTH} ${FIELD_LENGTH} ${rf} "${SSTABLE_SIZE_IN_MB}" ${compaction_strategy}
            done
        done
    done
}

main

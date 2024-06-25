#!/bin/bash


. /etc/profile

# Workload Settings
EXP_NAME="Exp-PureRead"
WORKLOADS=("workloadc")
REQUEST_DISTRIBUTIONS=("zipfian") # zipfian uniform
OPERATION_NUMBER=20000000
KV_NUMBER=100000000
FIELD_LENGTH=1000
KEY_LENGTH=24
KEY_LENGTHMin=24
KEY_LENGTHMax=24
REPLICAS=(3)
THREAD_NUMBER=(32)
MEMTABLE_SIZE=(2048)
MOTIVATION=("false") # true is only forward the read request to the primary lsm-tree
MEMORY_LIMIT="12G"
USE_DIRECTIO=("false") # enable direct io for read path or not
PURPOSE="ReadAmplification" # To prove ReadAmplification, CompactionOverhead, we select different dataset
READ_SENSISTIVITY=0.9
BRANCH="main"
LOG_LEVEL="info"
SSTABLE_SIZE_IN_MB=16
COMPACTION_STRATEGY="LCS"

# Debug Settings
REBUILD_SERVER="true"
REBUILD_CLIENT="true"

# Server settings
ROUND_NUMBER=5
COMPACTION_LEVEL=("zero") # zero one all

# Horse
SCHEDULING_INITIAL_DELAY=3600 # seconds
SCHEDULING_INTERVAL=(60) # seconds
STATES_UPDATE_INTERVAL=10 # seconds
THROTLLE_DATA_RATE=(90) # MB/s

JDK_VERSION="17"

SCHEMES=("horse" "depart" "mlsm")


function exportEnv {
    
    scheme=$1
    
    export BACKUP_MODE="local"
    export SCHEME=$scheme # horse or depart
    export CLUSTER_NAME="1x"
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
    source "${SCRIPT_DIR}/../common.sh"
    initConf
}

for scheme in "${SCHEMES[@]}"; do
    exportEnv $scheme
    if [ "${EXP_NAME}" == "Exp-PureRead" ]; then
        loadDataset "${EXP_NAME}" "$scheme" "${KV_NUMBER}" "${KEY_LENGTH}" "${FIELD_LENGTH}" "3" "${SSTABLE_SIZE_IN_MB}" "${COMPACTION_STRATEGY}"
    fi
    runExp "${EXP_NAME}" "$scheme" WORKLOADS[@] REQUEST_DISTRIBUTIONS[@] REPLICAS[@] THREAD_NUMBER[@] MEMTABLE_SIZE[@] "${OPERATION_NUMBER}" "${KV_NUMBER}" "${FIELD_LENGTH}" "${KEY_LENGTH}" "${KEY_LENGTHMin}" "${KEY_LENGTHMax}" "${ROUND_NUMBER}" COMPACTION_LEVEL[@]  MOTIVATION[@] "${MEMORY_LIMIT}" USE_DIRECTIO[@] "${REBUILD_SERVER}" "${REBUILD_CLIENT}" "${LOG_LEVEL}" "${BRANCH}" "${PURPOSE}" "${SCHEDULING_INITIAL_DELAY}" SCHEDULING_INTERVAL[@] "${STATES_UPDATE_INTERVAL}" "${READ_SENSISTIVITY}" THROTLLE_DATA_RATE[@] "${JDK_VERSION}" "${SSTABLE_SIZE_IN_MB}" "${COMPACTION_STRATEGY}"
    cleanup $scheme
done
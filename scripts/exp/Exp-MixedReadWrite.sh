#!/bin/bash


. /etc/profile

# Workload Settings
EXP_NAME="Exp-MixedReadWrite"
WORKLOADS=("workloada")
REQUEST_DISTRIBUTIONS=("zipfian") # zipfian uniform
OPERATION_NUMBER=30000000
KV_NUMBER=30000000
FIELD_LENGTH=1000
KEY_LENGTH=24
KEY_LENGTHMin=24
KEY_LENGTHMax=24

# Debug Settings
REBUILD_SERVER="true"
REBUILD_CLIENT="true"
LOG_LEVEL="info"

# Server settings
REPLICAS=(3)
THREAD_NUMBER=(32)
MEMTABLE_SIZE=(2048)
SSTABLE_SIZE_IN_MB=16
ROUND_NUMBER=5
COMPACTION_LEVEL=("zero") # zero one all
ENABLE_AUTO_COMPACTION="false"
ENABLE_COMPACTION_CFS=""
MOTIVATION=("false") # true is only forward the read request to the primary lsm-tree
MEMORY_LIMIT="12G"
USE_DIRECTIO=("false") # enable direct io for read path or not
BRANCH="main"
PURPOSE="CompactionOverhead" # To prove ReadAmplification, CompactionOverhead, we select different dataset
STARTUP_FROM_BACKUP="true"
SETTING=""

# Horse
SCHEDULING_INITIAL_DELAY=300 # seconds
SCHEDULING_INTERVAL=(60 120 180) # seconds
STATES_UPDATE_INTERVAL=10 # seconds
READ_SENSISTIVITY=0.9
STEP_SIZE=(0.02 0.01)
OFFLOAD_THRESHOLD=(0.1 0.2 0.3)
RECOVER_THRESHOLD=(0.1)
ENABLE_HORSE="true"
SHUFFLE_REPLICAS=("false")



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
    runExp "${EXP_NAME}" "$scheme" WORKLOADS[@] REQUEST_DISTRIBUTIONS[@] REPLICAS[@] THREAD_NUMBER[@] MEMTABLE_SIZE[@] "${SSTABLE_SIZE_IN_MB}" "${OPERATION_NUMBER}" "${KV_NUMBER}" "${FIELD_LENGTH}" "${KEY_LENGTH}" "${KEY_LENGTHMin}" "${KEY_LENGTHMax}" "${ROUND_NUMBER}" COMPACTION_LEVEL[@] "${ENABLE_AUTO_COMPACTION}" "${ENABLE_COMPACTION_CFS}" MOTIVATION[@] "${MEMORY_LIMIT}" USE_DIRECTIO[@] "${REBUILD_SERVER}" "${REBUILD_CLIENT}" "${LOG_LEVEL}" "${BRANCH}" "${PURPOSE}" "${STARTUP_FROM_BACKUP}" "${SETTING}" "${SCHEDULING_INITIAL_DELAY}" SCHEDULING_INTERVAL[@] "${STATES_UPDATE_INTERVAL}" "${READ_SENSISTIVITY}" STEP_SIZE[@] OFFLOAD_THRESHOLD[@] RECOVER_THRESHOLD[@] "${ENABLE_HORSE}" SHUFFLE_REPLICAS[@]
done






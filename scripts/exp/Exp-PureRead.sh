#!/bin/bash


. /etc/profile
export BACKUP_MODE="remote"
export SCHEME="horse" # horse or depart, used for initializing the configuration file
export CLUSTER_NAME="4x"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${SCRIPT_DIR}/../common.sh"

initConf
EXP_NAME="Exp-PureRead"
SCHEMES=("mlsm")
WORKLOADS=("workloadc")
REQUEST_DISTRIBUTIONS=("zipfian") # zipfian uniform
REPLICAS=(3)
THREAD_NUMBER=(32)
MEMTABLE_SIZE=(2048)
SSTABLE_SIZE_IN_MB=16
OPERATION_NUMBER=30000000
KV_NUMBER=30000000
FIELD_LENGTH=1000
KEY_LENGTH=24
KEY_LENGTHMin=24
KEY_LENGTHMax=24
ROUND_NUMBER=5
MODE="mlsm" # mlsm or cassandra
COMPACTION_LEVEL=("zero") # zero one all
ENABLE_AUTO_COMPACTION="false"
ENABLE_COMPACTION_CFS=""
MOTIVATION=("false") # true is only forward the read request to the primary lsm-tree
MEMORY_LIMIT="12G"
USE_DIRECTIO=("false") # enable direct io for read path or not
REBUILD_SERVER="true"
REBUILD_CLIENT="false"
LOG_LEVEL="info"
BRANCH="main"
PURPOSE="ReadAmplification" # To prove ReadAmplification, CompactionOverhead, we select different dataset
STARTUP_FROM_BACKUP="true"
SETTING=""
SCHEDULING_INITIAL_DELAY=300 # seconds
SCHEDULING_INTERVAL=(60 120 180) # seconds
STATES_UPDATE_INTERVAL=10 # seconds
READ_SENSISTIVITY=0.9
STEP_SIZE=(0.02 0.01)
OFFLOAD_THRESHOLD=(0.1 0.2 0.3)
RECOVER_THRESHOLD=(0.1)
ENABLE_HORSE="true"
SHUFFLE_REPLICAS=("false")

function main {

    if [ "${REBUILD_SERVER}" == "true" ]; then
        echo "Rebuild the server"
        rebuildServer "${BRANCH}"
    fi

    if [ "${REBUILD_CLIENT}" == "true" ]; then
        echo "Rebuild the client"
        rebuildClient
    fi

    # Run experiments
    for scheme in "${SCHEMES[@]}"; do
        echo "Start experiment to ${scheme}"
        for compactionLevel in "${COMPACTION_LEVEL[@]}"; do
            for rf in "${REPLICAS[@]}"; do
                # Copy the data set to each node and startup the process from th dataset
                # if [ "$BACKUP_MODE" != "local" ]; then
                #     copyDatasetToNodes "${EXP_NAME}" "${scheme}" "${KV_NUMBER}" "${KEY_LENGTH}" "${FIELD_LENGTH}" "${rf}" "${compactionLevel}" "${PURPOSE}"
                #     sleep 600
                # fi
                for round in $(seq 1 $ROUND_NUMBER); do
                    for dist in "${REQUEST_DISTRIBUTIONS[@]}"; do
                        for workload in "${WORKLOADS[@]}"; do
                            for threadsNum in "${THREAD_NUMBER[@]}"; do
                                for memtableSize in "${MEMTABLE_SIZE[@]}"; do
                                    for directIO in "${USE_DIRECTIO[@]}"; do
                                        for motivation in "${MOTIVATION[@]}"; do
                                            for schedulingInterval in "${SCHEDULING_INTERVAL[@]}"; do
                                                for stepSize in "${STEP_SIZE[@]}"; do
                                                    for offloadThreshold in "${OFFLOAD_THRESHOLD[@]}"; do
                                                        for recoverThreshold in "${RECOVER_THRESHOLD[@]}"; do
                                                            for shuffleReplicas in "${SHUFFLE_REPLICAS[@]}"; do

                                                                echo "RunDB: Start round ${round}, the threads number is ${threadsNum}, sstable size is ${SSTABLE_SIZE_IN_MB}, memtable size is ${memtableSize}, rf is ${rf}, workload is ${workload}, request distribution is ${dist}"
                                                                
                                                                if [ "${compactionLevel}" == "zero" ]; then
                                                                    ENABLE_AUTO_COMPACTION="false"
                                                                    ENABLE_COMPACTION_CFS=""
                                                                elif [ "${compactionLevel}" == "one" ]; then
                                                                    ENABLE_AUTO_COMPACTION="true"
                                                                    ENABLE_COMPACTION_CFS="usertable0"
                                                                elif ["${compactionLevel}" == "all" ]; then
                                                                    ENABLE_AUTO_COMPACTION="true"
                                                                    ENABLE_COMPACTION_CFS="usertable0 usertable1 usertable2"
                                                                fi

                                                                SETTING=$(getSettingName ${motivation} ${compactionLevel})

                                                                # startup from preload dataset
                                                                if [ "${STARTUP_FROM_BACKUP}" == "true" ]; then
                                                                    echo "Start from backup"
                                                                    startFromBackup "LoadDB" $scheme ${KV_NUMBER} ${KEY_LENGTH} ${FIELD_LENGTH} ${rf} ${memtableSize} ${motivation} ${STARTUP_FROM_BACKUP} ${REBUILD_SERVER} "${directIO}" "${LOG_LEVEL}" "${BRANCH}" "${SCHEDULING_INITIAL_DELAY}" "${schedulingInterval}" "${STATES_UPDATE_INTERVAL}" "${READ_SENSISTIVITY}"
                                                                else
                                                                    echo "Start from current data"
                                                                    restartCassandra ${memtableSize} ${motivation} ${REBUILD_SERVER} "${directIO}" "${LOG_LEVEL}" "${BRANCH}" "${SCHEDULING_INITIAL_DELAY}" "${schedulingInterval}" "${STATES_UPDATE_INTERVAL}" "${READ_SENSISTIVITY}"
                                                                fi
                                                                run ${scheme} ${dist} ${workload} ${threadsNum} ${KV_NUMBER} ${OPERATION_NUMBER} ${KEY_LENGTH} ${FIELD_LENGTH} ${ENABLE_AUTO_COMPACTION} ${MODE} "${ENABLE_COMPACTION_CFS}" "${MEMORY_LIMIT}" "${LOG_LEVEL}" "${stepSize}" "${offloadThreshold}" "${recoverThreshold}" "${ENABLE_HORSE}" "${shuffleReplicas}"


                                                                # Collect load results
                                                                resultsDir=$(getResultsDir ${CLUSTER_NAME} ${scheme} ${EXP_NAME} ${SETTING} ${workload} ${dist} ${compactionLevel} ${threadsNum} ${schedulingInterval} ${stepSize} ${offloadThreshold} ${recoverThreshold} ${round} ${ENABLE_HORSE} ${shuffleReplicas})

                                                                # echo "Collect results to ${resultsDir}"
                                                                collectResults ${resultsDir}

                                                            done
                                                        done
                                                    done
                                                done
                                            done
                                        done
                                    done
                                done
                            done
                        done
                    done
                done
            done
        done
    done
}


main






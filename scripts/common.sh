#!/bin/bash
. /etc/profile
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source ${SCRIPT_DIR}/settings.sh

playbookSet=(playbook-load.yaml playbook-run.yaml playbook-flush.yaml playbook-backup.yaml playbook-startup.yaml playbook-fail.yaml playbook-recovery.yaml)
# Initialize an empty array to store tokens
tokens=()
TokenRanges=""
function getToken {
    nodeNum=$1
    # Get the output of the genToken.py script
    output=$(python3 ${SCRIPT_DIR}/genToken.py ${nodeNum} ${NumTokens})
    # Extract tokens from the output and add them to the array
    while read -r line; do
        if [[ $line == initial_token:* ]]; then
            # Extract the token value using parameter expansion
            token=${line#initial_token: }
            echo "Get token $token"
            # Add the token to the array
            tokens+=("$token")
        fi
    done <<< "$output"

    # Construct the token ranges
    for i in "${!tokens[@]}"; do
        TokenRanges+="${tokens[i]}"
        if [ $i -ne $((${#tokens[@]} - 1)) ]; then
            TokenRanges+=","
        fi
    done
}

function initConf {
    getToken ${#NodesIP[@]}
    # # Modify the the rpc_address listen_address seeds initial_token in the cassandra.yaml
    for ((i=0; i<${#NodesIP[@]}; i++)); do
        node_ip=${NodesIP[$i]}
        node=${Nodes[$i]}
        token=${tokens[$i]}
        echo "Set set the initial token of ${node} as ${token}"

        conf_dir="${SCRIPT_DIR}/conf/$SCHEME"
        

        if [[ $SCHEME == "depart" ]] || [[ $SCHEME == "cassandra" ]]; then
            sed -i "s/rpc_address:.*$/rpc_address: ${node_ip}/" ${conf_dir}/cassandra.yaml
            sed -i "s/listen_address:.*$/listen_address: ${node_ip}/" ${conf_dir}/cassandra.yaml
            sed -i "s/seeds:.*$/seeds: \"${SEEDS}\"/" ${conf_dir}/cassandra.yaml
            sed -i "s/initial_token:.*$/initial_token: ${token}/" ${conf_dir}/cassandra.yaml
            sed -i "s/num_tokens:.*$/num_tokens: ${NumTokens}/" ${conf_dir}/cassandra.yaml
        elif [[ $SCHEME == "horse" ]] || [[ $SCHEME == "mlsm" ]]; then
            sed -i "s/rpc_address:.*$/rpc_address: ${node_ip}/" ${conf_dir}/cassandra.yaml
            sed -i "s/listen_address:.*$/listen_address: ${node_ip}/" ${conf_dir}/cassandra.yaml
            sed -i "s/initial_token:.*$/initial_token: ${token}/" ${conf_dir}/cassandra.yaml
            sed -i "s/seeds:.*$/seeds: \"${SEEDS}\"/" ${conf_dir}/cassandra.yaml
            sed -i "s/token_ranges:.*$/token_ranges: ${TokenRanges}/" ${conf_dir}/cassandra.yaml
            sed -i "s/num_tokens:.*$/num_tokens: ${NumTokens}/" ${conf_dir}/cassandra.yaml
            sed -i "s/all_hosts:.*$/all_hosts: \"${Coordinators}\"/" ${conf_dir}/cassandra.yaml
        fi
        
        scp ${conf_dir}/cassandra.yaml ${node}:${PathToServer}/conf/cassandra.yaml
    done

    # modify the hosts.ini
    > ${SCRIPT_DIR}/playbook/hosts.ini
    echo "[cassandra_servers]" >> ${SCRIPT_DIR}/playbook/hosts.ini
    for node in "${Nodes[@]}"; do
        echo "${node} ansible_host=${node}" >> ${SCRIPT_DIR}/playbook/hosts.ini
    done
    echo "[cassandra_seeds]" >> ${SCRIPT_DIR}/playbook/hosts.ini
    for seed in "${Seeds[@]}"; do
        echo "${seed} ansible_host=${seed}" >> ${SCRIPT_DIR}/playbook/hosts.ini
    done
    echo "[cassandra_data_nodes]" >> ${SCRIPT_DIR}/playbook/hosts.ini
    for node in "${Nodes[@]}"; do
        if printf '%s\n' "${Seeds[@]}" | grep -q -P "^${node}$"; then
            continue
        fi
        echo "${node} ansible_host=${node}" >> ${SCRIPT_DIR}/playbook/hosts.ini
    done
    echo "[cassandra_client]" >> ${SCRIPT_DIR}/playbook/hosts.ini
    echo "${Client} ansible_host=${Client}" >> ${SCRIPT_DIR}/playbook/hosts.ini

}


function treeSizeEstimation {
    kvNumber=$1
    keylength=$2
    fieldlength=$3
    initial_count=${SSTableSize}
    ratio=${LSMTreeFanOutRatio}
    target_count=$((kvNumber * (keylength + fieldlength) / NodeNumber / 1024 / 1024 / 4))

    current_count=$initial_count
    current_level=1

    while [ $current_count -lt $target_count ]; do
        current_count=$((current_count * ratio))
        current_level=$((current_level + 1))
    done
    treeLevels=$((current_level))
    echo ${treeLevels}
}

function dataSizeEstimation {
    kvNumber=$1
    keylength=$2
    fieldlength=$3
    rf=$4
    dataSizeOnEachNode=$(echo "scale=2; $kvNumber * ($keylength + $fieldlength) / $NodeNumber / 1024 / 1024 / 1024 * $rf" | bc)
    echo ${dataSizeOnEachNode}
}


function waitFlushCompactionTimeEstimation {
    dataSizeOnEachNode=$1
    waitTime=$(echo "scale=2; $dataSizeOnEachNode * 500" | bc)
    waitTimeCeil=$(echo "scale=0; (${waitTime} + 0.5)/1" | bc)
    echo ${waitTimeCeil}

}

function resetPlaybook {
    task=$1
    if [ -f "playbook-${task}.yaml" ]; then
        rm -rf playbook-${task}.yaml
    fi
    cp ../playbook/playbook-${task}.yaml .
    cp ../playbook/hosts.ini .
}

function flush {
    expName=$1
    targetScheme=$2
    waitTime=$3
    echo "Start for flush and wait for compaction of ${targetScheme}, waiting ${waitTime} seconds"


    # Copy playbook
    resetPlaybook "flush"

    # Modify playbook
    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" playbook-flush.yaml
    sed -i "s|PATH_TO_SCRIPTS|${PathToScripts}|g" playbook-flush.yaml
    sed -i "s|PATH_TO_BACKUP|${PathToBackup}|g" playbook-flush.yaml
    sed -i "s/\(seconds: \)".*"/seconds: ${waitTime}/" playbook-flush.yaml
    if [ $targetScheme == "depart" ]; then
        sed -i 's|NODETOOL_OPTION|-h ::FFFF:127.0.0.1|g' playbook-flush.yaml
    else
        sed -i "s|NODETOOL_OPTION||g" playbook-flush.yaml
    fi

    ansible-playbook -v -i hosts.ini playbook-flush.yaml
}

function backup {
    expName=$1
    targetScheme=$2
    kvNumber=$3
    keylength=$4
    fieldlength=$5
    rf=$6

    echo "Start copy data of ${targetScheme} to backup, this will kill the online system!!!"


    # Copy playbook
    resetPlaybook "backup"
    # Modify playbook
    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" playbook-backup.yaml
    sed -i "s|PATH_TO_SCRIPTS|${PathToScripts}|g" playbook-backup.yaml
    sed -i "s|PATH_TO_BACKUP|${PathToBackup}|g" playbook-backup.yaml
    sed -i "s/Scheme/${targetScheme}/g" playbook-backup.yaml
    sed -i "s/DATAPATH/${expName}-kvNumber-${kvNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-RF-${rf}/g" playbook-backup.yaml
    ansible-playbook -v -i hosts.ini playbook-backup.yaml
}

function getSettingName() {
    local motivation=$1
    local compactionLevel=$2

    if [ "$compactionLevel" == "zero" -a "$motivation" == "false" ]; then
        echo "RunA"
    elif [ "$compactionLevel" == "zero" -a "$motivation" == "true" ]; then
        echo "RunB"
    elif [ "$compactionLevel" == "one" -a "$motivation" == "false" ]; then
        echo "RunC"
    elif [ "$compactionLevel" == "one" -a "$motivation" == "true" ]; then
        echo "RunD"
    elif [ "$compactionLevel" == "all" -a "$motivation" == "false" ]; then
        echo "RunE"
    elif [ "$compactionLevel" == "all" -a "$motivation" == "true" ]; then
        echo "RunF"
    fi
}


function copyDatasetToNodes {
    
    experiment=$1
    targetScheme=$2
    kvNumber=$3
    keylength=$4
    fieldlength=$5
    rf=$6
    compactionLevel=$7
    purpose=$8

    datasetName="LoadDB-kvNumber-${kvNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-RF-${rf}"
    targetDir="${PathToBackup}/${targetScheme}/${datasetName}"
    datasetDir=""

    if [ "${purpose}" == "CompactionOverhead" ]; then
        datasetDir="${datasetName}"
    elif [ "${purpose}" == "ReadAmplification" ]; then
        datasetDir="${datasetName}-CompactionLevel-${compactionLevel}"
    fi




    # copy playbook
    resetPlaybook "decompress"

    if [ "${experiment}" == "Exp-MixedReadWrite" ]; then
        for node in "${Nodes[@]}"; do
            dataset="${datasetDir}/${node}.tar.gz"
            echo "Copy the dataset ${dataset} to ${node}"
            ssh ${UserName}@${node} 'pkill -f CassandraDaemon'
            ssh ${UserName}@${node} "rm -rf ${targetDir}/data"
            ssh ${UserName}@${node} "rm -rf ${targetDir}/*.tar.gz"
            ssh ${UserName}@${node} "rm -rf ${PathToServer}/data"
            scp -r ${PathToBackup}/${targetScheme}/${dataset} ${UserName}@${node}:${targetDir}
        done
        sed -i "s|DECOMPRESS_DIR|${targetDir}|g" playbook-decompress.yaml
    elif [ "${experiment}" == "Exp-PureRead" ]; then
        for node in "${Nodes[@]}"; do
            dataset="${datasetDir}/${node}.tar.gz"
            echo "Copy the dataset ${dataset} to ${node}"
            ssh ${UserName}@${node} 'pkill -f CassandraDaemon'
            ssh ${UserName}@${node} "rm -rf ${targetDir}/data"
            ssh ${UserName}@${node} "rm -rf ${targetDir}/*.tar.gz"
            ssh ${UserName}@${node} "rm -rf ${PathToServer}/data"
            scp -r ${PathToBackup}/${targetScheme}/${dataset} ${UserName}@${node}:${targetDir}
        done
        sed -i "s|DECOMPRESS_DIR|${PathToServer}|g" playbook-decompress.yaml
    fi

    sleep 60


    # Modify playbook
    sed -i "s|TARGET_DIR|${targetDir}|g" playbook-decompress.yaml 

    ansible-playbook -v -i hosts.ini playbook-decompress.yaml
}

function rebuildServer {
    
    branch=$1
    scheme=$2
    echo "Building the server with branch ${branch}"
    resetPlaybook "rebuildServer"

    antOption="-Duse.jdk11=true"

    if [ "${scheme}" == "depart" ]; then
        antOption=""
    fi

    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" playbook-rebuildServer.yaml
    sed -i "s|BRANCH_NAME|${branch}|g" playbook-rebuildServer.yaml
    sed -i "s|ANT_OPTION|${antOption}|g" playbook-rebuildServer.yaml
    ansible-playbook -v -i hosts.ini playbook-rebuildServer.yaml
}

function rebuildClient {
    resetPlaybook "rebuildClient"
    sed -i "s|PATH_TO_CLIENT|${PathToClient}|g" playbook-rebuildClient.yaml
    ansible-playbook -v -i hosts.ini playbook-rebuildClient.yaml
}

function startFromBackup {
    expName=$1
    targetScheme=$2
    kvNumber=$3
    keylength=$4
    fieldlength=$5
    rf=$6
    memtable_heap_space=$7
    motivation=$8
    fromBackup=$9
    shift 9
    rebuild=$1
    useDirectIO=$2
    logLevel=$3
    branch=$4
    schedulingInitialDelay=$5
    schedulingInterval=$6
    statesUpdateInterval=$7
    readSensitivity=$8



    # Copy playbook
    resetPlaybook "startup"

    # We use the same dataset for horse and mlsm
    if [ "${targetScheme}" == "horse" ]; then
        targetScheme="mlsm"
    fi

    echo "Startup the system from backup, motivation is ${motivation}"
    # Modify playbook
    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" playbook-startup.yaml
    sed -i "s|PATH_TO_SCRIPTS|${PathToScripts}|g" playbook-startup.yaml
    sed -i "s|PATH_TO_BACKUP|${PathToBackup}|g" playbook-startup.yaml
    sed -i "s/Scheme/${targetScheme}/g" playbook-startup.yaml

    # if [ "$BACKUP_MODE" == "local" ]; then
    #     sed -i "s|DATAPATH|${expName}-kvNumber-${kvNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-RF-${rf}-CompactionLevel-${compactionLevel}|g" playbook-startup.yaml
    # elif [ "$BACKUP_MODE" == "remote" ]; then
    #     sed -i "s|DATAPATH|${expName}-kvNumber-${kvNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-RF-${rf}|g" playbook-startup.yaml
    # fi

    sed -i "s/DATAPATH/${expName}-kvNumber-${kvNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-RF-${rf}/g" playbook-startup.yaml
    sed -i "s/\(memtableSize: \)".*"/memtableSize: ${memtable_heap_space}MiB/" playbook-startup.yaml
    sed -i "s/\(motivation: \)".*"/motivation: \"${motivation}\"/" playbook-startup.yaml
    sed -i "s/\(fromBackup: \)".*"/fromBackup: \"${fromBackup}\"/" playbook-startup.yaml
    sed -i "s/\(rebuild: \)".*"/rebuild: \"${rebuild}\"/" playbook-startup.yaml
    sed -i "s/\(useDirectIO: \)".*"/useDirectIO: \"${useDirectIO}\"/" playbook-startup.yaml
    sed -i "s/\(branch: \)".*"/branch: \"${branch}\"/" playbook-startup.yaml
    sed -i "s|LOG_LEVEL|${logLevel}|g" playbook-startup.yaml
    sed -i "s|SUDO_PASSWD|${SudoPassword}|g" playbook-startup.yaml
    sed -i "s/\(schedulingInitialDelay: \)".*"/schedulingInitialDelay: ${schedulingInitialDelay}/" playbook-startup.yaml
    sed -i "s/\(schedulingInterval: \)".*"/schedulingInterval: ${schedulingInterval}/" playbook-startup.yaml
    sed -i "s/\(statesUpdateInterval: \)".*"/statesUpdateInterval: ${statesUpdateInterval}/" playbook-startup.yaml
    sed -i "s/\(readSensitivity: \)".*"/readSensitivity: ${readSensitivity}/" playbook-startup.yaml

    
    ansible-playbook -v -i hosts.ini playbook-startup.yaml
}

function restartCassandra {

    memtable_heap_space=$1
    motivation=$2
    rebuild=$3
    useDirectIO=$4
    logLevel=$5
    branch=$6
    schedulingInitialDelay=$7
    schedulingInterval=$8
    statesUpdateInterval=$9
    shift 9
    readSensitivity=$1
    
    # Copy playbook
    resetPlaybook "restartCassandra"

    # Modify playbook
    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" playbook-restartCassandra.yaml
    sed -i "s|PATH_TO_SCRIPTS|${PathToScripts}|g" playbook-restartCassandra.yaml
    sed -i "s/\(memtableSize: \)".*"/memtableSize: ${memtable_heap_space}MiB/" playbook-restartCassandra.yaml
    sed -i "s/\(motivation: \)".*"/motivation: \"${motivation}\"/" playbook-restartCassandra.yaml
    sed -i "s/\(rebuild: \)".*"/rebuild: \"${rebuild}\"/" playbook-restartCassandra.yaml
    sed -i "s/\(useDirectIO: \)".*"/useDirectIO: \"${useDirectIO}\"/" playbook-restartCassandra.yaml
    sed -i "s/\(branch: \)".*"/branch: \"${branch}\"/" playbook-restartCassandra.yaml
    # sed -i "s|LOG_LEVEL|${logLevel}|g" playbook-restartCassandra.yaml
    sed -i "s/\(schedulingInitialDelay: \)".*"/schedulingInitialDelay: ${schedulingInitialDelay}/" playbook-restartCassandra.yaml
    sed -i "s/\(schedulingInterval: \)".*"/schedulingInterval: ${schedulingInterval}/" playbook-restartCassandra.yaml
    sed -i "s/\(statesUpdateInterval: \)".*"/statesUpdateInterval: ${statesUpdateInterval}/" playbook-restartCassandra.yaml
    sed -i "s/\(readSensitivity: \)".*"/readSensitivity: ${readSensitivity}/" playbook-restartCassandra.yaml

    ansible-playbook -v -i hosts.ini playbook-restartCassandra.yaml
}

function collectResults {

    resultsDir=$1

    if [ -d ${resultsDir} ]; then
        rm -rf ${resultsDir}
    fi

    mkdir -p ${resultsDir}
    
    for node in "${Nodes[@]}"; do
        echo "Copy loading stats of ${targetScheme} back, ${node}"
        scp -r ${UserName}@${node}:/home/${UserName}/Results ${resultsDir}/${node}
        ssh ${UserName}@${node} "rm -rf /home/${UserName}/Results && mkdir -p /home/${UserName}/Results"
    done
    latest_file=$(ssh "$Client" "ls -t $ClientLogDir | head -n 1")
    echo "Copy the latest log file ${latest_file} from ${Client}"
    scp ${Client}:${ClientLogDir}${latest_file} ${resultsDir}/
}

function getResultsDir
{   
    CLUSTER_NAME=$1
    TARGET_SCHEME=$2
    EXP_NAME=$3
    SETTING=$4
    workload=$5
    dist=$6
    compactionLevel=$7
    threadsNum=$8
    schedulingInterval=$9
    shift 9
    stepSize=$1
    offloadThreshold=$2
    recoverThreshold=$3
    round=$4
    enableHorse=$5
    shuffleReplicas=$6

    resultsDir="/home/ymren/Results-${CLUSTER_NAME}/${TARGET_SCHEME}/${EXP_NAME}-${SETTING}-workload_${workload}-dist_${dist}-compactionLevel_${compactionLevel}-threads_${threadsNum}-enableHorse_${enableHorse}"

    if [ "${enableHorse}" == "true" ]; then
        resultsDir="${resultsDir}/schedulingInterval_${schedulingInterval}-stepSize_${stepSize}-offloadThreshold_${offloadThreshold}-recoverThreshold_${recoverThreshold}/round_${round}"
    else
        resultsDir="${resultsDir}/shuffleReplicas_${shuffleReplicas}/round_${round}"
    fi

    echo ${resultsDir}
}

function load {
    targetScheme=$1
    threads=$2
    sstableSize=$3
    memtable_heap_space=$4
    rf=$5
    workload=$6
    kvNumber=$7
    fieldlength=$8
    keyLength=$9
    


    echo "Start loading data into ${targetScheme}"

    # Copy playbook
    resetPlaybook "load"

    # Modify load playbook
    sed -i "s/\(expName: \)".*"/expName: "${ExpName}-${targetScheme}-Load"/" playbook-load.yaml
    sed -i "s/record_count:.*$/record_count: ${kvNumber}/" playbook-load.yaml
    sed -i "s/filed_length:.*$/filed_length: ${fieldlength}/" playbook-load.yaml
    sed -i "s/\(threads: \)".*"/threads: ${threads}/" playbook-load.yaml
    sed -i "s/\(sstable_size_in_mb: \)".*"/sstable_size_in_mb: ${sstableSize}/" playbook-load.yaml
    sed -i "s/\(replication_factor: \)".*"/replication_factor: ${rf}/" playbook-load.yaml
    sed -i "s/\(workload: \)".*"/workload: workloads\/${workload}/" playbook-load.yaml
    sed -i "s/\(mode: \)".*"/mode: ${targetScheme}/" playbook-load.yaml

    sed -i "s/memtable_heap_space=.*$/memtable_heap_space=\"${memtable_heap_space}MiB\" \&\&/" playbook-load.yaml
    sed -i "s/rebuild=.*$/rebuild=\"${rebuild}\" \&\&/" playbook-load.yaml
    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" playbook-load.yaml
    sed -i "s|PATH_TO_CLIENT|${PathToClient}|g" playbook-load.yaml
    sed -i "s|PATH_TO_SCRIPTS|${PathToScripts}|g" playbook-load.yaml
    sed -i "s|COORDINATORS|${Coordinators}|g" playbook-load.yaml
    sed -i "s|NODE_IP|${NodeIP}|g" playbook-load.yaml
    sed -i "s|PATH_TO_RESULT_DIR|${PathToResultDir}|g" playbook-load.yaml
    
    if [ $targetScheme == "depart" ]; then
        sed -i 's|NODETOOL_OPTION|-h ::FFFF:127.0.0.1|g' playbook-load.yaml
    else
        sed -i "s|NODETOOL_OPTION||g" playbook-load.yaml
    fi


    ansible-playbook -v -i hosts.ini playbook-load.yaml

    ## Collect load results
    resultsDir="/home/${UserName}/Results/Load-threads_${threads}-sstSize_${sstableSize}-memSize_${memtableSize}-rf_${rf}-workload_${workload}"
    collectResults ${resultsDir}
    
}

function run {

    targetScheme=$1
    dist=$2
    workload=$3
    threads=$4
    kvNumber=$5
    operations=$6
    keyLen=$7
    fieldLen=$8
    enableAutoCompaction=$9
    shift 9
    enableAutoCompactionCFs="$1"
    memoryLimit=$2
    logLevel=$3
    stepSize=$4
    offloadThreshold=$5
    recoverThreshold=$6
    enableHorse=$7
    shuffleReplicas=$8

    echo "Run ${targetScheme} with ${dist} ${workload} ${threads} ${kvNumber}, enableAutoCompaction is ${enableAutoCompaction}, mode is ${mode}, enableAutoCompactionCFs is ${enableAutoCompactionCFs}"

    resetPlaybook "run"



    # Modify run playbook
    sed -i "s|KEYSPACE|ycsb|g" playbook-run.yaml
    sed -i "s/\(recordNumber: \)".*"/recordNumber: ${kvNumber}/" playbook-run.yaml
    sed -i "s/\(operationNumber: \)".*"/operationNumber: ${operations}/" playbook-run.yaml
    sed -i "s/\(keyLength: \)".*"/keyLength: ${keyLen}/" playbook-run.yaml
    sed -i "s/\(fieldLength: \)".*"/fieldLength: ${fieldLen}/" playbook-run.yaml
    sed -i "s/\(threads: \)".*"/threads: ${threads}/" playbook-run.yaml
    sed -i "s/workload:.*$/workload: workloads\/${workload}/" playbook-run.yaml
    sed -i "s/requestDistribution:.*$/requestDistribution: ${dist}/" playbook-run.yaml
    sed -i "s/\(scheme: \)".*"/scheme: ${targetScheme}/" playbook-run.yaml
    sed -i "s|ENABLE_AUTO_COMPACTION|${enableAutoCompaction}|g" playbook-run.yaml
    sed -i "s/ENABL_COMPACTION_CFS/${enableAutoCompactionCFs}/g" playbook-run.yaml
    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" playbook-run.yaml
    sed -i "s|PATH_TO_CLIENT|${PathToClient}|g" playbook-run.yaml
    sed -i "s|PATH_TO_SCRIPTS|${PathToScripts}|g" playbook-run.yaml
    sed -i "s|COORDINATORS|${Coordinators}|g" playbook-run.yaml
    sed -i "s|PATH_TO_RESULT_DIR|${PathToResultDir}|g" playbook-run.yaml
    sed -i "s|DISK_DEVICE|${DiskDevice}|g" playbook-run.yaml
    sed -i "s|NETWORK_DEVICE|${NetworkInterface}|g" playbook-run.yaml
    sed -i "s|SUDO_PASSWD|${SudoPassword}|g" playbook-run.yaml
    sed -i "s|MEMORY_LIMIT|${memoryLimit}|g" playbook-run.yaml
    sed -i "s|LOG_LEVEL|${logLevel}|g" playbook-run.yaml
    sed -i "s|STEP_SIZE|${stepSize}|g" playbook-run.yaml
    sed -i "s|OFFLOAD_THRESHOLD|${offloadThreshold}|g" playbook-run.yaml
    sed -i "s|RECOVER_THRESHOLD|${recoverThreshold}|g" playbook-run.yaml
    sed -i "s|ENABLE_HORSE|${enableHorse}|g" playbook-run.yaml
    sed -i "s|SHUFFLE_REPLICAS|${shuffleReplicas}|g" playbook-run.yaml

    if [ $targetScheme == "depart" ]; then
        sed -i 's|NODETOOL_OPTION|-h ::FFFF:127.0.0.1|g' playbook-run.yaml
    else
        sed -i "s|NODETOOL_OPTION||g" playbook-run.yaml
    fi
    

    ansible-playbook -v -i hosts.ini playbook-run.yaml
}

function perpareJavaEnvironment {
    
    TARGET_SCHEME=$1

    resetPlaybook "prepareJavaEnv"

    javaVersion="/usr/lib/jvm/java-11-openjdk-amd64/bin/java"

    if [ "${TARGET_SCHEME}" == "depart" ]; then
        javaVersion="/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java"
    fi

    sed -i "s|SUDO_PASSWD|${SudoPassword}|g" playbook-prepareJavaEnv.yaml
    sed -i "s|JAVA_VERSION|${javaVersion}|g" playbook-prepareJavaEnv.yaml

    ansible-playbook -v -i hosts.ini playbook-prepareJavaEnv.yaml

}


function runExp {
    
    EXP_NAME=${1}
    TARGET_SCHEME=${2}
    WORKLOADS=("${!3}")
    REQUEST_DISTRIBUTIONS=("${!4}")
    REPLICAS=("${!5}")
    THREAD_NUMBER=("${!6}")
    MEMTABLE_SIZE=("${!7}")
    SSTABLE_SIZE_IN_MB=${8}
    OPERATION_NUMBER=${9}
    KV_NUMBER=${10}
    FIELD_LENGTH=${11}
    KEY_LENGTH=${12}
    KEY_LENGTHMin=${13}
    KEY_LENGTHMax=${14}
    ROUND_NUMBER=${15}
    COMPACTION_LEVEL=("${!16}")
    ENABLE_AUTO_COMPACTION=${17}
    ENABLE_COMPACTION_CFS=${18}
    MOTIVATION=("${!19}")
    MEMORY_LIMIT=${20}
    USE_DIRECTIO=("${!21}")
    REBUILD_SERVER=${22}
    REBUILD_CLIENT=${23}
    LOG_LEVEL=${24}
    BRANCH=${25}
    PURPOSE=${26}
    STARTUP_FROM_BACKUP=${27}
    SETTING=${28}
    SCHEDULING_INITIAL_DELAY=${29}
    SCHEDULING_INTERVAL=("${!30}")
    STATES_UPDATE_INTERVAL=${31}
    READ_SENSISTIVITY=${32}
    STEP_SIZE=("${!33}")
    OFFLOAD_THRESHOLD=("${!34}")
    RECOVER_THRESHOLD=("${!35}")
    ENABLE_HORSE=${36}
    SHUFFLE_REPLICAS=("${!37}")

    # test the parameters
    # echo "EXP_NAME: ${EXP_NAME}, TARGET_SCHEME: ${TARGET_SCHEME}, WORKLOADS: ${WORKLOADS[@]}, REQUEST_DISTRIBUTIONS: ${REQUEST_DISTRIBUTIONS[@]}, REPLICAS: ${REPLICAS[@]}, THREAD_NUMBER: ${THREAD_NUMBER[@]}, MEMTABLE_SIZE: ${MEMTABLE_SIZE[@]}, SSTABLE_SIZE_IN_MB: ${SSTABLE_SIZE_IN_MB}, OPERATION_NUMBER: ${OPERATION_NUMBER}, KV_NUMBER: ${KV_NUMBER}, FIELD_LENGTH: ${FIELD_LENGTH}, KEY_LENGTH: ${KEY_LENGTH}, KEY_LENGTHMin: ${KEY_LENGTHMin}, KEY_LENGTHMax: ${KEY_LENGTHMax}, ROUND_NUMBER: ${ROUND_NUMBER}, COMPACTION_LEVEL: ${COMPACTION_LEVEL[@]}, ENABLE_AUTO_COMPACTION: ${ENABLE_AUTO_COMPACTION}, ENABLE_COMPACTION_CFS: ${ENABLE_COMPACTION_CFS}, MOTIVATION: ${MOTIVATION[@]}, MEMORY_LIMIT: ${MEMORY_LIMIT}, USE_DIRECTIO: ${USE_DIRECTIO[@]}, REBUILD_SERVER: ${REBUILD_SERVER}, REBUILD_CLIENT: ${REBUILD_CLIENT}, LOG_LEVEL: ${LOG_LEVEL}, BRANCH: ${BRANCH}, PURPOSE: ${PURPOSE}, STARTUP_FROM_BACKUP: ${STARTUP_FROM_BACKUP}, SETTING: ${SETTING}, SCHEDULING_INITIAL_DELAY: ${SCHEDULING_INITIAL_DELAY}, SCHEDULING_INTERVAL: ${SCHEDULING_INTERVAL[@]}, STATES_UPDATE_INTERVAL: ${STATES_UPDATE_INTERVAL}, READ_SENSISTIVITY: ${READ_SENSISTIVITY}, STEP_SIZE: ${STEP_SIZE[@]}, OFFLOAD_THRESHOLD: ${OFFLOAD_THRESHOLD[@]}, RECOVER_THRESHOLD: ${RECOVER_THRESHOLD[@]}, ENABLE_HORSE: ${ENABLE_HORSE[@]}, SHUFFLE_REPLICAS: ${SHUFFLE_REPLICAS[@]}"

    if [[ "${TARGET_SCHEME}" == "horse" ]]; then
        ENABLE_HORSE="true"
    else
        ENABLE_HORSE="false"
    fi




    if [ "${REBUILD_SERVER}" == "true" ]; then
        echo "Rebuild the server"
        rebuildServer "${BRANCH}"
    fi

    if [ "${REBUILD_CLIENT}" == "true" ]; then
        echo "Rebuild the client"
        rebuildClient
    fi


    # Run experiments
    echo "Start experiment to ${TARGET_SCHEME}"
    perpareJavaEnvironment "${TARGET_SCHEME}"



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
                                                                startFromBackup "LoadDB" $TARGET_SCHEME ${KV_NUMBER} ${KEY_LENGTH} ${FIELD_LENGTH} ${rf} ${memtableSize} ${motivation} ${STARTUP_FROM_BACKUP} ${REBUILD_SERVER} "${directIO}" "${LOG_LEVEL}" "${BRANCH}" "${SCHEDULING_INITIAL_DELAY}" "${schedulingInterval}" "${STATES_UPDATE_INTERVAL}" "${READ_SENSISTIVITY}"
                                                            else
                                                                echo "Start from current data"
                                                                restartCassandra ${memtableSize} ${motivation} ${REBUILD_SERVER} "${directIO}" "${LOG_LEVEL}" "${BRANCH}" "${SCHEDULING_INITIAL_DELAY}" "${schedulingInterval}" "${STATES_UPDATE_INTERVAL}" "${READ_SENSISTIVITY}"
                                                            fi
                                                            run ${TARGET_SCHEME} ${dist} ${workload} ${threadsNum} ${KV_NUMBER} ${OPERATION_NUMBER} ${KEY_LENGTH} ${FIELD_LENGTH} ${ENABLE_AUTO_COMPACTION} "${ENABLE_COMPACTION_CFS}" "${MEMORY_LIMIT}" "${LOG_LEVEL}" "${stepSize}" "${offloadThreshold}" "${recoverThreshold}" "${ENABLE_HORSE}" "${shuffleReplicas}"


                                                            # Collect load results
                                                            resultsDir=$(getResultsDir ${CLUSTER_NAME} ${TARGET_SCHEME} ${EXP_NAME} ${SETTING} ${workload} ${dist} ${compactionLevel} ${threadsNum} ${schedulingInterval} ${stepSize} ${offloadThreshold} ${recoverThreshold} ${round} ${ENABLE_HORSE} ${shuffleReplicas})

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
}
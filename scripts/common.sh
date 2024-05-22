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
        

        if [[ $SCHEME == "depart" ]]; then
            sed -i "s/rpc_address:.*$/rpc_address: ${node_ip}/" ${conf_dir}/cassandra.yaml
            sed -i "s/listen_address:.*$/listen_address: ${node_ip}/" ${conf_dir}/cassandra.yaml
            sed -i "s/seeds:.*$/seeds: \"${SEEDS}\"/" ${conf_dir}/cassandra.yaml
        elif [[ $SCHEME == "Horse" ]]; then
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
    sed -i "s|PATH_TO_CODE_BASE|${PathToServer}|g" playbook-flush.yaml
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
    sed -i "s|PATH_TO_CODE_BASE|${PathToServer}|g" playbook-backup.yaml
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
    sed -i "s|PATH_TO_CODE_BASE|${PathToServer}|g" playbook-rebuildServer.yaml
    sed -i "s|BRANCH_NAME|${branch}|g" playbook-rebuildServer.yaml
}

function rebuildClient {
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

    echo "Startup the system from backup, motivation is ${motivation}"
    # Modify playbook
    sed -i "s|PATH_TO_CODE_BASE|${PathToServer}|g" playbook-startup.yaml
    sed -i "s|PATH_TO_SCRIPTS|${PathToScripts}|g" playbook-startup.yaml
    sed -i "s|PATH_TO_BACKUP|${PathToBackup}|g" playbook-startup.yaml
    sed -i "s/Scheme/${targetScheme}/g" playbook-startup.yaml

    if [ "$BACKUP_MODE" == "local" ]; then
        sed -i "s|DATAPATH|${expName}-kvNumber-${kvNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-RF-${rf}-CompactionLevel-${compactionLevel}|g" playbook-startup.yaml
    elif [ "$BACKUP_MODE" == "remote" ]; then
        sed -i "s|DATAPATH|${expName}-kvNumber-${kvNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-RF-${rf}|g" playbook-startup.yaml
    fi

    # sed -i "s/DATAPATH/${expName}-kvNumber-${kvNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-RF-${rf}/g" playbook-startup.yaml
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
    sed -i "s|PATH_TO_CODE_BASE|${PathToServer}|g" playbook-restartCassandra.yaml
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
    scp ${Client}:${ClientLogDir}${latest_file} ${resultsDir}/
}


function load {
    targetScheme=$1
    threads=$2
    sstableSize=$3
    memtable_heap_space=$4
    rf=$5
    workload=$6
    kvNumber=$7
    rebuild=$8
    fieldlength=$9
    shift 9 
    keyLength=$1
    mode=$2
    ExpName=$3
    


    echo "Start loading data into ${targetScheme}, the mode is ${mode}"

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
    sed -i "s/\(mode: \)".*"/mode: ${mode}/" playbook-load.yaml

    sed -i "s/memtable_heap_space=.*$/memtable_heap_space=\"${memtable_heap_space}MiB\" \&\&/" playbook-load.yaml
    sed -i "s/rebuild=.*$/rebuild=\"${rebuild}\" \&\&/" playbook-load.yaml
    sed -i "s|PATH_TO_CODE_BASE|${PathToServer}|g" playbook-load.yaml
    sed -i "s|PATH_TO_YCSB|${PathToClient}|g" playbook-load.yaml
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
    resultsDir="/home/${UserName}/Results/Load-threads_${threads}-sstSize_${sstableSize}-memSize_${memtableSize}-rf_${rf}--workload_${workload}"
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
    mode=$1
    enableAutoCompactionCFs="$2"
    memoryLimit=$3
    logLevel=$4
    stepSize=$5
    offloadThreshold=$6
    recoverThreshold=$7
    enableHorse=$8

    echo "Run ${targetScheme} with ${dist} ${workload} ${threads} ${kvNumber}, enableAutoCompaction is ${enableAutoCompaction}, mode is ${mode}, enableAutoCompactionCFs is ${enableAutoCompactionCFs}"

    resetPlaybook "run"

    if [[ "${mode}" == "mlsm"  || "${targetScheme}" == "depart" ]]; then
        sed -i "s|KEYSPACE|ycsb|g" playbook-run.yaml
    elif [ "${mode}" == "cassandra" ]; then
        sed -i "s|KEYSPACE|ycsbraw|g" playbook-run.yaml
    fi

    # Modify run playbook
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
    sed -i "s|PATH_TO_CODE_BASE|${PathToServer}|g" playbook-run.yaml
    sed -i "s|PATH_TO_YCSB|${PathToClient}|g" playbook-run.yaml
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

    if [ $targetScheme == "depart" ]; then
        sed -i 's|NODETOOL_OPTION|-h ::FFFF:127.0.0.1|g' playbook-run.yaml
    else
        sed -i "s|NODETOOL_OPTION||g" playbook-run.yaml
    fi
    

    ansible-playbook -v -i hosts.ini playbook-run.yaml
}
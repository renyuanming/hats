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

    realSeeds=$1

    if [[ $realSeeds == "true" ]]; then
        seeds=${SEEDS}
    else
        seeds=${ALL_SERVERS}
    fi

    getToken ${#ServersIP[@]}
    # # Modify the the rpc_address listen_address seeds initial_token in the cassandra.yaml
    for ((i=0; i<${#ServersIP[@]}; i++)); do
        node_ip=${ServersIP[$i]}
        node=${Servers[$i]}
        token=${tokens[$i]}
        echo "Set set the initial token of ${node} as ${token}"

        conf_dir="${SCRIPT_DIR}/conf/$SCHEME"
        

        if [[ $SCHEME == "depart" ]] || [[ $SCHEME == "cassandra-3.11.4" ]] || [[ $SCHEME == "cassandra-5.0" ]] || [[ $SCHEME == "depart-5.0" ]]; then
            sed -i "s/rpc_address:.*$/rpc_address: ${node_ip}/" ${conf_dir}/cassandra.yaml
            sed -i "s/listen_address:.*$/listen_address: ${node_ip}/" ${conf_dir}/cassandra.yaml
            sed -i "s/seeds:.*$/seeds: \"${seeds}\"/" ${conf_dir}/cassandra.yaml
            sed -i "s/initial_token:.*$/initial_token: ${token}/" ${conf_dir}/cassandra.yaml
            sed -i "s/num_tokens:.*$/num_tokens: ${NumTokens}/" ${conf_dir}/cassandra.yaml
        elif [[ $SCHEME == "horse" ]] || [[ $SCHEME == "mlsm" ]] || [[ $SCHEME == "c3" ]]; then
            sed -i "s/rpc_address:.*$/rpc_address: ${node_ip}/" ${conf_dir}/cassandra.yaml
            sed -i "s/listen_address:.*$/listen_address: ${node_ip}/" ${conf_dir}/cassandra.yaml
            sed -i "s/initial_token:.*$/initial_token: ${token}/" ${conf_dir}/cassandra.yaml
            sed -i "s/seeds:.*$/seeds: \"${seeds}\"/" ${conf_dir}/cassandra.yaml
            sed -i "s/token_ranges:.*$/token_ranges: ${TokenRanges}/" ${conf_dir}/cassandra.yaml
            sed -i "s/num_tokens:.*$/num_tokens: ${NumTokens}/" ${conf_dir}/cassandra.yaml
            sed -i "s/all_hosts:.*$/all_hosts: \"${Coordinators}\"/" ${conf_dir}/cassandra.yaml
        fi
        
        scp ${conf_dir}/cassandra.yaml ${node}:${PathToServer}/conf/cassandra.yaml
    done

    # modify the hosts.ini
    > ${SCRIPT_DIR}/playbook/hosts.ini
    echo "[cassandra_servers]" >> ${SCRIPT_DIR}/playbook/hosts.ini
    for node in "${Servers[@]}"; do
        echo "${node} ansible_host=${node}" >> ${SCRIPT_DIR}/playbook/hosts.ini
    done
    if [[ $realSeeds == "true" ]]; then
        echo "[cassandra_seeds]" >> ${SCRIPT_DIR}/playbook/hosts.ini
        for seed in "${Seeds[@]}"; do
            echo "${seed} ansible_host=${seed}" >> ${SCRIPT_DIR}/playbook/hosts.ini
        done
        echo "[cassandra_data_nodes]" >> ${SCRIPT_DIR}/playbook/hosts.ini
        for node in "${Servers[@]}"; do
            if printf '%s\n' "${Seeds[@]}" | grep -q -P "^${node}$"; then
                continue
            fi
            echo "${node} ansible_host=${node}" >> ${SCRIPT_DIR}/playbook/hosts.ini
        done
    else
        echo "[cassandra_seeds]" >> ${SCRIPT_DIR}/playbook/hosts.ini
        for node in "${Servers[@]}"; do
            echo "${node} ansible_host=${node}" >> ${SCRIPT_DIR}/playbook/hosts.ini
        done
    fi

    echo "[cassandra_clients]" >> ${SCRIPT_DIR}/playbook/hosts.ini
    for client in "${Clients[@]}"; do
        echo "${client} ansible_host=${client}" >> ${SCRIPT_DIR}/playbook/hosts.ini
    done

}


function treeSizeEstimation {
    kvNumber=$1
    keylength=$2
    fieldlength=$3
    initial_count=${SSTableSize}
    ratio=${LSMTreeFanOutRatio}
    target_count=$((kvNumber * (keylength + fieldlength) / ServerNumber / 1024 / 1024 / 4))

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
    dataSizeOnEachNode=$(echo "scale=2; $kvNumber * ($keylength + $fieldlength) / $ServerNumber / 1024 / 1024 / 1024 * $rf" | bc)
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
    playbook="playbook-flush.yaml"

    # Modify playbook
    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" ${playbook}
    sed -i "s|PATH_TO_SCRIPTS|${PathToScripts}|g" ${playbook}
    sed -i "s|PATH_TO_BACKUP|${PathToBackup}|g" ${playbook}
    sed -i "s/\(seconds: \)".*"/seconds: ${waitTime}/" ${playbook}
    if [ $targetScheme == "depart" ]|| [ $targetScheme == "cassandra-3.11.4" ]; then
        sed -i 's|NODETOOL_OPTION|-h ::FFFF:127.0.0.1|g' ${playbook}
    else
        sed -i "s|NODETOOL_OPTION||g" ${playbook}
    fi

    ansible-playbook -v -i hosts.ini ${playbook} -f ${ServerNumber}
}

function backup {
    expName=$1
    targetScheme=$2
    kvNumber=$3
    keylength=$4
    fieldlength=$5
    rf=$6
    sstableSize=$7
    compactionStrategy=$8

    echo "Start copy data of ${targetScheme} to backup, this will kill the online system!!!"


    # Copy playbook
    resetPlaybook "backup"
    playbook="playbook-backup.yaml"

    # Modify playbook
    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" ${playbook}
    sed -i "s|PATH_TO_SCRIPTS|${PathToScripts}|g" ${playbook}
    sed -i "s|PATH_TO_BACKUP|${PathToBackup}|g" ${playbook}
    sed -i "s/Scheme/${targetScheme}/g" ${playbook}
    sed -i "s/DATAPATH/${expName}-kvNumber-${kvNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-RF-${rf}-SSTableSize-${sstableSize}-CompactionStrategy-${compactionStrategy}/g" ${playbook}
    ansible-playbook -v -i hosts.ini ${playbook} -f ${ServerNumber}
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
        for node in "${Servers[@]}"; do
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
        for node in "${Servers[@]}"; do
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

function reloadSeeds {
    targetScheme=$1
    resetPlaybook "reloadSeeds"
    playbook="playbook-reloadSeeds.yaml"

    # antOption="-Duse.jdk11=true"

    # if [ "${targetScheme}" == "depart" ] || [ "$targetScheme" == "cassandra-3.11.4" ]; then
    antOption=""
    # fi

    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" ${playbook}
    ansible-playbook -v -i hosts.ini ${playbook} -f ${ServerNumber}
}

function rebuildServer {
    
    branch=$1
    targetScheme=$2
    echo "Building the server with branch ${branch}"
    resetPlaybook "rebuildServer"
    playbook="playbook-rebuildServer.yaml"

    # antOption="-Duse.jdk11=true"

    # if [ "${targetScheme}" == "depart" ] || [ "$targetScheme" == "cassandra-3.11.4" ]; then
    antOption=""
    # fi

    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" ${playbook}
    sed -i "s|BRANCH_NAME|${branch}|g" ${playbook}
    sed -i "s|ANT_OPTION|${antOption}|g" ${playbook}
    ansible-playbook -v -i hosts.ini ${playbook} -f ${ServerNumber}
}

function rebuildClient {
    resetPlaybook "rebuildClient"
    sed -i "s|PATH_TO_CLIENT|${PathToClient}|g" playbook-rebuildClient.yaml
    ansible-playbook -v -i hosts.ini playbook-rebuildClient.yaml
}

function loadDataset {

    expName=$1
    local targetScheme=$2
    kvNumber=$3
    keylength=$4
    fieldlength=$5
    rf=$6
    sstableSize=$7
    compactionStrategy=$8

    if [ "${targetScheme}" == "horse" ] || [ "${targetScheme}" == "c3" ]; then
        targetScheme="mlsm"
    fi
    # Copy playbook
    resetPlaybook "loadDataset"
    playbook="playbook-loadDataset.yaml"
    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" ${playbook}
    sed -i "s|PATH_TO_BACKUP|${PathToBackup}|g" ${playbook}
    sed -i "s/Scheme/${targetScheme}/g" ${playbook}
    sed -i "s/DATAPATH/LoadDB-kvNumber-${kvNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-RF-${rf}-SSTableSize-${sstableSize}-CompactionStrategy-${compactionStrategy}/g" ${playbook}

    ansible-playbook -v -i hosts.ini ${playbook} -f ${ServerNumber}
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
    shift 8
    rebuild=$1
    useDirectIO=$2
    logLevel=$3
    branch=$4
    schedulingInitialDelay=$5
    schedulingInterval=$6
    statesUpdateInterval=$7
    readSensitivity=$8
    enableHorse=$9
    shift 9
    throttleDataRate=$1
    sstableSize=$2
    compactionStrategy=$3



    # Copy playbook
    resetPlaybook "startup"
    playbook="playbook-startup.yaml"

    # We use the same dataset for horse and mlsm
    if [ "${targetScheme}" == "horse" ] || [ "${targetScheme}" == "c3" ]; then
        targetScheme="mlsm"
    fi

    echo "Startup the system from backup, motivation is ${motivation}"
    # Modify playbook
    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" ${playbook}
    sed -i "s|PATH_TO_SCRIPTS|${PathToScripts}|g" ${playbook}
    sed -i "s|PATH_TO_BACKUP|${PathToBackup}|g" ${playbook}
    sed -i "s/Scheme/${targetScheme}/g" ${playbook}

    # if [ "$BACKUP_MODE" == "local" ]; then
    #     sed -i "s|DATAPATH|${expName}-kvNumber-${kvNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-RF-${rf}-CompactionLevel-${compactionLevel}|g" ${playbook}
    # elif [ "$BACKUP_MODE" == "remote" ]; then
    #     sed -i "s|DATAPATH|${expName}-kvNumber-${kvNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-RF-${rf}|g" ${playbook}
    # fi

    sed -i "s/DATAPATH/${expName}-kvNumber-${kvNumber}-KeySize-${keylength}-ValueSize-${fieldlength}-RF-${rf}-SSTableSize-${sstableSize}-CompactionStrategy-${compactionStrategy}/g" ${playbook}
    sed -i "s/\(memtableSize: \)".*"/memtableSize: ${memtable_heap_space}MiB/" ${playbook}
    sed -i "s/\(motivation: \)".*"/motivation: \"${motivation}\"/" ${playbook}
    sed -i "s/\(rebuild: \)".*"/rebuild: \"${rebuild}\"/" ${playbook}
    sed -i "s/\(useDirectIO: \)".*"/useDirectIO: \"${useDirectIO}\"/" ${playbook}
    sed -i "s/\(branch: \)".*"/branch: \"${branch}\"/" ${playbook}
    sed -i "s|LOG_LEVEL|${logLevel}|g" ${playbook}
    sed -i "s|SUDO_PASSWD|${SudoPassword}|g" ${playbook}
    sed -i "s/\(schedulingInitialDelay: \)".*"/schedulingInitialDelay: ${schedulingInitialDelay}/" ${playbook}
    sed -i "s/\(schedulingInterval: \)".*"/schedulingInterval: ${schedulingInterval}/" ${playbook}
    sed -i "s/\(statesUpdateInterval: \)".*"/statesUpdateInterval: ${statesUpdateInterval}/" ${playbook}
    sed -i "s/\(readSensitivity: \)".*"/readSensitivity: ${readSensitivity}/" ${playbook}
    sed -i "s|ENABLE_HORSE|${enableHorse}|g" ${playbook}
    sed -i "s|THROTTLE_DATA_RATE|${throttleDataRate}|g" ${playbook}

    
    ansible-playbook -v -i hosts.ini ${playbook} -f ${ServerNumber}
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
    enableHorse=$2
    throttleDataRate=$3
    
    # Copy playbook
    resetPlaybook "restartCassandra"
    playbook="playbook-restartCassandra.yaml"

    echo "Restart the server with enableHorse ${enableHorse}, the throttle data rate is ${throttleDataRate} MB/s"

    # Modify playbook
    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" ${playbook}
    sed -i "s|PATH_TO_SCRIPTS|${PathToScripts}|g" ${playbook}
    sed -i "s/\(memtableSize: \)".*"/memtableSize: ${memtable_heap_space}MiB/" ${playbook}
    sed -i "s/\(motivation: \)".*"/motivation: \"${motivation}\"/" ${playbook}
    sed -i "s/\(rebuild: \)".*"/rebuild: \"${rebuild}\"/" ${playbook}
    sed -i "s/\(useDirectIO: \)".*"/useDirectIO: \"${useDirectIO}\"/" ${playbook}
    sed -i "s/\(branch: \)".*"/branch: \"${branch}\"/" ${playbook}
    # sed -i "s|LOG_LEVEL|${logLevel}|g" ${playbook}
    sed -i "s/\(schedulingInitialDelay: \)".*"/schedulingInitialDelay: ${schedulingInitialDelay}/" ${playbook}
    sed -i "s/\(schedulingInterval: \)".*"/schedulingInterval: ${schedulingInterval}/" ${playbook}
    sed -i "s/\(statesUpdateInterval: \)".*"/statesUpdateInterval: ${statesUpdateInterval}/" ${playbook}
    sed -i "s/\(readSensitivity: \)".*"/readSensitivity: ${readSensitivity}/" ${playbook}
    sed -i "s|ENABLE_HORSE|${enableHorse}|g" ${playbook}
    sed -i "s|THROTTLE_DATA_RATE|${throttleDataRate}|g" ${playbook}

    ansible-playbook -v -i hosts.ini ${playbook} -f ${ServerNumber}
}

function collectResults {

    resultsDir=$1

    if [ -d ${resultsDir} ]; then
        rm -rf ${resultsDir}
    fi

    mkdir -p ${resultsDir}
    
    for server in "${Servers[@]}"; do
        echo "Copy loading stats of ${targetScheme} back, ${server}"
        scp -r ${UserName}@${server}:/home/${UserName}/Results ${resultsDir}/${server}
        ssh ${UserName}@${server} "rm -rf /home/${UserName}/Results && mkdir -p /home/${UserName}/Results"
    done

    for client in "${Clients[@]}"; do
        latest_file=$(ssh "$client" "ls -t $ClientLogDir | head -n 1")
        echo "Copy the latest log file ${latest_file} from ${client}"
        scp ${client}:${ClientLogDir}${latest_file} ${resultsDir}/
    done
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
    round=$1
    enableHorse=$2
    throttleDataRate=$3
    operationNumber=$4
    kvNumber=$5
    sstableSize=$6
    compactionStrategy=$7
    consistencyLevel=$8

    resultsDir="/home/ymren/Results-${CLUSTER_NAME}/${TARGET_SCHEME}/${EXP_NAME}-workload_${workload}-dist_${dist}-compactionLevel_${compactionLevel}-threads_${threadsNum}-schedulingInterval-${schedulingInterval}-throttleDataRate_${throttleDataRate}-kvNumber-${kvNumber}-operationNumber_${operationNumber}-sstableSize_${sstableSize}-compactionStrategy-${compactionStrategy}-CL-${consistencyLevel}/round_${round}"

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
    shift 9
    compaction_strategy=$1
    logLevel=$2
    branch=$3
    


    echo "Start loading data into ${targetScheme}"

    # Copy playbook
    resetPlaybook "load"
    playbook="playbook-load.yaml"

    # Modify load playbook
    sed -i "s/\(expName: \)".*"/expName: "${ExpName}-${targetScheme}-Load"/" ${playbook}
    sed -i "s/record_count:.*$/record_count: ${kvNumber}/" ${playbook}
    sed -i "s/filed_length:.*$/filed_length: ${fieldlength}/" ${playbook}
    sed -i "s/\(threads: \)".*"/threads: ${threads}/" ${playbook}
    sed -i "s/\(sstable_size_in_mb: \)".*"/sstable_size_in_mb: ${sstableSize}/" ${playbook}
    sed -i "s/\(replication_factor: \)".*"/replication_factor: ${rf}/" ${playbook}
    sed -i "s/\(workload: \)".*"/workload: workloads\/${workload}/" ${playbook}
    sed -i "s/\(mode: \)".*"/mode: ${targetScheme}/" ${playbook}
    sed -i "s/\(compaction_strategy: \)".*"/compaction_strategy: ${compaction_strategy}/" ${playbook}

    sed -i "s/memtable_heap_space=.*$/memtable_heap_space=\"${memtable_heap_space}MiB\" \&\&/" ${playbook}
    sed -i "s/rebuild=.*$/rebuild=\"${rebuild}\" \&\&/" ${playbook}
    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" ${playbook}
    sed -i "s|PATH_TO_CLIENT|${PathToClient}|g" ${playbook}
    sed -i "s|PATH_TO_SCRIPTS|${PathToScripts}|g" ${playbook}
    sed -i "s|COORDINATORS|${Coordinators}|g" ${playbook}
    sed -i "s|NODE_IP|${NodeIP}|g" ${playbook}
    sed -i "s|PATH_TO_RESULT_DIR|${PathToResultDir}|g" ${playbook}
    sed -i "s|LOG_LEVEL|${logLevel}|g" ${playbook}
    sed -i "s|BRANCH|${branch}|g" ${playbook}
    
    if [ $targetScheme == "depart" ] || [ $targetScheme == "cassandra-3.11.4" ]; then
        sed -i 's|NODETOOL_OPTION|-h ::FFFF:127.0.0.1|g' ${playbook}
    else
        sed -i "s|NODETOOL_OPTION||g" ${playbook}
    fi


    ansible-playbook -v -i hosts.ini ${playbook} -f ${ServerNumber}

    ## Collect load results
    resultsDir="/home/${UserName}/Results/Load-threads_${threads}-sstSize_${sstableSize}-memSize_${memtableSize}-rf_${rf}-workload_${workload}-compactionStrategy_${compaction_strategy}"
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
    enableHorse=$4
    consistencyLevel=$5

    echo "Run ${targetScheme} with ${dist} ${workload} ${threads} ${kvNumber}, enableAutoCompaction is ${enableAutoCompaction}, mode is ${mode}, enableAutoCompactionCFs is ${enableAutoCompactionCFs}"

    resetPlaybook "run"
    playbook="playbook-run.yaml"



    # Modify run playbook
    sed -i "s|KEYSPACE|ycsb|g" ${playbook}
    sed -i "s/\(recordNumber: \)".*"/recordNumber: ${kvNumber}/" ${playbook}
    sed -i "s/\(operationNumber: \)".*"/operationNumber: ${operations}/" ${playbook}
    sed -i "s/\(keyLength: \)".*"/keyLength: ${keyLen}/" ${playbook}
    sed -i "s/\(fieldLength: \)".*"/fieldLength: ${fieldLen}/" ${playbook}
    sed -i "s/\(threads: \)".*"/threads: ${threads}/" ${playbook}
    sed -i "s/workload:.*$/workload: workloads\/${workload}/" ${playbook}
    sed -i "s/requestDistribution:.*$/requestDistribution: ${dist}/" ${playbook}
    sed -i "s/\(scheme: \)".*"/scheme: ${targetScheme}/" ${playbook}
    sed -i "s|ENABLE_AUTO_COMPACTION|${enableAutoCompaction}|g" ${playbook}
    sed -i "s/ENABL_COMPACTION_CFS/${enableAutoCompactionCFs}/g" ${playbook}
    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" ${playbook}
    sed -i "s|PATH_TO_CLIENT|${PathToClient}|g" ${playbook}
    sed -i "s|PATH_TO_SCRIPTS|${PathToScripts}|g" ${playbook}
    sed -i "s|COORDINATORS|${Coordinators}|g" ${playbook}
    sed -i "s|PATH_TO_RESULT_DIR|${PathToResultDir}|g" ${playbook}
    sed -i "s|DISK_DEVICE|${DiskDevice}|g" ${playbook}
    sed -i "s|NETWORK_DEVICE|${NetworkInterface}|g" ${playbook}
    sed -i "s|SUDO_PASSWD|${SudoPassword}|g" ${playbook}
    sed -i "s|MEMORY_LIMIT|${memoryLimit}|g" ${playbook}
    sed -i "s|LOG_LEVEL|${logLevel}|g" ${playbook}
    sed -i "s|ENABLE_HORSE|${enableHorse}|g" ${playbook}
    sed -i "s|PATH_TO_LOG_DIR|${PathToLogDir}|g" ${playbook}
    sed -i "s|CONSISTENCY|${consistencyLevel}|g" ${playbook}

    if [ $targetScheme == "depart" ]|| [ $targetScheme == "cassandra-3.11.4" ]; then
        sed -i 's|NODETOOL_OPTION|-h ::FFFF:127.0.0.1|g' ${playbook}
    else
        sed -i "s|NODETOOL_OPTION||g" ${playbook}
    fi
    

    ansible-playbook -v -i hosts.ini ${playbook} -f ${ServerNumber}
}

function perpareJavaEnvironment {
    
    TARGET_SCHEME=$1
    JDK_VERSION=$2

    resetPlaybook "prepareJavaEnv"

    javaVersion="/usr/lib/jvm/java-11-openjdk-amd64/bin/java"
    if [ "${JDK_VERSION}" == "8" ]; then
        javaVersion="/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java"
    elif [ "${JDK_VERSION}" == "11" ]; then
        javaVersion="/usr/lib/jvm/java-11-openjdk-amd64/bin/java"
    elif [ "${JDK_VERSION}" == "17" ]; then
        javaVersion="/usr/lib/jvm/java-17-openjdk-amd64/bin/java"
    fi

    # if [ "${TARGET_SCHEME}" == "depart" ] || [ "${TARGET_SCHEME}" == "cassandra-3.11.4" ]; then
    #     javaVersion="/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java"
    # fi

    sed -i "s|SUDO_PASSWD|${SudoPassword}|g" playbook-prepareJavaEnv.yaml
    sed -i "s|JAVA_VERSION|${javaVersion}|g" playbook-prepareJavaEnv.yaml

    ansible-playbook -v -i hosts.ini playbook-prepareJavaEnv.yaml

}

function cleanup {
    scheme=$1

    resetPlaybook "cleanup"
    playbook="playbook-cleanup.yaml"

    sed -i "s|PATH_TO_SERVER|${PathToServer}|g" ${playbook}

    ansible-playbook -v -i hosts.ini ${playbook} -f ${ServerNumber}

}


function runExp {
    
    EXP_NAME=${1}
    TARGET_SCHEME=${2}
    Workloads=("${!3}")
    REQUEST_DISTRIBUTIONS=("${!4}")
    REPLICAS=("${!5}")
    THREAD_NUMBER=("${!6}")
    MEMTABLE_SIZE=("${!7}")
    OPERATION_NUMBER=${8}
    KV_NUMBER=${9}
    FIELD_LENGTH=${10}
    KEY_LENGTH=${11}
    KEY_LENGTHMin=${12}
    KEY_LENGTHMax=${13}
    ROUND_NUMBER=${14}
    COMPACTION_LEVEL=("${!15}")
    MOTIVATION=("${!16}")
    MEMORY_LIMIT=${17}
    USE_DIRECTIO=("${!18}")
    REBUILD_SERVER=${19}
    REBUILD_CLIENT=${20}
    LOG_LEVEL=${21}
    BRANCH=${22}
    PURPOSE=${23}
    SCHEDULING_INITIAL_DELAY=${24}
    SCHEDULING_INTERVAL=("${!25}")
    STATES_UPDATE_INTERVAL=${26}
    READ_SENSISTIVITY=${27}
    THROTLLE_DATA_RATE=("${!28}")
    JDK_VERSION=${29}
    SSTABLE_SIZE_IN_MB=${30}
    compaction_strategy=${31}
    CONSISTENCY_LEVEL=("${!32}")

    # test the parameters
    # echo "EXP_NAME: ${EXP_NAME}, TARGET_SCHEME: ${TARGET_SCHEME}, Workloads: ${Workloads[@]}, REQUEST_DISTRIBUTIONS: ${REQUEST_DISTRIBUTIONS[@]}, REPLICAS: ${REPLICAS[@]}, THREAD_NUMBER: ${THREAD_NUMBER[@]}, MEMTABLE_SIZE: ${MEMTABLE_SIZE[@]}, SSTABLE_SIZE_IN_MB: ${SSTABLE_SIZE_IN_MB}, OPERATION_NUMBER: ${OPERATION_NUMBER}, KV_NUMBER: ${KV_NUMBER}, FIELD_LENGTH: ${FIELD_LENGTH}, KEY_LENGTH: ${KEY_LENGTH}, KEY_LENGTHMin: ${KEY_LENGTHMin}, KEY_LENGTHMax: ${KEY_LENGTHMax}, ROUND_NUMBER: ${ROUND_NUMBER}, COMPACTION_LEVEL: ${COMPACTION_LEVEL[@]}, ENABLE_AUTO_COMPACTION: ${ENABLE_AUTO_COMPACTION}, ENABLE_COMPACTION_CFS: ${ENABLE_COMPACTION_CFS}, MOTIVATION: ${MOTIVATION[@]}, MEMORY_LIMIT: ${MEMORY_LIMIT}, USE_DIRECTIO: ${USE_DIRECTIO[@]}, REBUILD_SERVER: ${REBUILD_SERVER}, REBUILD_CLIENT: ${REBUILD_CLIENT}, LOG_LEVEL: ${LOG_LEVEL}, BRANCH: ${BRANCH}, PURPOSE: ${PURPOSE}, SETTING: ${SETTING}, SCHEDULING_INITIAL_DELAY: ${SCHEDULING_INITIAL_DELAY}, SCHEDULING_INTERVAL: ${SCHEDULING_INTERVAL[@]}, STATES_UPDATE_INTERVAL: ${STATES_UPDATE_INTERVAL}, READ_SENSISTIVITY: ${READ_SENSISTIVITY}, STEP_SIZE: ${STEP_SIZE[@]}, OFFLOAD_THRESHOLD: ${OFFLOAD_THRESHOLD[@]}, RECOVER_THRESHOLD: ${RECOVER_THRESHOLD[@]}

    ENABLE_HORSE="false"
    if [[ "${TARGET_SCHEME}" == "horse" ]]; then
        ENABLE_HORSE="true"
    fi


    # Run experiments
    echo "Start experiment to ${TARGET_SCHEME}"
    perpareJavaEnvironment "${TARGET_SCHEME}" "${JDK_VERSION}"



    if [ "${REBUILD_SERVER}" == "true" ]; then
        echo "Rebuild the server"
        rebuildServer "${BRANCH}"
    fi

    if [ "${REBUILD_CLIENT}" == "true" ]; then
        echo "Rebuild the client"
        rebuildClient
    fi




    for compactionLevel in "${COMPACTION_LEVEL[@]}"; do
        for rf in "${REPLICAS[@]}"; do
            # Copy the data set to each node and startup the process from th dataset
            # if [ "$BACKUP_MODE" != "local" ]; then
            #     copyDatasetToNodes "${EXP_NAME}" "${scheme}" "${KV_NUMBER}" "${KEY_LENGTH}" "${FIELD_LENGTH}" "${rf}" "${compactionLevel}" "${PURPOSE}"
            #     sleep 600
            # fi
            for round in $(seq 1 $ROUND_NUMBER); do
                for dist in "${REQUEST_DISTRIBUTIONS[@]}"; do
                    for workload in "${Workloads[@]}"; do
                        for threadsNum in "${THREAD_NUMBER[@]}"; do
                            for memtableSize in "${MEMTABLE_SIZE[@]}"; do
                                for directIO in "${USE_DIRECTIO[@]}"; do
                                    for motivation in "${MOTIVATION[@]}"; do
                                        for schedulingInterval in "${SCHEDULING_INTERVAL[@]}"; do
                                            for throttleDataRate in "${THROTLLE_DATA_RATE[@]}"; do
                                                for consistencyLevel in "${CONSISTENCY_LEVEL[@]}"; do
                                                    if [ "${compactionLevel}" == "zero" ]; then
                                                        ENABLE_AUTO_COMPACTION="false"
                                                        ENABLE_COMPACTION_CFS=""
                                                    elif [ "${compactionLevel}" == "one" ]; then
                                                        ENABLE_AUTO_COMPACTION="true"
                                                        ENABLE_COMPACTION_CFS="usertable0"
                                                    elif [ "${compactionLevel}" == "all" ]; then
                                                        ENABLE_AUTO_COMPACTION="true"
                                                        ENABLE_COMPACTION_CFS="usertable0 usertable1 usertable2"
                                                    fi
                                                    echo "RunDB: Start round ${round}, the threads number is ${threadsNum}, sstable size is ${SSTABLE_SIZE_IN_MB}, memtable size is ${memtableSize}, rf is ${rf}, workload is ${workload}, request distribution is ${dist} and compaction level is ${compactionLevel}, enableAutoCompaction is ${ENABLE_AUTO_COMPACTION}, throttleDataRate is ${throttleDataRate} MB/s"

                                                    SETTING=$(getSettingName ${motivation} ${compactionLevel})

                                                    # init the configuration file, set all nodes as the seeds to support fast startup
                                                    initConf "false"

                                                    # if [ "$TARGET_SCHEME" != "depart-5.0" ]; then
                                                    # startup from preload dataset
                                                    if [ "${EXP_NAME}" == "Exp-MixedReadWrite" ]; then
                                                        echo "Start from backup"
                                                        startFromBackup "LoadDB" $TARGET_SCHEME ${KV_NUMBER} ${KEY_LENGTH} ${FIELD_LENGTH} ${rf} ${memtableSize} ${motivation} ${REBUILD_SERVER} "${directIO}" "${LOG_LEVEL}" "${BRANCH}" "${SCHEDULING_INITIAL_DELAY}" "${schedulingInterval}" "${STATES_UPDATE_INTERVAL}" "${READ_SENSISTIVITY}" ${ENABLE_HORSE} ${throttleDataRate} ${SSTABLE_SIZE_IN_MB} ${compaction_strategy}
                                                    else
                                                        echo "Start from current data"
                                                        restartCassandra ${memtableSize} ${motivation} ${REBUILD_SERVER} "${directIO}" "${LOG_LEVEL}" "${BRANCH}" "${SCHEDULING_INITIAL_DELAY}" "${schedulingInterval}" "${STATES_UPDATE_INTERVAL}" "${READ_SENSISTIVITY}" ${ENABLE_HORSE} ${throttleDataRate}
                                                    fi
                                                    # fi
                                                    # modify the seeds as the specific nodes, and reload the configuration file
                                                    initConf "true"
                                                    reloadSeeds ${TARGET_SCHEME}

                                                    run ${TARGET_SCHEME} ${dist} ${workload} ${threadsNum} ${KV_NUMBER} ${OPERATION_NUMBER} ${KEY_LENGTH} ${FIELD_LENGTH} ${ENABLE_AUTO_COMPACTION} "${ENABLE_COMPACTION_CFS}" "${MEMORY_LIMIT}" "${LOG_LEVEL}" "${ENABLE_HORSE}" "${consistencyLevel}"

                                                    # Set the seed nodes as all the nodes, and reload the configuration file
                                                    initConf "false"
                                                    reloadSeeds ${TARGET_SCHEME}


                                                    # Collect load results
                                                    resultsDir=$(getResultsDir ${CLUSTER_NAME} ${TARGET_SCHEME} ${EXP_NAME} ${SETTING} ${workload} ${dist} ${compactionLevel} ${threadsNum} ${schedulingInterval} ${round} ${ENABLE_HORSE} ${throttleDataRate} ${OPERATION_NUMBER} ${KV_NUMBER} ${SSTABLE_SIZE_IN_MB} ${compaction_strategy} ${consistencyLevel}) 

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
}
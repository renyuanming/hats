#!/bin/bash

# CLUSTER_NAME=""
# BACKUP_MODE="" # local remote
# SCHEME="" # depart or Horse
PROJECT_NAME="Horse"
HOME_PATH=""
echo "Load the settings.sh, CLUSTER_NAME is ${CLUSTER_NAME}, BACKUP_MODE is ${BACKUP_MODE}, SCHEME is ${SCHEME}"
if [[ "$CLUSTER_NAME" == "" ]]; then
    echo "Please specify the CLUSTER_NAME"
    exit 1
elif [[ "$CLUSTER_NAME" == "1x" ]]; then
    Nodes=("node11" "node13" "node15" "node16" "node18" "node19")
    NodesIP=("192.168.10.21" "192.168.10.23" "192.168.10.25" "192.168.10.26" "192.168.10.28" "192.168.10.29")
    Client="node20"
    ClientIP="192.168.10.29"
    Seeds=("node11")
    SeedsIP=("192.168.10.21")
    NodeIP="192.168.10.21" # Only used for start-client.sh
    UserName="ymren"
    SudoPassword="ymren"
    HOME_PATH="/mnt/ssd"
elif [[ "$CLUSTER_NAME" == "4x" ]]; then
    Nodes=("node41" "node42" "node43" "node45")
    NodesIP=("192.168.10.51" "192.168.10.52" "192.168.10.53" "192.168.10.55")
    Client="node49"
    ClientIP="192.168.10.59"
    Seeds=("node41" "node42")
    SeedsIP=("192.168.10.51" "192.168.10.52")
    NodeIP="192.168.10.51" # Only used for start-client.sh
    UserName="ymren"
    SudoPassword="898915"
    HOME_PATH="/home/ymren"
else
    echo "Invalid cluster name $CLUSTER_NAME"
    exit 1
fi




NodeNumber=${#Nodes[@]}
Coordinators=""
for i in "${!NodesIP[@]}"; do
    Coordinators+="${NodesIP[i]}"
    if [ $i -ne $((${#NodesIP[@]} - 1)) ]; then
        Coordinators+=","
    fi
done

SEEDS=""
for i in "${!SeedsIP[@]}"; do
    SEEDS+="${SeedsIP[i]}"
    if [ $i -ne $((${#SeedsIP[@]} - 1)) ]; then
        SEEDS+=","
    fi
done

AllNodes=("${Nodes[@]}")
AllNodes+=("${Client}")
echo "All nodes list is ${AllNodes}"

NumTokens=1
PathToServer="${HOME_PATH}/$SCHEME/server"
PathToScripts="${HOME_PATH}/${PROJECT_NAME}/scripts"
PathToClient="${HOME_PATH}/${PROJECT_NAME}/client"
PathToBackup="${HOME_PATH}/backups"
ClientLogDir="${PathToClient}/logs/"
NetworkInterface="enp1s0f0"
PathToResultDir="$PathToServer/metrics"

if [[ "$SCHEME" == "" || "$BACKUP_MODE" == "" ]]; then
    echo "Please specify the SCHEME and BACKUP_MODE"
elif [ "$BACKUP_MODE" == "local" ]; then
    PathToBackup="/home/ymren/backups"
elif [ "$BACKUP_MODE" == "remote" ]; then
    echo "Do nothing"
else
    echo "Invalid BACKUP_MODE $BACKUP_MODE"
fi
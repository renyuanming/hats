#!/bin/bash
. /etc/profile
func() {

    recordcount=$1
    operationcount=$2
    threads=$3
    workload=$4
    expName=$5
    keyspace=$6
    resultDir=$7
    

    dirName="${expName}-${workload}-${keyspace}-${recordcount}-${operationcount}-${threads}-$(date +%s)"

    cp -r /home/ymren/cassandraWS/logs /home/ymren/logs/$dirName
    cp -r ${resultDir} /home/ymren/logs/$dirName
    
    cp -r ${resultDir} ~/Results
}

func "$1" "$2" "$3" "$4" "$5" "$6" "$7"

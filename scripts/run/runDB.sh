#!/bin/bash
. /etc/profile

recordcount=$1
operationcount=$2
key_length=$3
field_length=$4
threads=$5
workload=$6
requestDistribution=$7
PathToClient=$8
coordinator=$9
shift 9
scheme=$1
enableHorse=$2
shuffleReplicas=$3

cd ${PathToClient} || exit

keyspace=""
echo "Running YCSB with scheme: $scheme"
if [ "$scheme" == "mlsm" ]; then
    keyspace="ycsb"
    sed -i "s/table=.*$/table=usertable0/" ${workload}
elif [ "$scheme" == "cassandra" ]; then
    keyspace="ycsbraw"
    sed -i "s/table=.*$/table=usertable/" ${workload}
elif [ "$scheme" == "depart" ]; then
    keyspace="ycsb"
    sed -i "s/table=.*$/table=usertable/" ${workload}
else
    echo "Unknow scheme $scheme"
fi

sed -i "s/recordcount=.*$/recordcount=${recordcount}/" ${workload}
sed -i "s/operationcount=.*$/operationcount=${operationcount}/" ${workload}
sed -i "s/keylength=.*$/keylength=${key_length}/" ${workload}
sed -i "s/fieldlength=.*$/fieldlength=${field_length}/" ${workload}
# sed -i "s/requestdistribution=.*$/requestdistribution=${requestDistribution}/" ${workload}

mkdir -p logs
file_name="Run-$(date +%s)-${operationcount}-${field_length}-${threads}-${requestDistribution}"

bin/ycsb run cassandra-cql -p hosts=${coordinator} -p cassandra.keyspace=${keyspace} -p cassandra.tracing="false" -p enable.horse="${enableHorse}" -p shuffle.replicas="${shuffleReplicas}" -threads $threads -s -P ${workload} > logs/${file_name}.log 2>&1

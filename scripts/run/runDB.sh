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
targetScheme=$1
enableHorse=$2
hostName=$(hostname)

cd ${PathToClient} || exit

echo "Running YCSB with scheme: $targetScheme"


keyspace="ycsb"
if [ "$targetScheme" == "c3" ] || [ "$targetScheme" == "horse" ] || [ "$targetScheme" == "mlsm" ]; then
    sed -i "s/table=.*$/table=usertable0/" ${workload}
elif [ "$targetScheme" == "cassandra-5.0" ] || [ "$targetScheme" == "depart" ] || [ "$targetScheme" == "cassandra-3.11.4" ] || [ "$targetScheme" == "depart-5.0" ]; then
    sed -i "s/table=.*$/table=usertable/" ${workload}
else
    echo "Unknow targetScheme $targetScheme"
fi

sed -i "s/recordcount=.*$/recordcount=${recordcount}/" ${workload}
sed -i "s/operationcount=.*$/operationcount=${operationcount}/" ${workload}
sed -i "s/keylength=.*$/keylength=${key_length}/" ${workload}
sed -i "s/fieldlength=.*$/fieldlength=${field_length}/" ${workload}
# sed -i "s/requestdistribution=.*$/requestdistribution=${requestDistribution}/" ${workload}

mkdir -p logs
file_name="Run-$(date +%s)-${hostname}-${operationcount}-${field_length}-${threads}-${requestDistribution}"

bin/ycsb run cassandra-cql -p hosts=${coordinator} -p cassandra.keyspace=${keyspace} -p cassandra.tracing="false" -p enable.horse="${enableHorse}" -threads $threads -s -P ${workload} > logs/${file_name}.log 2>&1

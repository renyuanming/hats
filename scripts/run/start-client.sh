# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

. /etc/profile

kill -9 $(ps aux | grep ycsb | grep -v grep | awk 'NR == 1'  | awk {'print $2'})
func() {
    coordinator=$1
    sstable_size=$2
    fanout_size=$3
    file_dir=$4
    replication_factor=$5
    mode=$6

    cd $file_dir

    if [ "$mode" == "mlsm" ] || [ "$mode" == "horse" ]; then
        echo "Enable multiple LSM tree"
        bin/cqlsh "$coordinator" -e "create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': $replication_factor };
        USE ycsb;
        create table usertable0 (y_id varchar primary key, field0 varchar);
        ALTER TABLE usertable0 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': ${sstable_size}, 'fanout_size': ${fanout_size}};
        ALTER TABLE usertable1 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': ${sstable_size}, 'fanout_size': ${fanout_size}};
        ALTER TABLE usertable2 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': ${sstable_size}, 'fanout_size': ${fanout_size}};
        consistency all;"
    elif [ "$mode" == "cassandra-5.0" ] || [ "$mode" == "depart" ] || [ "$mode" == "cassandra-3.11.4" ]; then
        bin/cqlsh "$coordinator" -e "create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': $replication_factor };
        USE ycsb;
        create table usertable (y_id varchar primary key, field0 varchar);
        ALTER TABLE usertable WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': $sstable_size, 'fanout_size': $fanout_size};
        consistency all;"
    else

        echo "ERROR mode $mode"
        exit 1
    fi
}

func "$1" "$2" "$3" "$4" "$5" "$6"

# bin/cqlsh "192.168.10.41" -e "create keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3 };
#         USE ycsb;
#         create table usertable0 (y_id varchar primary key, field0 varchar);
#         ALTER TABLE usertable0 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 16, 'fanout_size': 10};
#         ALTER TABLE usertable1 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 16, 'fanout_size': 10};
#         ALTER TABLE usertable2 WITH compaction = { 'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 16, 'fanout_size': 10};
#         consistency all;"
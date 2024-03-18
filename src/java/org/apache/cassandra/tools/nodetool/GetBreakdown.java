/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tools.nodetool;

import static java.lang.String.format;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;


@Command(name = "getbreakdown", description = "Print the breakdown of the foreground requests")
public class GetBreakdown extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace>]", description = "The keyspace name")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        Multimap<String, String> tablesList = HashMultimap.create();

        // a <keyspace, set<table>> mapping for verification or as reference if none provided
        Multimap<String, String> allTables = HashMultimap.create();
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> tableMBeans = probe.getColumnFamilyStoreMBeanProxies();
        while (tableMBeans.hasNext())
        {
            Map.Entry<String, ColumnFamilyStoreMBean> entry = tableMBeans.next();
            allTables.put(entry.getKey(), entry.getValue().getTableName());
        }

        if (args.size() == 1)
        {
            String keyspace = args.get(0);
            for (String table : allTables.get(keyspace))
            {
                tablesList.put(keyspace, table);
            }
        }
        else if (args.size() == 0)
        {
            // use all tables
            tablesList = allTables;
        }
        else
        {
            throw new IllegalArgumentException("tablehistograms requires <keyspace> format argument.");
        }

        // Get the local operation latency for each table and generate the average read latency 
        for(String keyspace : tablesList.keys().elementSet())
        {
            long totalReadCount = 0;
            long totalWriteCount = 0;
            Map<String, Long> readCount = new HashMap<>();
            Map<String, Long> writeCount = new HashMap<>();
            Map<String, Double> readLatency = new HashMap<>();
            Map<String, Double> writeLatency = new HashMap<>();
            
            // Get local operation latecy of each table
            for(String table : tablesList.get(keyspace))
            {
                long tableWriteCount = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspace, table, "WriteLatency")).getCount();
                long tableReadCount = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspace, table, "ReadLatency")).getCount();
                double localReadLatency = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspace, table, "ReadLatency")).getMean();
                double localWriteLatency = ((CassandraMetricsRegistry.JmxTimerMBean) probe.getColumnFamilyMetric(keyspace, table, "WriteLatency")).getMean();

                
                totalReadCount += tableReadCount;
                totalWriteCount += tableWriteCount;
                readCount.put(table, tableReadCount);
                writeCount.put(table, tableReadCount);
                readLatency.put(table, localReadLatency);
                writeLatency.put(table, localWriteLatency);
            }

            double averageLocalReadLatency = 0;
            double averageLocalWriteLatency = 0;
            for(String table : tablesList.get(keyspace))
            {
                out.println(format("Local read latency for table %s: %f", table, readLatency.get(table)));
                out.println(format("Local read count for table %s: %d", table, readCount.get(table)));
                out.println(format("Local write latency for table %s: %f", table, writeLatency.get(table)));
                out.println(format("Local write count for table %s: %d", table, writeCount.get(table)));
                averageLocalReadLatency += readLatency.get(table) * readCount.get(table) / totalReadCount;
                averageLocalWriteLatency += writeLatency.get(table) * writeCount.get(table) / totalWriteCount;
            }
            out.println(format("Average local read latency for keyspace %s: %f", keyspace, averageLocalReadLatency));
            out.println(format("Total local read count for keyspace %s: %d", keyspace, totalReadCount));
            out.println(format("Average local write latency for keyspace %s: %f", keyspace,averageLocalWriteLatency));
            out.println(format("Total local write count for keyspace %s: %d", keyspace, totalWriteCount));
            out.println();

        }

        // Get messaging queuing latency
        
        String[] messageTypes = {"READ_RSP", "READ_REQ", "MUTATION_RSP", "MUTATION_REQ"};
        out.println("Print the average messaging queue wait latency for each message type:");
        for (String messageType : messageTypes)
        {
            out.println(format("Messaging queue wait latency for %s: %f", messageType, probe.getMessagingQueueWaitMetrics(messageType).getMean()));
        }
        out.println();

        // Get the network operations latency
        String[] networkOperations = {"Read", "Write", "Range"};
        out.println("Print the average network operations latency for each operation type:");
        for (String operation : networkOperations)
        {
            out.println(format("Network operations latency for %s: %f", operation, probe.getProxyMetric(operation).getMean()));
        }

    }
}

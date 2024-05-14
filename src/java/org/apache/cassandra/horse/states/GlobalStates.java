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

package org.apache.cassandra.horse.states;


import java.io.Serializable;
import java.net.InetAddress;
import java.util.Map;

import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.horse.HorseUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalStates implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(GlobalStates.class);

    public static GlobalStates globalStates;
    
    public Double[] scoreVector; // N
    public Double[] latencyVector; // N
    public int[] readCountVector; // N
    public int[] versionVector; // N
    public int[][][] loadMatrix; // N X M X 2
    private final int nodeCount;
    private final int rf;

    public GlobalStates(int nodeCount, int rf)
    {
        this.nodeCount = nodeCount;
        this.rf = rf;
        initialization();
    }

    public void initialization()
    {
        this.scoreVector = new Double[this.nodeCount];
        this.latencyVector = new Double[this.nodeCount];
        this.readCountVector = new int[this.nodeCount];
        this.loadMatrix = new int[this.nodeCount][this.rf][2];
        this.versionVector = new int[this.nodeCount];
    }

    public synchronized void mergeGlobalStates(Map<InetAddress, LocalStates> gatheredStates)
    {
        if(gatheredStates.size() != this.nodeCount)
        {
            logger.debug("rymDebug: the gathered states number is not equal to the node count.");
        }

        for (Map.Entry<InetAddress, LocalStates> entry : gatheredStates.entrySet())
        {
            int nodeIndex = Gossiper.getAllHosts().indexOf(InetAddressAndPort.getByAddress(entry.getKey()));
            if(nodeIndex == -1)
            {
                throw new IllegalStateException("Host not found in Gossiper");
            }

            if(entry.getValue().version >= globalStates.versionVector[nodeIndex])
            {
                globalStates.versionVector[nodeIndex] = entry.getValue().version;
                globalStates.latencyVector[nodeIndex] = entry.getValue().latency;
                globalStates.readCountVector[nodeIndex] = 0;

                for (Map.Entry<InetAddress, Integer> entry1 : entry.getValue().completedReadRequestCount.entrySet())
                {
                    int replicaIndex = HorseUtils.getReplicaIndex(nodeIndex, entry1.getKey());
                    GlobalStates.globalStates.loadMatrix[nodeIndex][replicaIndex][0] = entry1.getValue();
                    GlobalStates.globalStates.readCountVector[nodeIndex] += entry1.getValue();
                }

                globalStates.scoreVector[nodeIndex] = getScore(globalStates.latencyVector[nodeIndex], globalStates.readCountVector[nodeIndex]);

            }
            else
            {
                continue;
            }
        }

        StorageService.instance.stateGatheringSignalInFlight.decrementAndGet();
    }

    private static double getScore(double latency, int requestCount)
    {
        double score = latency;

        return score;
    }

}

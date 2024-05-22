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

package org.apache.cassandra.horse.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.horse.states.LocalStates;
import org.apache.cassandra.locator.InetAddressAndPort;


public class ReplicaSelector 
{

    public final static RandomSelector randomSelector = new RandomSelector();
    /**
     * 1. It can periodically update the score based on the placement policy and the sampling latency
     */

    public static ConcurrentHashMap<InetAddressAndPort, Double> sampleLatency = new ConcurrentHashMap<InetAddressAndPort, Double>();
    public static volatile double minLatency = 0.0;

    public static double getScore(InetAddressAndPort replicationGroup, InetAddressAndPort targetAddr)
    {
        double greedyScore = 0.0;
        double latencyScore = 0.0;

        if(LocalStates.localPolicyWithAddress.get(replicationGroup) != null)
        {
            greedyScore = LocalStates.localPolicyWithAddress.get(replicationGroup).get(targetAddr);
        }

        if(sampleLatency.containsKey(targetAddr))
        {
            latencyScore = minLatency / sampleLatency.get(targetAddr);
        }
        return greedyScore + latencyScore;
    }

    
    public static class RandomSelector
    {
        private final Random random;
        private static final List<Double> testPolicy = new ArrayList<Double>(Arrays.asList(0.5, 0.25, 0.25));
        public RandomSelector() 
        {
            this.random = new Random();
        }

        public int selectReplica(List<Double> policy) 
        {
            double rand = this.random.nextDouble();
            if(rand < policy.get(0)) 
            {
                return 0;
            }
            else if(rand < policy.get(0) + policy.get(1)) 
            {
                return 1;
            }
            else 
            {
                return 2;
            }
        }

        public int selectReplica() 
        {
            return selectReplica(testPolicy);
        }
    }
}

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

package org.apache.cassandra.horse;


import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.horse.leaderelection.election.ElectionBootstrap;
import org.apache.cassandra.horse.leaderelection.priorityelection.PriorityElectionBootstrap;
import org.apache.cassandra.horse.net.StatesGatheringSignal;
import org.apache.cassandra.horse.states.GlobalStates;
import org.apache.cassandra.horse.states.LocalStates;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Scheduler {

    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);
    private static Boolean isPriorityElection = false;
    private static int priority = 100;
    private static Set<InetAddressAndPort> liveSeeds;

    public static Runnable getLeaderElectionRunnable()
    {
        return new LeaderElectionRunnable();
    }

    private static class LeaderElectionRunnable implements Runnable
    {
        // Check the seed node status, if there is less than one seed node, change the election scheme
        @Override
        public void run() {
            Set<InetAddressAndPort> liveSeeds = Gossiper.getAllSeeds().stream().filter(Gossiper.instance::isAlive).collect(Collectors.toSet());
            
            // Step1. Start a new leader election scheme if needed
            if (!getIsPriorityElection() && liveSeeds.size() <= 1)
            {
                logger.debug("rymDebug: no more than 1 seed node is alive, we need to change the election scheme. Lived seed node is {}, live members are: {}", liveSeeds, Gossiper.instance.getLiveMembers());

                setIsPriorityElection(true);
                ElectionBootstrap.shutdownElection(liveSeeds);
                if(liveSeeds.contains(FBUtilities.getBroadcastAddressAndPort()))
                {
                    priority = 1000000000;
                }

                PriorityElectionBootstrap.initElection(HorseUtils.getRaftLogPath(), 
                                        "ElectDataNodes", 
                                        DatabaseDescriptor.getListenAddress().getHostAddress()+":"+DatabaseDescriptor.getRaftPort()+"::"+priority, 
                                        HorseUtils.InetAddressAndPortSetToString(Gossiper.instance.getLiveMembers(), DatabaseDescriptor.getRaftPort(), liveSeeds));
                // [TODO] Wait for the new leader election to finish
            }
            else
            {
                logger.debug("rymDebug: isPriorityElection: {}, seenAnySeed: {}, liveMembers: {}, seed nodes are: {}", getIsPriorityElection(), Gossiper.instance.seenAnySeed(), Gossiper.instance.getLiveMembers(), Gossiper.instance.getSeeds());
            }

        }
        
    }

    public static Boolean getIsPriorityElection()
    {
        return isPriorityElection;
    }

    public static void setIsPriorityElection(Boolean isPriorityElection)
    {
        Scheduler.isPriorityElection = isPriorityElection;
    }


    public static Runnable getSchedulerRunnable()
    {
        return new SchedulerRunnable();
    }

    private static class SchedulerRunnable implements Runnable
    {
        @Override
        public void run()
        {
            if (ElectionBootstrap.isLeader() || PriorityElectionBootstrap.isLeader())
            {
                logger.debug("rymDebug: Node {} is the leader. Start the scheduler.", FBUtilities.getBroadcastAddressAndPort());

                // Step1. Gather the load statistic
                gatheringLoadStatistic();
                while(StorageService.instance.stateGatheringSignalInFlight.get() != 0)
                {
                    try {
                        Thread.sleep(1);
                        logger.debug("rymDebug: wait until the states are gathered, there are still {} left.", 
                                     StorageService.instance.stateGatheringSignalInFlight.get());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                logger.debug("rymDebug: we now have the global states, the version vector is {}, latency vector is {}, request count vector is {}, score vector is {}, the load matrix is {}", 
                             GlobalStates.globalStates.versionVector, 
                             GlobalStates.globalStates.latencyVector, 
                             GlobalStates.globalStates.readCountVector, 
                             GlobalStates.globalStates.scoreVector, 
                             GlobalStates.globalStates.loadMatrix);
                
                // Step2. Recalculate the placement policy if needed.

                // Step3. Distribute the placement policy to all nodes and clients
            }
        }
    }

    /**
    * Gather the load statistic has three cases:
    * 1. If we have multiple live seed nodes, we gathering the load statistic from these seed nodes
    * 2. If we have only one live seed node, we gathering the load statistic locally
    * 3. If we have no live seed node, we gathering the load statistic from all live members
    */
    public static void gatheringLoadStatistic()
    {
        // Check if this node is the leader
        if (!ElectionBootstrap.isLeader() && !PriorityElectionBootstrap.isLeader())
        {
            throw new IllegalStateException("This method should be called by the leader node.");
        }
        logger.debug("rymDebug: Node {} is the leader. Start the scheduler.", FBUtilities.getBroadcastAddressAndPort());

        GlobalStates.globalStates = new GlobalStates(Gossiper.getAllHosts().size(), 3);
        if(liveSeeds.size() == 1)
        {
            for (Map.Entry<InetAddressAndPort, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
            {
                String localStatesStr = entry.getValue().getApplicationState(ApplicationState.FOREGROUND_LOAD).value;
                int version = entry.getValue().getApplicationState(ApplicationState.FOREGROUND_LOAD).version;

                LocalStates localStates = LocalStates.fromString(localStatesStr, version);

                int nodeIndex = Gossiper.getAllHosts().indexOf(entry.getKey());
                if (nodeIndex == -1)
                {
                    throw new IllegalStateException("Host not found in Gossiper");
                }

                GlobalStates.globalStates.latencyVector[nodeIndex] = localStates.latency;
                GlobalStates.globalStates.versionVector[nodeIndex] = localStates.version;
                GlobalStates.globalStates.readCountVector[nodeIndex] = 0;
                for (Map.Entry<InetAddress, Integer> entry1 : localStates.completedReadRequestCount.entrySet())
                {
                    int replicaIndex = HorseUtils.getReplicaIndex(nodeIndex, entry1.getKey());
                    GlobalStates.globalStates.loadMatrix[nodeIndex][replicaIndex][0] = entry1.getValue();
                    GlobalStates.globalStates.readCountVector[nodeIndex] += entry1.getValue();
                }
            }
        }
        else if (liveSeeds.size() > 1)
        {
            StatesGatheringSignal signal = new StatesGatheringSignal(true);
            for(InetAddressAndPort seed : liveSeeds)
            {
                StorageService.instance.stateGatheringSignalInFlight.incrementAndGet();
                if(seed.equals(FBUtilities.getBroadcastAddressAndPort()))
                {
                    continue;
                }
                signal.sendStatesGatheringSignal(seed);
            }
        }
        else
        {
            StatesGatheringSignal signal = new StatesGatheringSignal(false);
            for(InetAddressAndPort follower : Gossiper.instance.getLiveMembers())
            {
                StorageService.instance.stateGatheringSignalInFlight.incrementAndGet();
                if(follower.equals(FBUtilities.getBroadcastAddressAndPort()))
                {
                    continue;
                }
                signal.sendStatesGatheringSignal(follower);
            }
        }
    }

}

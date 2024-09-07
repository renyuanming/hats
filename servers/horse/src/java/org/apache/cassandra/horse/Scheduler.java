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
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.horse.controller.BackgroundController;
import org.apache.cassandra.horse.controller.LoadBalancer;
import org.apache.cassandra.horse.leaderelection.election.ElectionBootstrap;
import org.apache.cassandra.horse.leaderelection.priorityelection.PriorityElectionBootstrap;
import org.apache.cassandra.horse.net.PolicyDistribute;
import org.apache.cassandra.horse.net.PolicyReplicate;
import org.apache.cassandra.horse.net.StatesGatheringSignal;
import org.apache.cassandra.horse.states.GlobalStates;
import org.apache.cassandra.horse.states.LocalStates;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * @author renyuanming1@gmail.com
 */

public class Scheduler {

    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);
    private static Boolean isPriorityElection = false;
    private static int priority = 100;
    private static Set<InetAddressAndPort> liveSeeds;


    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_BLUE = "\u001B[34m";

    public static Runnable getLeaderElectionRunnable()
    {
        return new LeaderElectionRunnable();
    }

    private static class LeaderElectionRunnable implements Runnable
    {
        // Check the seed node status, if there is less than one seed node, change the election scheme
        @Override
        public void run() {
            liveSeeds = Gossiper.getAllSeeds().stream().filter(Gossiper.instance::isAlive).collect(Collectors.toSet());
            
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
                                        DatabaseDescriptor.getListenAddress().getHostAddress() + ":"+DatabaseDescriptor.getRaftPort()+"::"+priority, 
                                        HorseUtils.InetAddressAndPortSetToString(Gossiper.instance.getLiveMembers(), 
                                                                                 DatabaseDescriptor.getRaftPort(), 
                                                                                 liveSeeds));
            }
            // else
            // {
            //     logger.debug("rymDebug: isPriorityElection: {}, seenAnySeed: {}, liveMembers: {}, seed nodes are: {}", getIsPriorityElection(), Gossiper.instance.seenAnySeed(), Gossiper.instance.getLiveMembers(), Gossiper.instance.getSeeds());
            // }

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

            if(ElectionBootstrap.isStarted() || PriorityElectionBootstrap.isStarted())
            {
                if(GlobalStates.globalPolicy == null)
                {
                    GlobalStates.initializeGlobalPolicy();                
                }
            }
            else
            {
                return;
            }

            if (ElectionBootstrap.isLeader() || PriorityElectionBootstrap.isLeader())
            {
                logger.debug("rymDebug: Node {} is the leader. Start the scheduler.", FBUtilities.getBroadcastAddressAndPort());

                // Step1. Gather the load statistic
                gatheringLoadStatistic();
                    
                // Step2. Calculate the placement policy if needed.
                calculateGlobalPolicy();

                // Step3. Update local Policy for coordinator
                LocalStates.updateLocalPolicy();

                // Step4. Acknowledge to the client driver
                StorageService.instance.notifyPolicy(GlobalStates.transformPolicyForClient(), GlobalStates.globalStates.getGlobalCoordinatorReadLatency());

                // Step5. Replicate the placement policy to the followers
                replicateGlobalPolicy();
                
                // Step6. Update the policy for background task control.
                // BackgroundController.updateLimiter(GlobalStates.globalPolicy[Gossiper.getAllHosts().indexOf(FBUtilities.getBroadcastAddressAndPort())]);
                BackgroundController.updateLimiter(GlobalStates.translatePolicyForBackgroundController(StorageService.instance.localAddressAndPort));

            }
        }
    }

    // We send the placement policy to all the live nodes
    private static void replicateGlobalPolicy()
    {
        for(InetAddressAndPort follower : Gossiper.instance.getLiveMembers())
        {
            if (follower.equals(FBUtilities.getBroadcastAddressAndPort())) 
            {
                continue;
            }

            // Replicate the placement policy
            PolicyReplicate.sendPlacementPolicy(follower, GlobalStates.globalPolicy);
        }
    }

    // Note that we only send the partial placement policy to all the data nodes.
    private static void distributeCompactionRate()
    {
        if (liveSeeds.size() > 1)
        {
            for (InetAddressAndPort dataNode : Gossiper.instance.getLiveMembers())
            {
                if(liveSeeds.contains(dataNode))
                {
                    continue;
                }
                
                Double[] backgroundCompactionRate = new Double[GlobalStates.rf];
                int nodeIndex = Gossiper.getAllHosts().indexOf(dataNode);
                for(int j = 0; j < GlobalStates.rf; j++)
                {

                    // Calculate the rate limit for each replica
                    backgroundCompactionRate[j] = GlobalStates.globalPolicy[nodeIndex][j];
                }
                
                PolicyDistribute.sendPlacementPolicy(dataNode, backgroundCompactionRate);
            }
        }
    }

    /**
     * Calculate the placement policy.
     * 
     * Basic idea: To minimize the resource contention within a node, we initially place all the read 
     * requests of each replication group to the first $RCL$ replicas. Then we greedily adjust the placement 
     * policy of each replication group along the ring. For each node, we will compute a score based on the 
     * number of read requests and the mean value of the read latency within a period of time. Note that the
     * larger latency and the request count, the higher the score. We consider the following cases:
     * 
     * 1. If $node_i$ has a larger latency than $node_{i+1,...,i+M-1}$, we consider gently moving the load of 
     *    $RP_i$ to $node_{i+1,...,i+M-1}$. Here we just use a very simple algorithm to adjust the placement 
     *    policy, for example AIMD or fixed step size.
     * 2. If $node_i$ has a smaller latency than $node_{i+1,...,i+M-1}$, we consider recovering the load of 
     *    $RP_i$ from $node_{i+1,...,i+M-1}$.
     * 
     * Input: scoreVector, loadMatrix
     * Output: placementPolicy
     * 
     */
    private static void calculateGlobalPolicy()
    {
        logger.info(ANSI_YELLOW+"rymInfo: Calculating placement policy, the old value is {}"+ ANSI_RESET, Arrays.deepToString(GlobalStates.globalPolicy));

        GlobalStates.globalPolicy = LoadBalancer.balanceLoad(GlobalStates.globalStates.nodeCount, 
                                                             GlobalStates.rf, 
                                                             DatabaseDescriptor.getSchedulingInterval(), 
                                                             GlobalStates.globalStates.latencyVector, 
                                                             GlobalStates.globalStates.loadMatrix);


        logger.info(ANSI_YELLOW+"rymInfo: The new placement policy is {}"+ ANSI_RESET, Arrays.deepToString(GlobalStates.globalPolicy));
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
        // logger.debug("rymDebug: Node {} is the leader. Start the scheduler.", FBUtilities.getBroadcastAddressAndPort());

        
        StorageService.instance.stateGatheringSignalInFlight.set(0);

        GlobalStates.globalStates = new GlobalStates(Gossiper.getAllHosts().size());
        if(liveSeeds.size() == 1)
        {
            for (Map.Entry<InetAddressAndPort, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
            {
                String localStatesStr = entry.getValue().getApplicationState(ApplicationState.FOREGROUND_LOAD).value;
                int version = entry.getValue().getApplicationState(ApplicationState.FOREGROUND_LOAD).version;

                LocalStates localStates = LocalStates.fromString(localStatesStr, version);

                if(localStates == null)
                {
                    continue;
                }

                int nodeIndex = Gossiper.getAllHosts().indexOf(entry.getKey());
                if (nodeIndex == -1)
                {
                    throw new IllegalStateException("Host not found in Gossiper");
                }

                GlobalStates.globalStates.latencyVector[nodeIndex] = localStates.latency;
                GlobalStates.globalStates.versionVector[nodeIndex] = localStates.version;
                GlobalStates.globalStates.readCountOfEachNode[nodeIndex] = 0;
                for (Map.Entry<InetAddress, Integer> entry1 : localStates.completedReadRequestCount.entrySet())
                {
                    int replicaIndex = HorseUtils.getReplicaIndexFromGossipInfo(nodeIndex, entry1.getKey());
                    GlobalStates.globalStates.loadMatrix[nodeIndex][replicaIndex] = entry1.getValue();
                    GlobalStates.globalStates.readCountOfEachNode[nodeIndex] += entry1.getValue();
                }
                GlobalStates.globalStates.scoreVector[nodeIndex] = GlobalStates.getScore(GlobalStates.globalStates.latencyVector[nodeIndex], 
                                                                                         GlobalStates.globalStates.readCountOfEachNode[nodeIndex]);
            }
        }
        else if (liveSeeds.size() > 1)
        {
            logger.info("rymInfo: Gathering the load statistic from the seed nodes {}", liveSeeds);
            StatesGatheringSignal signal = new StatesGatheringSignal(true);
            for(InetAddressAndPort seed : liveSeeds)
            {
                if(seed.equals(FBUtilities.getBroadcastAddressAndPort()))
                {
                    continue;
                }
                StorageService.instance.stateGatheringSignalInFlight.incrementAndGet();
                signal.sendStatesGatheringSignal(seed);
                logger.info("rymInfo: send the signal to the seed node {}, the stateGatheringSignalInFlight is {}", seed, StorageService.instance.stateGatheringSignalInFlight.get());
            }
        }
        else
        {
            StatesGatheringSignal signal = new StatesGatheringSignal(false);
            for(InetAddressAndPort follower : Gossiper.instance.getLiveMembers())
            {
                if(follower.equals(FBUtilities.getBroadcastAddressAndPort()))
                {
                    continue;
                }
                StorageService.instance.stateGatheringSignalInFlight.incrementAndGet();
                signal.sendStatesGatheringSignal(follower);
            }
        }

        // Wait until we get all the states

        int retryCount = 0;
        while(StorageService.instance.stateGatheringSignalInFlight.get() != 0)
        {
            try {
                Thread.sleep(1000);
                retryCount++;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(retryCount >= 5)
            {
                logger.warn("rymWARN: we have waited for 5s, but we still have {} states gathering signal in flight, so we stop this scheduling.", 
                                StorageService.instance.stateGatheringSignalInFlight.get());
                StorageService.instance.stateGatheringSignalInFlight.set(0);
                return;
            }
        }

        // update the readCountOfEachRg
        for(int i = 0; i < GlobalStates.globalStates.nodeCount; i++)
        {
            for(int j = 0; j < GlobalStates.rf; j++)
            {
                // int rgIndex = HorseUtils.getReplicaIndexForRGInEachNode(i, i + j);
                int rgIndex = (i - j + GlobalStates.globalStates.nodeCount) % GlobalStates.globalStates.nodeCount;
                GlobalStates.globalStates.updatingReadCountOfEachRG[rgIndex] += GlobalStates.globalStates.loadMatrix[i][j];
            }
        }
        
        GlobalStates.globalStates.readCountOfEachNode = Arrays.copyOf(GlobalStates.globalStates.updatingReadCountOfEachRG, GlobalStates.globalStates.nodeCount);
        GlobalStates.globalStates.updatingReadCountOfEachRG = new int[GlobalStates.globalStates.nodeCount];

        logger.info(ANSI_RED + "rymInfo: we now have the global states, request count vector is {}, score vector is {}, the load matrix is {}"+ ANSI_RESET,
                    //  GlobalStates.globalStates.latencyVector, 
                     GlobalStates.globalStates.readCountOfEachNode, 
                     GlobalStates.globalStates.scoreVector, 
                     GlobalStates.globalStates.loadMatrix);

    }



    
    public static Runnable getPrintStatisticRunnable()
    {
        return new PrintStatisticRunnable();
    }

    private static class PrintStatisticRunnable implements Runnable
    {

        @Override
        public void run() {
            logger.info("rymInfo: The Flush rate is {} mb/s, the compaction rate is {} mb/s, read request in flight is {}, the get endpoint cost is {} us, foreground load {}, local read latency: {}, local read count: {}, local write latency: {}, local write count: {}", 
                        StorageService.instance.flushRateMonitor.getRateInMB() * 3,
                        // StorageService.instance.coordinatorReadRateMonitor.getRateInMB(),
                        // StorageService.instance.localReadRateMonitor.getRateInMB(),
                        StorageService.instance.compactionRateMonitor.getRateInMB(),
                        StorageService.instance.readRequestInFlight.get(),
                        // StorageService.instance.pendingFlushRate.getRate(),
                        StorageService.instance.getEndpointCost.get() / 1000,
                        StorageService.instance.totalReadCntOfEachReplica, 
                        StorageService.instance.localReadLatencyCalculator.getLatencyForLocalStates(),
                        StorageService.instance.localReadLatencyCalculator.getCount(), 
                        StorageService.instance.writeLatencyCalculator.getLatencyForLocalStates(), 
                        StorageService.instance.writeLatencyCalculator.getCount());        
        }
        
    }

    public static void main(String[] args) {
        double value = 0.123456789;
        System.out.println(HorseUtils.rounding(value, 2));
    }

}

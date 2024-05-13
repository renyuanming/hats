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


import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.horse.leaderelection.election.ElectionBootstrap;
import org.apache.cassandra.horse.leaderelection.priorityelection.PriorityElectionBootstrap;
import org.apache.cassandra.locator.InetAddressAndPort;
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

                // Shutdown the current election scheme
                ElectionBootstrap.shutdownElection(liveSeeds);
                // Start a new priority election scheme
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

            // Step2. Gather the load statistic
            gatheringLoadStatistic();
        }
        
    }

    public static void gatheringLoadStatistic()
    {
        // Check if this node is the leader
        if (ElectionBootstrap.isLeader() || PriorityElectionBootstrap.isLeader())
        {
            logger.debug("rymDebug: Node {} is the leader. Start the scheduler.", FBUtilities.getBroadcastAddressAndPort());
            // If this node is the leader, gathering the load statistic and  check load change happens
            /*
                * Gather the load statistic has three cases:
                * 1. If we have multiple live seed nodes, we gathering the load statistic from these seed nodes
                * 2. If we have only one live seed node, we gathering the load statistic locally
                * 3. If we have no live seed node, we gathering the load statistic from all live members
                */

            if(liveSeeds.size() == 1)
            {
                for (Map.Entry<InetAddressAndPort, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
                {
                    String foregroundLoad = entry.getValue().getApplicationState(ApplicationState.FOREGROUND_LOAD).value;
                    logger.debug("rymDebug: Node {} has load statistic: {}", 
                                 entry.getKey(), 
                                 foregroundLoad);

                    int index = Gossiper.getAllHosts().indexOf(entry.getKey());
                }
            }
        }
        else if (ElectionBootstrap.isStarted() || PriorityElectionBootstrap.isStarted())
        {
            // Followers send the load statistic to the leader
            if(liveSeeds.size() > 1)
            {
                // If we have multiple live seed nodes, seed nodes send the load statistic to the leader
            }
            else if (liveSeeds.size() == 0)
            {
                // If we have no live seed nodes, all the node send the load statistic to the leader

            }
            else
            {
                // If we only have one seed nodes, we do nothing
                logger.debug("As we only has one seed node, followers do nothing.");
            }
            logger.debug("rymDebug: Node {} is NOT the leader. Exit the scheduler.", FBUtilities.getBroadcastAddressAndPort());
        }
        else
        {
            logger.debug("rymDebug: Node {} is neither the leader node, nor the follower nodes, we do not need to gathering states from it.", FBUtilities.getBroadcastAddressAndPort());
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

            // Check if this node is the leader
            if (ElectionBootstrap.isLeader() || PriorityElectionBootstrap.isLeader())
            {
                logger.debug("rymDebug: Node {} is the leader. Start the scheduler.", FBUtilities.getBroadcastAddressAndPort());

                // Recalculate the placement policy if needed.

                // Distribute the placement policy to all nodes and clients
            }
        }
    }
}

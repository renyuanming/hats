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

package org.apache.cassandra.adaptivekv;

import org.apache.cassandra.adaptivekv.leaderelection.election.ElectionBootstrap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Scheduler {

    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);
    private static Boolean isInitElectionBootstrap = false;


    public static Runnable getSchedulerRunnable()
    {
        return new SchedulerRunnable();
    }

    private static class SchedulerRunnable implements Runnable
    {
        @Override
        public void run()
        {

            // Check if there is lived seed node, if not, start a new election scheme
            if (!getIsInitElectionBootstrap() && !Gossiper.instance.seenAnySeed() && !Gossiper.instance.getLiveMembers().isEmpty())
            {
                logger.debug("rymDebug: No seed node is alive, but there are live members: {}. Start new election scheme.", Gossiper.instance.getLiveMembers());
                setIsInitElectionBootstrap(true);
                ElectionBootstrap.initElection(AKUtils.getRaftLogPath(), 
                                        "ElectDataNodes", 
                                        DatabaseDescriptor.getListenAddress().getHostAddress()+":"+DatabaseDescriptor.getRaftPort(), 
                                        AKUtils.InetAddressAndPortSetToString(Gossiper.instance.getLiveMembers(), DatabaseDescriptor.getRaftPort()));
            }
            else
            {
                logger.debug("rymDebug: IsInitElectionBootstrap: {}, seenAnySeed: {}, liveMembers: {}, seed nodes are: {}", getIsInitElectionBootstrap(), Gossiper.instance.seenAnySeed(), Gossiper.instance.getLiveMembers(), Gossiper.instance.getSeeds());
            }

            // Check if this node is the leader
            if (ElectionBootstrap.isLeader())
            {
                logger.debug("rymDebug: Node {} is the leader. Start the scheduler.", FBUtilities.getBroadcastAddressAndPort());
                // If this node is the leader, gathering the load statistic and  check load change happens

                // If load change happens, trigger the placement algorithm

                // Distribute the placement policy to all nodes and clients
            }
            else
            {
                logger.debug("rymDebug: Node {} is NOT the leader. Exit the scheduler.", FBUtilities.getBroadcastAddressAndPort());
            }
        }
    }

    public static Boolean getIsInitElectionBootstrap()
    {
        return isInitElectionBootstrap;
    }

    public static void setIsInitElectionBootstrap(Boolean isInitElectionBootstrap)
    {
        Scheduler.isInitElectionBootstrap = isInitElectionBootstrap;
    }
}

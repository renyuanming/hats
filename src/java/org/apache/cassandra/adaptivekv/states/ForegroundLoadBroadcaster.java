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

package org.apache.cassandra.adaptivekv.states;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.gms.*;


// import static org.apache.cassandra.config.CassandraRelevantProperties.BROADCAST_INTERVAL_MS;

public class ForegroundLoadBroadcaster implements IEndpointStateChangeSubscriber
{
    static final int BROADCAST_INTERVAL_MS = 10000;

    public static final ForegroundLoadBroadcaster instance = new ForegroundLoadBroadcaster();

    private static final Logger logger = LoggerFactory.getLogger(ForegroundLoadBroadcaster.class);

    private ConcurrentMap<InetAddressAndPort, String> foregroundLoadInfo = new ConcurrentHashMap<>();

    private ForegroundLoadBroadcaster()
    {
        Gossiper.instance.register(this);
    }

    public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
        if (state != ApplicationState.FOREGROUND_LOAD)
            return;
        foregroundLoadInfo.put(endpoint, value.value);
    }

    public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
    {
        VersionedValue localValue = epState.getApplicationState(ApplicationState.FOREGROUND_LOAD);
        if (localValue != null)
        {
            onChange(endpoint, ApplicationState.FOREGROUND_LOAD, localValue);
        }
    }

    public void onRemove(InetAddressAndPort endpoint)
    {
        foregroundLoadInfo.remove(endpoint);
    }

    // public Map<InetAddressAndPort, Double> getLoadInfo()
    // {
    //     return Collections.unmodifiableMap(loadInfo);
    // }

    public void startBroadcasting()
    {
        // send the first broadcast "right away" (i.e., in 2 gossip heartbeats, when we should have someone to talk to);
        // after that send every BROADCAST_INTERVAL.
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                if (!Gossiper.instance.isEnabled())
                    return;
                if (logger.isTraceEnabled())
                    logger.trace("Disseminating load info ...");
                logger.debug("rymDebug: foreground load {}, total read count: {}",StorageService.instance.readCountOfEachReplicaGroup, StorageService.instance.totalReadCcount.get());
                
                // Gossiper.instance.addLocalApplicationState(ApplicationState.FOREGROUND_LOAD,
                //                                            StorageService.instance.valueFactory.foregroundLoad(String.valueOf(StorageService.instance.totalReadCcount.get())));
                Gossiper.instance.addLocalApplicationState(ApplicationState.FOREGROUND_LOAD,
                                                           StorageService.instance.valueFactory.foregroundLoad(StorageService.instance.readCountOfEachReplicaGroup));
            }
        };
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(runnable, 2 * Gossiper.intervalInMillis, BROADCAST_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
}


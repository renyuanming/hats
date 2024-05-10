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
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AKUtils {

    private static final Logger logger = LoggerFactory.getLogger(AKUtils.class);
    private static final String raftLogPath = System.getProperty("user.dir") + "/log/raft.log";

    public enum AKLogLevels {
        TRACE, 
        DEBUG, 
        INFO, 
        WARN, 
        ERROR
    }

    public static String getRaftLogPath()
    {
        return raftLogPath;
    }

    public static void printStackTace(AKLogLevels logLevel, String msg) 
    {
        if (logLevel.equals(AKLogLevels.DEBUG))
            logger.debug("stack trace {}", new Exception(msg));
        if (logLevel.equals(AKLogLevels.ERROR))
            logger.error("stack trace {}", new Exception(msg));
        if (logLevel.equals(AKLogLevels.INFO))
            logger.info("stack trace {}", new Exception(msg));
    }

    public static class TimeCounter {
        private int cnt = 0;
        private Map<Long, Integer> history = new TreeMap<>();
        private Timer timer = new Timer();

        public TimeCounter(int seconds) {
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    saveAndReset();
                }
            }, seconds * 1000, seconds * 1000);
        }

        public synchronized void increment() {
            cnt++;
        }

        private synchronized void saveAndReset() {
            history.put(System.currentTimeMillis() / 1000, cnt);
            cnt = 0;
        }

        public synchronized Map<Long, Integer> getHistory() {
            return new TreeMap<>(history);
        }

        public void stop() {
            timer.cancel();
        }
    }
    

    public static String InetAddressAndPortSetToString(Set<InetAddressAndPort> from, int port, Set<InetAddressAndPort> liveSeeds)
    {
        String to = "";
        
        for (int i = 0; i < from.size(); i++)
        {
            InetAddressAndPort ip = (InetAddressAndPort) from.toArray()[i];
            if(liveSeeds.contains(ip))
                to += ip.getHostName() + ":" + port + "::" + 200;
            else
                to += ip.getHostName() + ":" + port + "::" + 100;
            if (i < from.size() - 1)
                to += ",";
        }
        return to;
        // return from.stream().map(ip -> ip.getHostName() + ":" + port + "::" + priority).collect(Collectors.joining(","));
    }

    public static void forceDelete(File path) {
        try {
            logger.debug("rymDebug: Deleting file {}", path);
            FileUtils.forceDelete(path);
        } catch (final IOException e) {
            logger.error("Fail to delete file {}.", path);
        }
    }

    public static class ReplicaRequestCounter
    {
        private final long intervalMillis;
        private final ConcurrentHashMap<InetAddress, ConcurrentLinkedQueue<Long>> requestsPerReplica;
    
        public ReplicaRequestCounter(long intervalMillis) {
            this.intervalMillis = intervalMillis;
            this.requestsPerReplica = new ConcurrentHashMap<>();
        }
    
        public synchronized void mark(InetAddress ip) {
            long currentTime = System.currentTimeMillis();
            ConcurrentLinkedQueue<Long> timestamps = requestsPerReplica.computeIfAbsent(ip, k -> new ConcurrentLinkedQueue<>());
            timestamps.add(currentTime);
            cleanupOldRequests(ip);
        }
    
        public int getCount(InetAddress ip) {
            cleanupOldRequests(ip);
            ConcurrentLinkedQueue<Long> timestamps = requestsPerReplica.get(ip);
            return timestamps != null ? timestamps.size() : 0;
        }
    
        private void cleanupOldRequests(InetAddress ip) {
            Queue<Long> timestamps = requestsPerReplica.get(ip);
            if (timestamps != null) {
                long cutoffTime = System.currentTimeMillis() - this.intervalMillis;
                while (!timestamps.isEmpty() && timestamps.peek() < cutoffTime) {
                    timestamps.poll();
                }
            }
        }

        public ConcurrentHashMap<InetAddress, ConcurrentLinkedQueue<Long>>  getCounter() {
            return requestsPerReplica;
        }
    }

}

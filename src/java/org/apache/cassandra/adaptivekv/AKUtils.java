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
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

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
    

}

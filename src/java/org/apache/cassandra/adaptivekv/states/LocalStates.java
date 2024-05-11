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


import java.net.InetAddress;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.adaptivekv.AKUtils.ReplicaRequestCounter;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalStates {
    private static final Logger logger = LoggerFactory.getLogger(LocalStates.class);
    private static double localEWMAReadLatency = 0.0; // micro second
    private static double localEWMAWriteLatency = 0.0; // micro second
    private static final double ALPHA = 0.9;
    public final ReplicaRequestCounter readCounter;
    public double latency = 0.0; // micro second

    public LocalStates(ReplicaRequestCounter readCounter)
    {
        this.readCounter = readCounter;
        this.latency = DatabaseDescriptor.getReadSensitiveFactor() * localEWMAReadLatency + (1 - DatabaseDescriptor.getReadSensitiveFactor()) * localEWMAWriteLatency;
    }

    public static synchronized void recordEWMALocalReadLatency(long localReadLatency) {
        long latencyInMicros = localReadLatency / 1000;
        // logger.debug("rymDebug: record the read latency: {} ns, {} us, EWMA latency is {}", localReadLatency, latencyInMicros, localEWMAReadLatency);
        localEWMAReadLatency = getEWMA(latencyInMicros, localEWMAReadLatency);
    }

    public static synchronized void recordEWMALocalWriteLatency(long localWriteLatency) {
        long latencyInMicros = localWriteLatency / 1000;
        // logger.debug("rymDebug: record the write latency: {} ns, {} us, EWMA latency is {}", localWriteLatency, latencyInMicros, localEWMAWriteLatency);
        localEWMAWriteLatency = getEWMA(latencyInMicros, localEWMAWriteLatency);
    }

    public static double getEWMA(double newValue, double ewmaValue)
    {
        return ALPHA * newValue + (1 - ALPHA) * ewmaValue;
    }

    public double getEWMALocalReadLatency() {
        return localEWMAReadLatency;
    }

    public double getEWMALocalWriteLatency() {
        return localEWMAWriteLatency;
    }

    public String toString()
    {
        String requests = "";

        for (InetAddress ip : readCounter.getCounter().keySet())
        {
            requests += ip.getHostName() + ": " + readCounter.getCount(ip) + ",";
            // requests += ip.getHostName() + ": " + readCounter.getCounter().get(ip).size() + ",";
        }
        return String.format("LocalStates{Latency=%f, Requests=%s}", latency, requests);
    }

    public static class LatencyCalculator
    {
        private final ConcurrentLinkedQueue<Double> producer = new ConcurrentLinkedQueue<>();
        private final AtomicReference<Double> ewmaValue = new AtomicReference<>(0.0);
        private final AtomicBoolean running = new AtomicBoolean(true);
        private Thread workerThread;

        public LatencyCalculator() {
            startWorker();
        }

        private void startWorker() {
            workerThread = new Thread(() -> {
                while (running.get()) {
                    computeEWMA();
                    try {
                        Thread.sleep(1000); // Calculate every second
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            workerThread.start();
        }

        private void computeEWMA() {
            while (producer.size() > 0) {
                double currentValue = producer.poll();
                ewmaValue.updateAndGet(prev -> prev == 0.0 ? currentValue : ALPHA * currentValue + (1 - ALPHA) * prev);
            }
        }

        public void record(double latency) {
            producer.add(latency);
        }

        public double getEWMA() {
            return ewmaValue.get();
        }

        public void stop() {
            running.set(false);
            workerThread.interrupt();
        }

        @Override
        protected void finalize() throws Throwable {
            stop();
            super.finalize();
        }
        }

}

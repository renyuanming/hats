/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HorseUtils 
{
    private static final Logger logger = LoggerFactory.getLogger(HorseUtils.class);

    public enum HorseLogLevels 
    {
        TRACE, 
        DEBUG, 
        INFO, 
        WARN, 
        ERROR
    }

    public static void printStackTace(HorseLogLevels logLevel, String msg) 
    {
        if (logLevel.equals(HorseLogLevels.DEBUG))
            logger.debug("stack trace {}", new Exception(msg));
        if (logLevel.equals(HorseLogLevels.ERROR))
            logger.error("stack trace {}", new Exception(msg));
        if (logLevel.equals(HorseLogLevels.INFO))
            logger.info("stack trace {}", new Exception(msg));
    }

    public static class ByteObjectConversion 
    {
        public static byte[] objectToByteArray(Serializable obj) throws IOException 
        {
            logger.debug("HORSE-Debug: start to transform");
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            oos.close();
            bos.close();
            return bos.toByteArray();
        }

        public static Object byteArrayToObject(byte[] bytes) throws Exception 
        {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            Object obj = ois.readObject();
            bis.close();
            ois.close();
            return obj;
        }
    }

    public static class HorseReplicaSelector 
    {
        private final List<Host> targets;
        private final double[] cumulativeWeights;
        private final AtomicLong[] selectionCounts;
        private final Random random;

        public HorseReplicaSelector(List<Host> targets, List<Double> ratios) 
        {
            if (targets.size() != ratios.size()) 
            {
                throw new IllegalArgumentException("Targets and ratios must have the same length.");
            }
            this.targets = targets;
            this.cumulativeWeights = new double[ratios.size()];
            this.selectionCounts = new AtomicLong[targets.size()];
            for (int i = 0; i < targets.size(); i++) 
            {
                selectionCounts[i] = new AtomicLong(0);
                cumulativeWeights[i] = (i == 0 ? 0 : cumulativeWeights[i - 1]) + ratios.get(i);
            }
            this.random = new Random();
        }

        public Host selectTarget() 
        {
            double rand = random.nextDouble();
            int targetIndex = Arrays.binarySearch(cumulativeWeights, rand);
            if (targetIndex < 0) 
            {
                targetIndex = -targetIndex - 1;
            }
            selectionCounts[targetIndex].incrementAndGet();
            return targets.get(targetIndex);
        }

        public long[] getSelectionCounts() 
        {
            return Arrays.stream(selectionCounts).mapToLong(AtomicLong::get).toArray();
        }
    }

    public static void main(String[] args) throws UnknownHostException, InterruptedException 
    {
        List<Host> targets = IntStream.range(0, 3)
                .mapToObj(i -> {
                    return new Host(InetSocketAddress.createUnresolved("192.168.1." + (i + 1), 7000), new ConvictionPolicy.DefaultConvictionPolicy.Factory(), null);
                })
                .collect(Collectors.toList());
        // double[] ratios = {0.7, 0.2, 0.1};
        List<Double> ratios = Arrays.asList(0.7, 0.2, 0.1);
        HorseReplicaSelector selector = new HorseReplicaSelector(targets, ratios);

        // Create thread pool to simulate concurrent selection
        ExecutorService executor = Executors.newFixedThreadPool(10);
        int totalSelections = 1000;

        for (int i = 0; i < totalSelections; i++) 
        {
            executor.submit(selector::selectTarget);
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        // Print the results
        long[] selectionCounts = selector.getSelectionCounts();
        for (int i = 0; i < selectionCounts.length; i++) 
        {
            System.out.printf("Target %s: %d selections (%.2f%%)%n", targets.get(i), selectionCounts[i], selectionCounts[i] * 100.0 / totalSelections);
        }
    }

}

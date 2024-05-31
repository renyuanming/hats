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

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author renyuanming1@gmail.com
 */

public class RateLimiter 
{
    private final ConcurrentHashMap<Integer, Integer> targetRatios; // 目标比例
    private final ConcurrentHashMap<Integer, AtomicInteger> servedCounts; // 实际服务计数
    private final ConcurrentHashMap<Integer, AtomicInteger> receivedCounts; // 接收任务计数
    private final int totalTasks; // 任务类型总数
    private final AtomicInteger totalServed; // 已服务的任务总数
    private final AtomicInteger totalReceived; // 接收的任务总数
    private final Random random;
    private final double targetTotalRatio;

    public RateLimiter(int[] targetRatiosArray) {
        this.targetRatios = new ConcurrentHashMap<>();
        this.servedCounts = new ConcurrentHashMap<>();
        this.receivedCounts = new ConcurrentHashMap<>();
        this.totalTasks = targetRatiosArray.length;
        this.totalServed = new AtomicInteger(0);
        this.totalReceived = new AtomicInteger(0);
        this.random = new Random();
        this.targetTotalRatio = 1.0;

        for (int i = 0; i < targetRatiosArray.length; i++) {
            targetRatios.put(i, targetRatiosArray[i]);
            servedCounts.put(i, new AtomicInteger(0));
            receivedCounts.put(i, new AtomicInteger(0));
        }
    }

    public void receiveTask(int taskType) {
        // 更新接收的任务计数
        receivedCounts.get(taskType).incrementAndGet();
        totalReceived.incrementAndGet();

        // 根据当前实际服务比例和目标比例决定是否服务该任务
        if (shouldServeTask(taskType)) {
            serveTask(taskType);
        }
    }

    private boolean shouldServeTask(int taskType) {
        if (totalServed.get() == 0) {
            return true;
        }


        // TODO: check if the rate is below the target rate
        if(totalServed.get() *1.0 / totalReceived.get() < targetTotalRatio)
        {
            return true;
        }

        // 使用整数计算服务比例，避免浮点运算
        int currentRatio = servedCounts.get(taskType).get() * 100;
        int targetRatio = targetRatios.get(taskType) * totalServed.get();

        // 动态调整阈值，允许一定范围内的误差
        int allowance = (int) (0.1 * targetRatios.get(taskType) * totalReceived.get());
        return currentRatio < (targetRatio + allowance);
    }

    private void serveTask(int taskType) {
        servedCounts.get(taskType).incrementAndGet();
        totalServed.incrementAndGet();
        // System.out.println("Served task type: " + taskType);
    }
    public void reset() {
        this.totalServed.set(0);
        this.totalReceived.set(0);
        for (int i = 0; i < totalTasks; i++) {
            this.servedCounts.get(i).set(0);
            this.receivedCounts.get(i).set(0);
        }
    }

    public static void main(String[] args) {
        int[] targetRatios = {80, 10, 10}; // 目标比例
        RateLimiter rateLimiter = new RateLimiter(targetRatios);

        // 测试用例1：随机选择任务
        System.out.println("Test case 1: Random task selection");
        runRandomTaskSelectionTest(rateLimiter);

        // 重置计数器
        rateLimiter.reset();

        // 测试用例2：模拟type 0被耗尽的情况，发送率{0.1, 0.45, 0.45}
        System.out.println("Test case 2: Simulate task type 0 exhaustion");
        runSimulatedTaskDistributionTest(rateLimiter);
    }

    private static void runRandomTaskSelectionTest(RateLimiter rateLimiter) {
        int numThreads = 10;
        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            threads[t] = new Thread(() -> {
                for (int i = 0; i < 100; i++) {
                    int taskType = rateLimiter.random.nextInt(3);
                    rateLimiter.receiveTask(taskType);
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        printFinalServedCounts(rateLimiter);
    }

    private static void runSimulatedTaskDistributionTest(RateLimiter rateLimiter) {
        int numThreads = 10;
        double[] probabilities = {0.0, 0.45, 0.45};
        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            threads[t] = new Thread(() -> {
                for (int i = 0; i < 100; i++) {
                    int taskType = getTaskTypeByProbability(probabilities, rateLimiter.random);
                    rateLimiter.receiveTask(taskType);
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        printFinalServedCounts(rateLimiter);
    }

    private static int getTaskTypeByProbability(double[] probabilities, Random random) {
        double p = random.nextDouble();
        double cumulativeProbability = 0.0;
        for (int i = 0; i < probabilities.length; i++) {
            cumulativeProbability += probabilities[i];
            if (p <= cumulativeProbability) {
                return i;
            }
        }
        return probabilities.length - 1; // Fallback in case of rounding errors
    }

    private static void printFinalServedCounts(RateLimiter rateLimiter) {
        System.out.println("Final served counts:");
        for (int i = 0; i < rateLimiter.totalTasks; i++) {
            System.out.println("Task type " + i + ": " + rateLimiter.servedCounts.get(i).get() + "/" + rateLimiter.receivedCounts.get(i).get());
        }
    }
}
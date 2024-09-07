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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LoadBalancer {

    public static double[][] balanceLoad(int N, int R, int W, double[] L, double[] T, double[][] C, double[] lambda) {

        double[][] readRatio = new double[N][R];
        double[] actualCountOfEachNode = new double[N];
        double[] actualCountOfEachReplicationGroup = new double[N];

        // initialize actualCountOfEachNode based on C
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < R; j++) {
                actualCountOfEachNode[i] += C[i][j];
            }
        }

        // Traverse all nodes
        for (int i = 0; i < N; i++) {
            // Calculate the mean latency of the replication group
            double localAvgLatency = 0;
            for (int j = i; j < i + R; j++) {
                localAvgLatency += L[j % N];
            }
            localAvgLatency /= R;

            // Calculate the estimated request count of the R nodes
            double[] localEstimateCount = new double[R];
            for (int j = i; j < i + R; j++) {
                localEstimateCount[j - i] = lambda[j % N] * W / localAvgLatency;
                // localEstimateCount[j - i] = W / localAvgLatency;
            }

            // For replication group i, update C[i][i,...,i+R-1] to minimize the difference between the estimated request count and the actual request count
            double[] localDelta = new double[R];
            for (int j = i; j < i + R; j++) {
                localDelta[j - i] = localEstimateCount[j - i] - actualCountOfEachNode[j % N];
                // localDelta[j - i] = actualCountOfEachNode[i] - localEstimateCount[j - i];
            }

            // Update the request count of the R nodes

            // Get the indices of the nodes with positive and negative delta
            List<Integer> positiveIndices = new ArrayList<>(); // overloaded nodes
            List<Integer> negativeIndices = new ArrayList<>(); // light load nodes

            for (int j = 0; j < R; j++) {
                if (localDelta[j] > 0) {
                    positiveIndices.add(j); // positive value: overloaded nodes
                } else if (localDelta[j] < 0) {
                    negativeIndices.add(j); // negative value: light load nodes
                }
            }

            // move requests from overloaded nodes to light load nodes
            for (int pos : positiveIndices) {
                int nodeIndexPos = (i + pos) % N; 

                for (int neg : negativeIndices) {
                    int nodeIndexNeg = (i + neg) % N; 

                   
                    double adjustment = Math.min(localDelta[pos], -localDelta[neg]);
                    adjustment = Math.min(adjustment, C[nodeIndexPos][pos]);

                    C[nodeIndexPos][pos] -= adjustment; 
                    C[nodeIndexNeg][neg] += adjustment; 

                    localDelta[pos] -= adjustment;
                    localDelta[neg] += adjustment;

                    if (localDelta[neg] == 0) {
                        break;
                    }
                }
            }

            // update actualCountOfEachNode
            for (int j = 0; j < R; j++) {
                int nodeIndex = (i + j) % N;
                actualCountOfEachNode[nodeIndex] = 0;

                // update actualCountOfEachNode
                for (int k = 0; k < R; k++) {
                    actualCountOfEachNode[nodeIndex] += C[nodeIndex][k];
                }

                // update estimate latency L[i]
                L[nodeIndex] = lambda[nodeIndex] * W / actualCountOfEachNode[nodeIndex];
                // L[nodeIndex] =  W / actualCountOfEachNode[nodeIndex];
                actualCountOfEachReplicationGroup[i] += C[nodeIndex][j];
            }
        }
        // Calculate the read ratio of each replication group
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < R; j++) {
                readRatio[i][j] = C[i][j] / actualCountOfEachReplicationGroup[(i - j + N) % N];
            }
        }

        return readRatio;
    }

    public static void main(String[] args) {
        int N = 10; // node number
        int R = 3; // replication factor

        int W = 60; // time window size, 60 seconds

        double[] L = {100, 105, 90, 180, 200, 85, 150, 130, 120, 115}; // average latency of each node
        for (int i = 0; i < N; i++) {
            L[i] = L[i] / 1000000; // convert to seconds
        }
        double[] T = new double[N]; // service rate of each node T_i = W / (L_i * 1e-6)
        for (int i = 0; i < N; i++) {
            T[i] = W / L[i];
        }

        // Request count of each replication group on each node
        // double[][] C = {
        //     {100000, 0, 0},
        //     {100000, 0, 0},
        //     {100000, 0, 0},
        //     {100000, 0, 0},
        //     {100000, 0, 0},
        //     {100000, 0, 0},
        //     {100000, 0, 0},
        //     {100000, 0, 0},
        //     {100000, 0, 0},
        //     {100000, 0, 0}
        // };

        double[][] C = {
            {100000, 0, 0},
            {100000, 0, 0},
            {50000, 0, 0},
            {100000, 20000, 0},
            {100000, 0, 30000},
            {100000, 0, 0},
            {100000, 0, 0},
            {100000, 0, 0},
            {100000, 0, 0},
            {100000, 0, 0}
        };

        double[] lambda = new double[N]; // concurrency factor of each node lambda_i = C_i / T_i
        for (int i = 0; i < N; i++) {
            lambda[i] = C[i][0] / T[i];
            // lambda[i] = 32;
        }

        // print the transposed request count matrix
        System.out.println("Transposed request count matrix:");
        for (int i = 0; i < R; i++) {
            for (int j = 0; j < N; j++) {
                System.out.printf("%10.2f ", C[j][i]);
            }
            System.out.println();
        }
        // print the average latency, service rate, and concurrency factor of each node
        System.out.println("Average latency of each node:");
        System.out.println(Arrays.toString(L));
        System.out.println("Service rate of each node:");
        System.out.println(Arrays.toString(T));
        System.out.println("Concurrency factor of each node:");
        System.out.println(Arrays.toString(lambda));

        double[][] readRatio = balanceLoad(N, R, W, L, T, C, lambda);

        // Print the transposed request count matrix
        System.out.println("Transposed request count matrix:");
        for (int i = 0; i < R; i++) {
            for (int j = 0; j < N; j++) {
                System.out.printf("%10.2f ", C[j][i]);
            }
            System.out.println();
        }

        // Print the transposed read ratio matrix
        System.out.println("Transposed read ratio matrix:");
        for (int i = 0; i < R; i++) {
            for (int j = 0; j < N; j++) {
                System.out.printf("%10.2f ", readRatio[j][i]);
            }
            System.out.println();
        }
    }
}

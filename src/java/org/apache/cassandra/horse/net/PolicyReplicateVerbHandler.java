/**
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
package org.apache.cassandra.horse.net;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.horse.HorseUtils;
import org.apache.cassandra.horse.HorseUtils.ByteObjectConversion;
import org.apache.cassandra.horse.states.GlobalStates;
import org.apache.cassandra.horse.states.LocalStates;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolicyReplicateVerbHandler implements IVerbHandler<PolicyReplicate>{

    public static final PolicyReplicateVerbHandler instance = new PolicyReplicateVerbHandler();
    private static final Logger logger = LoggerFactory.getLogger(PolicyReplicateVerbHandler.class);
    @Override
    public void doVerb(Message<PolicyReplicate> message) throws IOException {
        PolicyReplicate payload = message.payload;

        try {
            GlobalStates.globalPolicy = (Double[][][]) ByteObjectConversion.byteArrayToObject(payload.placementPolicyInBytes);
            // Get the placement policy for local replicas
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // Get the local placement policy
        int nodeIndex = Gossiper.getAllHosts().indexOf(FBUtilities.getBroadcastAddressAndPort());
        int nodeCount = Gossiper.getAllHosts().size();
        for(int i = nodeIndex; i > nodeIndex - 3; i--)
        {
            int rgIndex = (i + nodeCount) % nodeCount;
            List<Double> policy = new ArrayList<>();
            InetAddressAndPort rg = Gossiper.getAllHosts().get(rgIndex);
            for(int curNodeIndex = rgIndex; curNodeIndex < rgIndex + 3; curNodeIndex++)
            {
                int replicaIndex = HorseUtils.getReplicaIndexForRGInEachNode(rgIndex, curNodeIndex);
                policy.add(GlobalStates.globalPolicy[curNodeIndex][replicaIndex][0]);
            }
            LocalStates.localPolicy.put(rg, policy);
        }

        logger.info("rymInfo: We get the global placement policy {}, the local placement policy {}", GlobalStates.globalPolicy, LocalStates.localPolicy);
    }
    
}

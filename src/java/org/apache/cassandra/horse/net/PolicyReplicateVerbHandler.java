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

import org.apache.cassandra.horse.HorseUtils.ByteObjectConversion;
import org.apache.cassandra.horse.states.GlobalStates;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolicyReplicateVerbHandler implements IVerbHandler<PolicyReplicate>{

    public static final PolicyReplicateVerbHandler instance = new PolicyReplicateVerbHandler();
    private static final Logger logger = LoggerFactory.getLogger(PolicyReplicateVerbHandler.class);
    @Override
    public void doVerb(Message<PolicyReplicate> message) throws IOException {
        PolicyReplicate policy = message.payload;

        try {
            GlobalStates.placementPolicy = (Double[][][]) ByteObjectConversion.byteArrayToObject(policy.placementPolicyInBytes);
            // Get the placement policy for local replicas
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
}

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
import java.io.Serializable;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.horse.HorseUtils.ByteObjectConversion;
import org.apache.cassandra.horse.states.GlobalStates;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.db.TypeSizes.sizeof;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author renyuanming1@gmail.com
 */

public class PolicyReplicate 
{

    private static final Logger logger = LoggerFactory.getLogger(PolicyReplicate.class);
    public static final Serializer serializer = new Serializer();

    public final byte[] placementPolicyInBytes;
    private final int placementPolicyInBytesSize;
    public final byte[] backgroundPolicyInBytes;
    private final int backgroundPolicyInBytesSize;
    public final int expectedRequestNumber;
    // Map<InetAddress, LocalStates> states
    public PolicyReplicate(byte[] placementPolicyInBytes, byte[] backgroundPolicyInBytes, int expectedRequestNumber)
    {
        this.placementPolicyInBytes = placementPolicyInBytes;
        this.placementPolicyInBytesSize = placementPolicyInBytes.length;
        this.backgroundPolicyInBytes = backgroundPolicyInBytes;
        this.backgroundPolicyInBytesSize = backgroundPolicyInBytes.length;
        this.expectedRequestNumber = expectedRequestNumber;
    }

    public static void sendPlacementPolicy(InetAddressAndPort follower, Double[][] placementPolicy)
    {
        byte[] placementPolicyInBytes = null;
        byte[] backgroundPolicyInBytes = null;

        Double[] backgroundPolicy = GlobalStates.translatePolicyForBackgroundController(follower);
        int expectedRequestNumber = GlobalStates.globalStates.expectedRequestNumber[Gossiper.getAllHosts().indexOf(follower)];
                
        try {
            placementPolicyInBytes = ByteObjectConversion.objectToByteArray((Serializable) placementPolicy);
            backgroundPolicyInBytes = ByteObjectConversion.objectToByteArray((Serializable) backgroundPolicy);
            PolicyReplicate policy = new PolicyReplicate(placementPolicyInBytes, backgroundPolicyInBytes, expectedRequestNumber);
            Message<PolicyReplicate> message = Message.outWithFlag(Verb.POLICY_REPLICATE_REQ, policy, MessageFlag.CALL_BACK_ON_FAILURE);
            MessagingService.instance().send(message, follower);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static final class Serializer implements IVersionedSerializer<PolicyReplicate> 
    {

        @Override
        public void serialize(PolicyReplicate t, DataOutputPlus out, int version) throws IOException 
        {
            out.writeInt(t.placementPolicyInBytesSize);
            out.write(t.placementPolicyInBytes);
            out.writeInt(t.backgroundPolicyInBytesSize);
            out.write(t.backgroundPolicyInBytes);
            out.write(t.expectedRequestNumber);
        }

        @Override
        public PolicyReplicate deserialize(DataInputPlus in, int version) throws IOException 
        {
            int placementPolicyInBytesSize = in.readInt();
            byte[] placementPolicyInBytes = new byte[placementPolicyInBytesSize];
            in.readFully(placementPolicyInBytes);
            int backgroundPolicyInBytesSize = in.readInt();
            byte[] backgroundPolicyInBytes = new byte[backgroundPolicyInBytesSize];
            in.readFully(backgroundPolicyInBytes);
            int expectedRequestNumber = in.readInt();
            return new PolicyReplicate(placementPolicyInBytes, backgroundPolicyInBytes, expectedRequestNumber);
        }

        @Override
        public long serializedSize(PolicyReplicate t, int version) 
        {
            long size = t.placementPolicyInBytesSize + 
                        sizeof(t.placementPolicyInBytesSize) + 
                        t.backgroundPolicyInBytesSize +
                        sizeof(t.backgroundPolicyInBytesSize) +
                        sizeof(t.expectedRequestNumber);
            return size;
        }

    }
}

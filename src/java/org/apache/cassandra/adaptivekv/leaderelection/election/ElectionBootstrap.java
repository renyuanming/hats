/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.adaptivekv.leaderelection.election;


import java.io.File;

import org.apache.cassandra.adaptivekv.AKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.entity.PeerId;

/**
 *
 * @author jiachun.fjc
 */
public class ElectionBootstrap {

    // Start elections by 3 instance. Note that if multiple instances are started on the same machine,
    // the first parameter `dataPath` should not be the same.

    private static final Logger logger = LoggerFactory.getLogger(ElectionBootstrap.class);
    private static ElectionNode node;
    public static void initElection(String dataPath, String groupId, String serverIdStr, String initialConfStr)
    {
        logger.info("rymInfo: Starting election with dataPath: {}, groupId: {}, serverIdStr: {}, initialConfStr: {}",
                    dataPath, groupId, serverIdStr, initialConfStr);
        File dataPathFile = new File(dataPath);
        if(dataPathFile.exists())
        {
            AKUtils.forceDelete(dataPathFile);
        }
        node = new ElectionNode();
        final ElectionNodeOptions electionOpts = new ElectionNodeOptions();
        electionOpts.setDataPath(dataPath);
        electionOpts.setGroupId(groupId);
        electionOpts.setServerAddress(serverIdStr);
        electionOpts.setInitialServerAddressList(initialConfStr);

        node.addLeaderStateListener(new LeaderStateListener() {

            @Override
            public void onLeaderStart(long leaderTerm) {
                PeerId serverId = node.getNode().getLeaderId();
                String ip = serverId.getIp();
                int port = serverId.getPort();
                logger.debug("rymDebug: Leader's ip is: {}, port: {}", ip, port);
                logger.debug("rymDebug: Leader start on term: {}", leaderTerm);
            }

            @Override
            public void onLeaderStop(long leaderTerm) {
                logger.debug("rymDebug: Leader stop on term: {}", leaderTerm);
            }
        });
        node.init(electionOpts);
    }

    public static Boolean isLeader()
    {
        if (!node.isStarted())
        {
            logger.debug("rymDebug: Election node is not initialized");
            return false;
        }
        return node.isLeader();
    }


    public static void main(final String[] args) {
        if (args.length < 4) {
            System.out
                .println("Usage : java com.alipay.sofa.jraft.example.election.ElectionBootstrap {dataPath} {groupId} {serverId} {initConf}");
            System.out
                .println("Example: java com.alipay.sofa.jraft.example.election.ElectionBootstrap /tmp/server1 election_test 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
            System.exit(1);
        }
        final String dataPath = args[0];
        final String groupId = args[1];
        final String serverIdStr = args[2];
        final String initialConfStr = args[3];

        initElection(dataPath, groupId, serverIdStr, initialConfStr);
    }
}

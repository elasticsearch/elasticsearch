/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.discovery.file;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.env.Environment;
import org.elasticsearch.transport.TransportService;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An implementation of {@link UnicastHostsProvider} that reads hosts/ports
 * from {@link #UNICAST_HOSTS_FILE}.
 *
 * Each unicast host/port that is part of the discovery process must be listed on
 * a separate line.  If the port is left off an entry, a default port of 9300 is
 * assumed.  An example unicast hosts file could read:
 *
 * 67.81.244.10
 * 67.81.244.11:9305
 * 67.81.244.15:9400
 */
public class FileBasedUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {

    static final String UNICAST_HOSTS_FILE = "unicast_hosts.txt";
    static final String UNICAST_HOST_PREFIX = "#zen_file_unicast_host_";

    private final TransportService transportService;

    private final Path unicastHostsFilePath;

    private final AtomicLong nodeIdGenerator = new AtomicLong(); // generates unique ids for the node

    @Inject
    public FileBasedUnicastHostsProvider(Settings settings, TransportService transportService) {
        super(settings);
        this.transportService = transportService;
        this.unicastHostsFilePath = new Environment(settings).configFile().resolve("discovery-file").resolve(UNICAST_HOSTS_FILE);
    }

    @Override
    public List<DiscoveryNode> buildDynamicNodes() {
        List<String> hostsList;
        try {
            hostsList = Files.readAllLines(unicastHostsFilePath);
        } catch (FileNotFoundException | NoSuchFileException e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("[discovery-file] Failed to find unicast hosts file [{}]",
                                                                        unicastHostsFilePath), e);
            hostsList = Collections.emptyList();
        } catch (IOException e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("[discovery-file] Error reading unicast hosts file [{}]",
                                                                        unicastHostsFilePath), e);
            hostsList = Collections.emptyList();
        }

        final List<DiscoveryNode> discoNodes = new ArrayList<>();
        for (final String host : hostsList) {
            TransportAddress[] addresses;
            try {
                addresses = transportService.addressesFromString(host, 1);
            } catch (Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("[discovery-file] Failed to parse transport address from [{}]",
                                                                            host), e);
                continue;
            }
            discoNodes.add(new DiscoveryNode(UNICAST_HOST_PREFIX + nodeIdGenerator.incrementAndGet() + "#",
                                                addresses[0],
                                                Version.CURRENT.minimumCompatibilityVersion()));
        }

        logger.debug("[discovery-file] Using dynamic discovery nodes {}", discoNodes);

        return discoNodes;
    }

}

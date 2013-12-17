/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.test;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocators;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecidersModule;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

/**
 */
public class ElasticsearchAllocationTestCase extends ElasticsearchTestCase {

    public static AllocationService createAllocationService() {
        return createAllocationService(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public static AllocationService createAllocationService(Settings settings) {
        return createAllocationService(settings, getRandom())    ;
    }

    public static AllocationService createAllocationService(Settings settings, Random random) {
        return new AllocationService(settings,
                randomAllocationDeciders(settings, new NodeSettingsService(ImmutableSettings.Builder.EMPTY_SETTINGS), random),
                new ShardsAllocators(settings), ClusterInfoService.EMPTY);
    }

    public static AllocationDeciders randomAllocationDeciders(Settings settings, NodeSettingsService nodeSettingsService, Random random) {
        final ImmutableSet<Class<? extends AllocationDecider>> defaultAllocationDeciders = AllocationDecidersModule.DEFAULT_ALLOCATION_DECIDERS;
        final List<AllocationDecider> list = new ArrayList<AllocationDecider>();
        for (Class<? extends AllocationDecider> deciderClass : defaultAllocationDeciders) {
            try {
                try {
                    Constructor<? extends AllocationDecider> constructor = deciderClass.getConstructor(Settings.class, NodeSettingsService.class);
                    list.add(constructor.newInstance(settings, nodeSettingsService));
                } catch (NoSuchMethodException e) {
                    Constructor<? extends AllocationDecider> constructor = null;
                    constructor = deciderClass.getConstructor(Settings.class);
                    list.add(constructor.newInstance(settings));
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        assertThat(list.size(), equalTo(defaultAllocationDeciders.size()));
        for (AllocationDecider d : list) {
            assertThat(defaultAllocationDeciders.contains(d.getClass()), is(true));
        }
        Collections.shuffle(list, random);
        return new AllocationDeciders(settings, list.toArray(new AllocationDecider[0]));

    }
}

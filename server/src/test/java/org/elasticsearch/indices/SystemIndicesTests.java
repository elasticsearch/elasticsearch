/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.tasks.TaskResultsService.TASKS_FEATURE_NAME;
import static org.elasticsearch.tasks.TaskResultsService.TASK_INDEX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class SystemIndicesTests extends ESTestCase {

    public void testBasicOverlappingPatterns() {
        SystemIndexDescriptor broadPattern = new SystemIndexDescriptor(".a*c*", "test");
        SystemIndexDescriptor notOverlapping = new SystemIndexDescriptor(".bbbddd*", "test");
        SystemIndexDescriptor overlapping1 = new SystemIndexDescriptor(".ac*", "test");
        SystemIndexDescriptor overlapping2 = new SystemIndexDescriptor(".aaaabbbccc", "test");
        SystemIndexDescriptor overlapping3 = new SystemIndexDescriptor(".aaabb*cccddd*", "test");

        // These sources have fixed prefixes to make sure they sort in the same order, so that the error message is consistent
        // across tests
        String broadPatternSource = "AAA" + randomAlphaOfLength(5);
        String otherSource = "ZZZ" + randomAlphaOfLength(6);
        Map<String, SystemIndices.Feature> descriptors = new HashMap<>();
        descriptors.put(broadPatternSource, new SystemIndices.Feature("test feature", List.of(broadPattern)));
        descriptors.put(otherSource,
            new SystemIndices.Feature("test 2", List.of(notOverlapping, overlapping1, overlapping2, overlapping3)));

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> SystemIndices.checkForOverlappingPatterns(descriptors));
        assertThat(exception.getMessage(), containsString("a system index descriptor [" + broadPattern +
            "] from [" + broadPatternSource + "] overlaps with other system index descriptors:"));
        String fromPluginString = " from [" + otherSource + "]";
        assertThat(exception.getMessage(), containsString(overlapping1.toString() + fromPluginString));
        assertThat(exception.getMessage(), containsString(overlapping2.toString() + fromPluginString));
        assertThat(exception.getMessage(), containsString(overlapping3.toString() + fromPluginString));
        assertThat(exception.getMessage(), not(containsString(notOverlapping.toString())));

        IllegalStateException constructorException = expectThrows(IllegalStateException.class, () -> new SystemIndices(descriptors));
        assertThat(constructorException.getMessage(), equalTo(exception.getMessage()));
    }

    public void testComplexOverlappingPatterns() {
        // These patterns are slightly more complex to detect because pattern1 does not match pattern2 and vice versa
        SystemIndexDescriptor pattern1 = new SystemIndexDescriptor(".a*c", "test");
        SystemIndexDescriptor pattern2 = new SystemIndexDescriptor(".ab*", "test");

        // These sources have fixed prefixes to make sure they sort in the same order, so that the error message is consistent
        // across tests
        String source1 = "AAA" + randomAlphaOfLength(5);
        String source2 = "ZZZ" + randomAlphaOfLength(6);
        Map<String, SystemIndices.Feature> descriptors = new HashMap<>();
        descriptors.put(source1, new SystemIndices.Feature("test", List.of(pattern1)));
        descriptors.put(source2, new SystemIndices.Feature("test", List.of(pattern2)));

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> SystemIndices.checkForOverlappingPatterns(descriptors));
        assertThat(exception.getMessage(), containsString("a system index descriptor [" + pattern1 +
            "] from [" + source1 + "] overlaps with other system index descriptors:"));
        assertThat(exception.getMessage(), containsString(pattern2.toString() + " from [" + source2 + "]"));

        IllegalStateException constructorException = expectThrows(IllegalStateException.class, () -> new SystemIndices(descriptors));
        assertThat(constructorException.getMessage(), equalTo(exception.getMessage()));
    }

    public void testBuiltInSystemIndices() {
        SystemIndices systemIndices = new SystemIndices(Map.of());
        assertTrue(systemIndices.isSystemIndex(".tasks"));
        assertTrue(systemIndices.isSystemIndex(".tasks1"));
        assertTrue(systemIndices.isSystemIndex(".tasks-old"));
    }

    public void testPluginCannotOverrideBuiltInSystemIndex() {
        Map<String, SystemIndices.Feature> pluginMap = Map.of(
            TASKS_FEATURE_NAME, new SystemIndices.Feature("test", List.of(new SystemIndexDescriptor(TASK_INDEX, "Task Result Index")))
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SystemIndices(pluginMap));
        assertThat(e.getMessage(), containsString("plugin or module attempted to define the same source"));
    }
}

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

package org.elasticsearch.plugin.example;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Example configuration.
 */
public class ExamplePluginConfiguration {
    
    private final Settings settings;
    
    private final Setting<String> TEST_SETTING;
    
    @Inject
    public ExamplePluginConfiguration(Environment env) throws IOException {
        // The directory part of the location matches the artifactId of this plugin
        Path path = env.configFile().resolve("jvm-example/example.yaml");
        settings = Settings.builder().loadFromPath(path).build();
       
        // asserts for tests
        assert settings != null;
        assert settings.get("test") != null;

        String testValue = settings.get("test", "default_value");
        TEST_SETTING = new Setting<>("test", testValue, value -> (value.toLowerCase()), false, Setting.Scope.CLUSTER);   
    }

    public Setting<String> getTestConfig() {
        return TEST_SETTING;
    }
}

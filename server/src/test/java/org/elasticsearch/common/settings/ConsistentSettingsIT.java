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

package org.elasticsearch.common.settings;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ConsistentSettingsIT extends ESIntegTestCase {

    static final Setting<SecureString> DUMMY_STRING_CONSISTENT_SETTING = SecureSetting.secureString("dummy.consistent.secure.string.setting",
            null, Setting.Property.Consistent);
//    static final AffixSetting<SecureString> DUMMY_AFIX_STRING_CONSISTENT_SETTING = Setting.affixKeySetting(
//            "dummy.consistent.secure.string.afix.setting.", "suffix",
//            key -> SecureSetting.secureString(key, null, Setting.Property.Consistent));

    public void testAllConsistent() throws Exception {
        final Environment environment = internalCluster().getInstance(Environment.class);
        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        assertTrue(new ConsistentSettingsService(environment.settings(), clusterService, Collections.singletonList(DUMMY_STRING_CONSISTENT_SETTING)).areAllConsistent());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("dummy.consistent.secure.string.setting", "string_value");
//        secureSettings.setString("dummy.consistent.secure.string.afix.setting." + "afix1" + ".sufix", "afix_value_1");
//        secureSettings.setString("dummy.consistent.secure.string.afix.setting." + "afix2" + ".sufix", "afix_value_2");
        assert builder.getSecureSettings() == null : "Deal with the settings merge";
        builder.setSecureSettings(secureSettings);
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> classes = new ArrayList<>(super.nodePlugins());
        classes.add(DummyPlugin.class);
        return classes;
    }

    public static final class DummyPlugin extends Plugin {

        public DummyPlugin() {
        }

        @Override
        public List<Setting<?>> getSettings() {
            List<Setting<?>> settings = new ArrayList<>(super.getSettings());
            settings.add(DUMMY_STRING_CONSISTENT_SETTING);
            //settings.add(DUMMY_AFIX_STRING_CONSISTENT_SETTING);
            return settings;
        }
    }
}

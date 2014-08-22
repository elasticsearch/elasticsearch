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

package org.elasticsearch.indices.template;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexTemplateFilter;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

@ClusterScope(scope = Scope.SUITE)
public class IndexTemplateFilteringTests extends ElasticsearchIntegrationTest{

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", TestPlugin.class.getName())
                .build();
    }

    @Test
    public void test() throws Exception {
        client().admin().indices().preparePutTemplate("template1")
                .setTemplate("test*")
                .addMapping("type1", XContentFactory.jsonBuilder()
                        .startObject()
                            .startObject("type1")
                                .startObject("properties")
                                    .startObject("field1")
                                        .field("type", "string")
                                        .field("store", "no")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject())
                .get();

        client().admin().indices().preparePutTemplate("template2")
                .setTemplate("test*")
                .addMapping("type2", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type2")
                        .startObject("properties")
                        .startObject("field2")
                        .field("type", "string")
                        .field("store", "no")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject())
                .get();

        prepareCreate("test").get();

        GetMappingsResponse response = client().admin().indices().prepareGetMappings("test").get();
        assertThat(response, notNullValue());
        ImmutableOpenMap<String, MappingMetaData> metadata = response.getMappings().get("test");
        assertThat(metadata.size(), is(1));
        assertThat(metadata.get("type2"), notNullValue());
    }


    public static class TestFilter implements IndexTemplateFilter {
        @Override
        public boolean apply(CreateIndexRequest request, IndexTemplateMetaData template) {
            return template.name().equals("template2");
        }
    }

    public static class TestPlugin extends AbstractPlugin {
        @Override
        public String name() {
            return "test-plugin";
        }

        @Override
        public String description() {
            return "";
        }

        public void onModule(ClusterModule module) {
            module.registerIndexTemplateFilter(TestFilter.class);
        }
    }

}

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
package org.elasticsearch.common.logging;


import org.apache.lucene.util.LuceneTestCase;
import org.hamcrest.Matchers;

public class ESJsonLayoutTests extends LuceneTestCase {

    public void testEmptyType() {
        expectThrows(IllegalArgumentException.class, () -> ESJsonLayout.newBuilder().build());
    }

    public void testLayout() {
        ESJsonLayout server = ESJsonLayout.newBuilder()
                                          .setType("server")
                                          .build();
        String conversionPattern = server.getPatternLayout().getConversionPattern();

        assertThat(conversionPattern, Matchers.equalTo(
            "{" +
                "\"type\": \"server\", " +
                "\"timestamp\": \"%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}\", " +
                "\"level\": \"%p\", " +
                "\"component\": \"%c{1.}\", " +
                "\"cluster.name\": \"${sys:es.logs.cluster_name}\", " +
                "\"node.name\": \"%node_name\", " +
                "\"message\": \"%notEmpty{%enc{%marker}{JSON} }%enc{%.-10000m}{JSON}\"" +
                "%notEmpty{, %node_and_cluster_id }" +
                "%exceptionAsJson }\n"));
    }

    public void testLayoutWithAdditionalFields() {
        ESJsonLayout server = ESJsonLayout.newBuilder()
                                          .setType("server")
                                          .setESMessageFields("x-opaque-id,someOtherField")
                                          .build();
        String conversionPattern = server.getPatternLayout().getConversionPattern();

        assertThat(conversionPattern, Matchers.equalTo(
            "{" +
                "\"type\": \"server\", " +
                "\"timestamp\": \"%d{yyyy-MM-dd'T'HH:mm:ss,SSSZZ}\", " +
                "\"level\": \"%p\", " +
                "\"component\": \"%c{1.}\", " +
                "\"cluster.name\": \"${sys:es.logs.cluster_name}\", " +
                "\"node.name\": \"%node_name\", " +
                "\"message\": \"%notEmpty{%enc{%marker}{JSON} }%enc{%.-10000m}{JSON}\"" +
                "%notEmpty{, \"x-opaque-id\": \"%ESMessageField{x-opaque-id}\"}" +
                "%notEmpty{, \"someOtherField\": \"%ESMessageField{someOtherField}\"}" +
                "%notEmpty{, %node_and_cluster_id }" +
                "%exceptionAsJson }\n"));
    }

}

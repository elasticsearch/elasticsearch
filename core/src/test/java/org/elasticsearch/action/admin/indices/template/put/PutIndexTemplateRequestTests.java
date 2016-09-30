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
package org.elasticsearch.action.admin.indices.template.put;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

public class PutIndexTemplateRequestTests extends ESTestCase {

    // bwc for #21009
    public void testPutIndexTemplateRequest510() throws IOException {
        PutIndexTemplateRequest putRequest = new PutIndexTemplateRequest("test");
        putRequest.patterns(Collections.singletonList("test*"));
        putRequest.order(5);

        PutIndexTemplateRequest multiPatternRequest = new PutIndexTemplateRequest("test");
        multiPatternRequest.patterns(Arrays.asList("test*", "*test2", "*test3*"));
        multiPatternRequest.order(5);

        // These bytes were retrieved by Base64 encoding the result of the above with 5_0_0 code.
        // Note: Instead of a list for the template, in 5_0_0 the element was provided as a string.
        String putRequestBytes = "ADwDAAR0ZXN0BXRlc3QqAAAABQAAAAAAAA==";
        BytesArray bytes = new BytesArray(Base64.getDecoder().decode(putRequestBytes));

        try (StreamInput in = bytes.streamInput()) {
            in.setVersion(Version.V_5_0_0);
            PutIndexTemplateRequest readRequest = new PutIndexTemplateRequest();
            readRequest.readFrom(in);
            assertEquals(putRequest.patterns(), readRequest.patterns());
            assertEquals(putRequest.order(), readRequest.order());

            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(Version.V_5_0_0);
            readRequest.writeTo(output);
            assertEquals(bytes.toBytesRef(), output.bytes().toBytesRef());

            // test that multi templates are reverse-compatible.
            // for the bwc case, if multiple patterns, use only the first pattern seen.
            output.reset();
            multiPatternRequest.writeTo(output);
            assertEquals(bytes.toBytesRef(), output.bytes().toBytesRef());
        }
    }

}

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
package org.elasticsearch.common.xcontent;

import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class XContentTypeTests extends ESTestCase {
    public void testFromJson() throws Exception {
        String mediaType = "application/json";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromJsonUppercase() throws Exception {
        String mediaType = "application/json".toUpperCase(Locale.ROOT);
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromYaml() throws Exception {
        String mediaType = "application/yaml";
        XContentType expectedXContentType = XContentType.YAML;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromSmile() throws Exception {
        String mediaType = "application/smile";
        XContentType expectedXContentType = XContentType.SMILE;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromCbor() throws Exception {
        String mediaType = "application/cbor";
        XContentType expectedXContentType = XContentType.CBOR;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromWildcard() throws Exception {
        String mediaType = "application/*";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromWildcardUppercase() throws Exception {
        String mediaType = "APPLICATION/*";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromRubbish() throws Exception {
        assertThat(XContentType.fromMediaTypeOrFormat(null), nullValue());
        assertThat(XContentType.fromMediaTypeOrFormat(""), nullValue());
        assertThat(XContentType.fromMediaTypeOrFormat("text/plain"), nullValue());
        assertThat(XContentType.fromMediaTypeOrFormat("gobbly;goop"), nullValue());
    }

    public void testMediaType() {
        String version = String.valueOf(Math.abs(randomByte()));
        assertThat(XContentType.parseMediaType("application/vnd.elasticsearch+json;compatible-with=" + version),
            equalTo("application/json"));
        assertThat(XContentType.parseMediaType("application/vnd.elasticsearch+cbor;compatible-with=" + version),
            equalTo("application/cbor"));
        assertThat(XContentType.parseMediaType("application/vnd.elasticsearch+smile;compatible-with=" + version),
            equalTo("application/smile"));
        assertThat(XContentType.parseMediaType("application/vnd.elasticsearch+yaml;compatible-with=" + version),
            equalTo("application/yaml"));
        assertThat(XContentType.parseMediaType("application/json"),
            equalTo("application/json"));


        assertThat(XContentType.parseMediaType("APPLICATION/VND.ELASTICSEARCH+JSON;COMPATIBLE-WITH=" + version),
            equalTo("application/json"));
        assertThat(XContentType.parseMediaType("APPLICATION/JSON"),
            equalTo("application/json"));
    }

    public void testVersionParsing() {
        String version = String.valueOf(Math.abs(randomByte()));
        assertThat(XContentType.parseVersion("application/vnd.elasticsearch+json;compatible-with=" + version),
            equalTo(version));
        assertThat(XContentType.parseVersion("application/vnd.elasticsearch+cbor;compatible-with=" + version),
            equalTo(version));
        assertThat(XContentType.parseVersion("application/vnd.elasticsearch+smile;compatible-with=" + version),
            equalTo(version));
        assertThat(XContentType.parseVersion("application/json"),
            nullValue());


        assertThat(XContentType.parseVersion("APPLICATION/VND.ELASTICSEARCH+JSON;COMPATIBLE-WITH=" + version),
            equalTo(version));
        assertThat(XContentType.parseVersion("APPLICATION/JSON"),
            nullValue());

        assertThat(XContentType.parseVersion("application/json;compatible-with=" + version+".0"),
            is(nullValue()));
    }

    public void testMediaTypeWithoutESSubtype() {
        String version = String.valueOf(Math.abs(randomByte()));
        assertThat(XContentType.fromMediaTypeOrFormat("application/json;compatible-with=" + version), nullValue());
    }

    public void testAnchoring(){
        String version = String.valueOf(Math.abs(randomByte()));
        assertThat(XContentType.fromMediaTypeOrFormat("sth_application/json;compatible-with=" + version+".0"), nullValue());
        assertThat(XContentType.fromMediaTypeOrFormat("sth_application/json;compatible-with=" + version+"_sth"), nullValue());
        //incorrect parameter not validated at the moment, just ignored
        assertThat(XContentType.fromMediaTypeOrFormat("application/json;compatible-with=" + version+"_sth"), nullValue());
    }

    public void testVersionParsingOnText() {
        String version = String.valueOf(Math.abs(randomByte()));
        assertThat(XContentType.parseVersion("text/vnd.elasticsearch+csv;compatible-with=" + version),
            equalTo(version));
        assertThat(XContentType.parseVersion("text/vnd.elasticsearch+text;compatible-with=" + version),
            equalTo(version));
        assertThat(XContentType.parseVersion("text/vnd.elasticsearch+tab-separated-values;compatible-with=" + version),
            equalTo(version));
        assertThat(XContentType.parseVersion("text/csv"),
            nullValue());

        assertThat(XContentType.parseVersion("TEXT/VND.ELASTICSEARCH+CSV;COMPATIBLE-WITH=" + version),
            equalTo(version));
        assertThat(XContentType.parseVersion("TEXT/csv"),
            nullValue());
    }
}

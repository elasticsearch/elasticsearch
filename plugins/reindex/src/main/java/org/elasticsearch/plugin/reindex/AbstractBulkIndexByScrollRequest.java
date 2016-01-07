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

package org.elasticsearch.plugin.reindex;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.Script;

import java.io.IOException;

public abstract class AbstractBulkIndexByScrollRequest<Self extends AbstractBulkIndexByScrollRequest<Self>>
        extends AbstractBulkByScrollRequest<Self> {
    /**
     * Script to modify the documents before they are processed.
     */
    private Script script;

    public AbstractBulkIndexByScrollRequest() {
    }

    public AbstractBulkIndexByScrollRequest(SearchRequest source) {
        super(source);
    }

    /**
     * Script to modify the documents before they are processed.
     */
    public Script getScript() {
        return script;
    }

    /**
     * Script to modify the documents before they are processed.
     */
    public Self setScript(@Nullable Script script) {
        this.script = script;
        return self();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            script = Script.readScript(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalStreamable(script);
    }

    @Override
    protected void searchToString(StringBuilder b) {
        super.searchToString(b);
        if (script != null) {
            b.append(" updated with [").append(script).append(']');
        }
    }
}

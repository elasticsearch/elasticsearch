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

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;

/**
 *  A simple example of handling leaf search script when only access to a document is needed.
 */
public final class ExampleSearchScript implements SearchScript {

    private final AbstractSearchScript leafSearchScript;
    private final SearchLookup lookup;
    private final boolean needScore;

    protected ExampleSearchScript(SearchLookup lookup, AbstractSearchScript leafSearchScript, boolean needScore) {
        this.lookup = lookup;
        this.leafSearchScript = leafSearchScript;
        this.needScore = needScore;
    }

    @Override
    public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
        leafSearchScript.setLookup(lookup.getLeafSearchLookup(context));
        return leafSearchScript;
    }

    @Override
    public boolean needsScores() {
        return needScore;
    }

}

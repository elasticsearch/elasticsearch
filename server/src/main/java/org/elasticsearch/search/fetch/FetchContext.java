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

package org.elasticsearch.search.fetch;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.collapse.CollapseContext;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescoreContext;

import java.util.Collections;
import java.util.List;

public class FetchContext {

    public FetchContext(SearchContext searchContext) {
        this.searchContext = searchContext;
    }

    public static FetchContext fromSearchContext(SearchContext context) {
        return new FetchContext(context);
    }

    private final SearchContext searchContext;

    public String getIndexName() {
        return searchContext.indexShard().shardId().getIndexName();
    }

    public ContextIndexSearcher searcher() {
        return searchContext.searcher();
    }

    public MapperService mapperService() {
        return searchContext.mapperService();
    }

    public IndexSettings getIndexSettings() {
        return mapperService().getIndexSettings();
    }

    public IndexFieldData<?> getForField(MappedFieldType fieldType) {
        return searchContext.getForField(fieldType);
    }

    public Query query() {
        return searchContext.query();
    }

    public ParsedQuery parsedQuery() {
        return searchContext.parsedQuery();
    }

    public ParsedQuery parsedPostFilter() {
        return searchContext.parsedPostFilter();
    }

    public FetchSourceContext fetchSourceContext() {
        return searchContext.fetchSourceContext();
    }

    public boolean explain() {
        return searchContext.explain();
    }

    public List<RescoreContext> rescore() {
        return searchContext.rescore();
    }

    public boolean seqNoAndPrimaryTerm() {
        return searchContext.seqNoAndPrimaryTerm();
    }

    public FetchDocValuesContext docValuesContext() {
        FetchDocValuesContext dvContext = searchContext.docValuesContext();
        if (searchContext.collapse() != null) {
            // retrieve the `doc_value` associated with the collapse field
            String name = searchContext.collapse().getFieldName();
            if (dvContext == null) {
                return new FetchDocValuesContext(Collections.singletonList(new FieldAndFormat(name, null)));
            } else if (searchContext.docValuesContext().fields().stream().map(ff -> ff.field).anyMatch(name::equals) == false) {
                dvContext.fields().add(new FieldAndFormat(name, null));
            }
        }
        return dvContext;
    }

    public SearchHighlightContext highlight() {
        return searchContext.highlight();
    }

    public boolean fetchScores() {
        return searchContext.sort() != null && searchContext.trackScores();
    }

    public InnerHitsContext innerHits() {
        return searchContext.innerHits();
    }

    public boolean version() {
        // TODO version is loaded from docvalues, not stored fields, so why are we checking
        // stored fields here?
        return searchContext.version() &&
            (searchContext.storedFieldsContext() == null || searchContext.storedFieldsContext().fetchFields());
    }

    public FetchFieldsContext fetchFieldsContext() {
        return searchContext.fetchFieldsContext();
    }

    public ScriptFieldsContext scriptFields() {
        return searchContext.scriptFields();
    }

    public SearchExtBuilder getSearchExt(String name) {
        return searchContext.getSearchExt(name);
    }
}

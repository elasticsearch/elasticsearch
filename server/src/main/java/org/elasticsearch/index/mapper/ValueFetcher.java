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

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsPhase;

import java.io.IOException;
import java.util.List;

/**
 * A helper class for fetching field values during the {@link FetchFieldsPhase}. Each {@link FieldMapper}
 * is in charge of defining a value fetcher through {@link FieldMapper#valueFetcher}.
 */
public interface ValueFetcher {
    /**
     * Fetch this field's values, converting them into a "standard form" and applying
     * any formats configured when the {@linkplain ValueFetcher} was built.
     * <p>
     * Note that for array values, the order in which values are returned is undefined and
     * should not be relied on.
     *
     * @param docId the document's id.
     * @return a list a standardized field values.
     */
    List<Object> fetchValues(int docId) throws IOException;

    /**
     * Update the leaf reader used to fetch values.
     */
    default void setNextReader(LeafReaderContext context) {}
}

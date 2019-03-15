/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ClientHelper;

import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Objects;

public abstract class BatchedDataIterator<T, E extends Collection<T>> {
    private static final Logger LOGGER = LogManager.getLogger(BatchedDataIterator.class);

    private static final String CONTEXT_ALIVE_DURATION = "5m";
    private static final int BATCH_SIZE = 10000;

    private final Client client;
    private final String index;
    private volatile long count;
    private volatile long totalHits;
    private volatile String scrollId;
    private volatile boolean isScrollInitialised;

    protected BatchedDataIterator(Client client, String index) {
        this.client = Objects.requireNonNull(client);
        this.index = Objects.requireNonNull(index);
        this.totalHits = 0;
        this.count = 0;
        this.isScrollInitialised = false;
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    public boolean hasNext() {
        return !isScrollInitialised || count != totalHits;
    }

    /**
     * The first time next() is called, the search will be performed and the first
     * batch will be returned. Any subsequent call will return the following batches.
     * <p>
     * Note that in some implementations it is possible that when there are no
     * results at all, the first time this method is called an empty {@code Deque} is returned.
     */
    public void next(ActionListener<E> listener) {
        if (!hasNext()) {
            listener.onFailure(new NoSuchElementException());
        }
        ActionListener<SearchResponse> wrappedListener = ActionListener.wrap(
            searchResponse -> {
                scrollId = searchResponse.getScrollId();
                listener.onResponse(mapHits(searchResponse));
            },
            listener::onFailure
        );
        if (scrollId == null) {
            initScroll(wrappedListener);
        } else {
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId).scroll(CONTEXT_ALIVE_DURATION);
            ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(),
                ClientHelper.DATA_FRAME_ORIGIN,
                searchScrollRequest,
                wrappedListener,
                client::searchScroll);
        }
    }

    private void initScroll(ActionListener<SearchResponse> listener) {
        LOGGER.trace("ES API CALL: search index {}", index);

        isScrollInitialised = true;

        IndicesOptions indicesOptions = SearchRequest.DEFAULT_INDICES_OPTIONS;
        indicesOptions = IndicesOptions.fromOptions(true, indicesOptions.allowNoIndices(), indicesOptions.expandWildcardsOpen(),
            indicesOptions.expandWildcardsClosed(), indicesOptions);

        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.indicesOptions(indicesOptions);
        searchRequest.scroll(CONTEXT_ALIVE_DURATION);
        searchRequest.source(new SearchSourceBuilder()
            .fetchSource(getFetchSourceContext())
            .size(BATCH_SIZE)
            .query(getQuery())
            .fetchSource(shouldFetchSource())
            .trackTotalHits(true)
            .sort(sortField(), sortOrder()));


        ActionListener<SearchResponse> wrappedListener = ActionListener.wrap(
            searchResponse -> {
                totalHits = searchResponse.getHits().getTotalHits().value;
                scrollId = searchResponse.getScrollId();
                listener.onResponse(searchResponse);
            },
            listener::onFailure
        );

        ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ClientHelper.DATA_FRAME_ORIGIN,
            searchRequest,
            wrappedListener,
            client::search);
    }

    private E mapHits(SearchResponse searchResponse) {
        E results = getCollection();

        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            T mapped = map(hit);
            if (mapped != null) {
                results.add(mapped);
            }
        }
        count += hits.length;

        if (!hasNext() && scrollId != null) {
            client.prepareClearScroll().setScrollIds(Collections.singletonList(scrollId)).get();
        }
        return results;
    }

    /**
     * Should fetch source? Defaults to {@code true}
     * @return whether the source should be fetched
     */
    protected boolean shouldFetchSource() {
        return true;
    }

    /**
     * Get the query to use for the search
     * @return the search query
     */
    protected abstract QueryBuilder getQuery();

    /**
     * Maps the search hit to the document type
     * @param hit
     *            the search hit
     * @return The mapped document or {@code null} if the mapping failed
     */
    protected abstract T map(SearchHit hit);

    protected abstract E getCollection();

    protected abstract SortOrder sortOrder();

    protected abstract String sortField();

    protected FetchSourceContext getFetchSourceContext() {
        return FetchSourceContext.FETCH_SOURCE;
    }

}

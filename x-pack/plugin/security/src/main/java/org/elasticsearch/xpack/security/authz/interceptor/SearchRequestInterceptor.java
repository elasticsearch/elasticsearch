/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.io.IOException;
import java.util.SortedMap;

/**
 * If field level security is enabled this interceptor disables the request cache for search requests.
 */
public class SearchRequestInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor {

    private static final ThreadLocal<BytesStreamOutput> threadLocalOutput = ThreadLocal.withInitial(BytesStreamOutput::new);

    public SearchRequestInterceptor(ThreadPool threadPool, XPackLicenseState licenseState) {
        super(threadPool.getThreadContext(), licenseState);
    }

    @Override
    void disableFeatures(IndicesRequest indicesRequest, SortedMap<String, IndicesAccessControl.IndexAccessControl> indicesAccessControlByIndex,
                         ActionListener<Void> listener) {
        final SearchSourceBuilder source;
        if (indicesRequest instanceof SearchRequest) {
            final SearchRequest request = (SearchRequest) indicesRequest;
            source = request.source();
        } else {
            final ShardSearchRequest request = (ShardSearchRequest) indicesRequest;
            try {
                BytesReference bytes = serialise(indicesAccessControlByIndex);
                request.cacheModifier(bytes);
            } catch (IOException e) {
                listener.onFailure(e);
            }
            source = request.source();
        }

        if (indicesAccessControlByIndex.values().stream().anyMatch(iac -> iac.getDocumentPermissions().hasDocumentLevelPermissions())) {
            if (source != null && source.suggest() != null) {
                listener.onFailure(new ElasticsearchSecurityException("Suggest isn't supported if document level security is enabled",
                    RestStatus.BAD_REQUEST));
            } else if (source != null && source.profile()) {
                listener.onFailure(new ElasticsearchSecurityException("A search request cannot be profiled if document level security " +
                    "is enabled", RestStatus.BAD_REQUEST));
            } else {
                listener.onResponse(null);
            }
        } else {
            listener.onResponse(null);
        }
    }

    private BytesReference serialise(SortedMap<String, IndicesAccessControl.IndexAccessControl> accessControlByIndex) throws IOException {
        BytesStreamOutput out = threadLocalOutput.get();
        try {
            for (IndicesAccessControl.IndexAccessControl iac : accessControlByIndex.values()) {
                iac.writeCacheKey(out);
            }
            // copy it over since we don't want to share the thread-local bytes in #scratch
            return out.copyBytes();
        } finally {
            out.reset();
        }
    }

    @Override
    public boolean supports(IndicesRequest request) {
        return request instanceof SearchRequest || request instanceof ShardSearchRequest;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.execution.PlanExecutor;
import org.elasticsearch.xpack.eql.parser.ParserParams;
import org.elasticsearch.xpack.eql.session.Configuration;
import org.elasticsearch.xpack.eql.session.Results;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

public class TransportEqlSearchAction extends HandledTransportAction<EqlSearchRequest, EqlSearchResponse> {
    private final SecurityContext securityContext;
    private final ClusterService clusterService;
    private final PlanExecutor planExecutor;

    @Inject
    public TransportEqlSearchAction(Settings settings, ClusterService clusterService, TransportService transportService,
            ThreadPool threadPool, ActionFilters actionFilters, PlanExecutor planExecutor) {
        super(EqlSearchAction.NAME, transportService, actionFilters, EqlSearchRequest::new);

        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings) ?
            new SecurityContext(settings, threadPool.getThreadContext()) : null;
        this.clusterService = clusterService;
        this.planExecutor = planExecutor;
    }

    @Override
    protected void doExecute(Task task, EqlSearchRequest request, ActionListener<EqlSearchResponse> listener) {
        operation(planExecutor, request, username(securityContext), clusterName(clusterService), listener);
    }

    public static void operation(PlanExecutor planExecutor, EqlSearchRequest request, String username,
            String clusterName, ActionListener<EqlSearchResponse> listener) {
        // TODO: these should be sent by the client
        ZoneId zoneId = DateUtils.of("Z");
        QueryBuilder filter = request.query();
        TimeValue timeout = TimeValue.timeValueSeconds(30);
        boolean includeFrozen = request.indicesOptions().ignoreThrottled() == false;
        String clientId = null;

        ParserParams params = new ParserParams()
                .fieldEventType(request.eventTypeField())
                .fieldTimestamp(request.timestampField())
                .implicitJoinKey(request.implicitJoinKeyField());

        Configuration cfg = new Configuration(request.indices(), zoneId, username, clusterName, filter, timeout, includeFrozen, clientId);
        //planExecutor.eql(cfg, request.rule(), params, wrap(r -> listener.onResponse(createResponse(r)), listener::onFailure));
        listener.onResponse(createResponse(null));
    }

    static EqlSearchResponse createResponse(Results results) {
        // Stubbed search response
        // TODO: implement actual search response processing once the parser/executor is in place
        // Updated for stubbed response to: process where serial_event_id = 1
        // to validate the sample test until the engine is wired in.
        List<SearchHit> events = Arrays.asList(
            new SearchHit(1, "111", null)
        );
        EqlSearchResponse.Hits hits = new EqlSearchResponse.Hits(events, null,
            null, new TotalHits(1, TotalHits.Relation.EQUAL_TO));

        return new EqlSearchResponse(hits, 0, false);
    }

    static String username(SecurityContext securityContext) {
        return securityContext != null && securityContext.getUser() != null ? securityContext.getUser().principal() : null;
    }

    static String clusterName(ClusterService clusterService) {
        return clusterService.getClusterName().value();
    }
}

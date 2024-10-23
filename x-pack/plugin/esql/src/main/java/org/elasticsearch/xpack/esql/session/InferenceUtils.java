/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class InferenceUtils {

    private InferenceUtils() {}

    public static List<Tuple<String, String>> semanticQueries(LogicalPlan analyzedPlan) {
        List<Tuple<String, String>> result = new ArrayList<>();

        analyzedPlan.forEachDown(plan -> {
            if (plan instanceof Filter) {
                ((Filter) plan).condition().forEachDown(expression -> {
                    if (expression instanceof Match && ((Match) expression).field().dataType() == DataType.SEMANTIC_TEXT) {
                        result.add(Tuple.tuple(((Match) expression).field().sourceText(), ((Match) expression).query().sourceText()));
                    }
                });
            }
        });

        return result;
    }

    public static void setInferenceResult(LogicalPlan analyzedPlan, String fieldName, String query, InferenceResults inferenceResults) {
        analyzedPlan.forEachDown(plan -> {
            if (plan instanceof Filter) {
                Filter filter = (Filter) plan;
                filter.condition().forEachDown(expression -> {
                    if (expression instanceof Match) {
                        Match match = (Match) expression;
                        if (match.field().sourceText().equals(fieldName) && match.query().sourceText().equals(query)) {
                            match.setInferenceResults(inferenceResults);
                        }
                    }
                });
            }
        });
    }

    public static Map<String, String> getInferenceIds(LogicalPlan analyzedPlan, ClusterService clusterService) {
        final Map<String, String> result = new HashMap<>();

        Map<String, IndexMetadata> indexMetadata = clusterService.state().getMetadata().getIndices();
        analyzedPlan.forEachDown(plan -> {
            if (plan instanceof EsRelation) {
                for (String indexName : ((EsRelation) plan).index().concreteIndices()) {
                    Map<String, InferenceFieldMetadata> inferenceFields = indexMetadata.get(indexName).getInferenceFields();
                    for (String fieldName : inferenceFields.keySet()) {
                        // TODO this does not handle things like fields conflicts or the idea that the same fieldName can have different
                        // inference IDs in different indices
                        // we want to move this in the Analyzer and have a check in the Verifier
                        result.put(fieldName, inferenceFields.get(fieldName).getInferenceId());
                    }
                }
            }
        });
        return result;
    }

    public static void setInferenceResults(
        LogicalPlan plan,
        Client client,
        ClusterService clusterService,
        ActionListener<Result> listener,
        BiConsumer<LogicalPlan, ActionListener<Result>> callback
    ) {
        List<Tuple<String, String>> semanticQueries = semanticQueries(plan);

        if (semanticQueries.isEmpty()) {
            callback.accept(plan, listener);
            return;
        }

        GroupedActionListener<InferenceAction.Response> actionListener = new GroupedActionListener<>(
            semanticQueries.size(),
            new ActionListener<Collection<InferenceAction.Response>>() {
                @Override
                public void onResponse(Collection<InferenceAction.Response> ignored) {
                    try {
                        callback.accept(plan, listener);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }
        );

        Map<String, String> inferenceIds = getInferenceIds(plan, clusterService);

        for (Tuple<String, String> semanticQuery : semanticQueries) {

            InferenceAction.Request inferenceRequest = new InferenceAction.Request(
                TaskType.ANY,
                inferenceIds.get(semanticQuery.v1()),
                null,
                List.of(semanticQuery.v2()),
                Map.of(),
                InputType.SEARCH,
                InferModelAction.Request.DEFAULT_TIMEOUT_FOR_API,
                false
            );

            executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                InferenceAction.INSTANCE,
                inferenceRequest,
                actionListener.delegateFailureAndWrap((next, inferenceResponse) -> {
                    InferenceResults inferenceResults = SemanticQueryBuilder.validateAndConvertInferenceResults(
                        inferenceResponse.getResults(),
                        semanticQuery.v1()
                    );
                    setInferenceResult(plan, semanticQuery.v1(), semanticQuery.v2(), inferenceResults);
                    next.onResponse(inferenceResponse);
                })
            );
        }
    }
}

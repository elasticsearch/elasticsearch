/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.chunking.EmbeddingRequestChunker;
import org.elasticsearch.xpack.inference.external.action.alibabacloudsearch.AlibabaCloudSearchActionCreator;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.request.alibabacloudsearch.AlibabaCloudSearchUtils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings.AlibabaCloudSearchEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.rerank.AlibabaCloudSearchRerankModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse.AlibabaCloudSearchSparseModel;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.action.InferenceAction.Request.DEFAULT_TIMEOUT;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidModelException;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.parsePersistedConfigErrorMsg;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceFields.EMBEDDING_MAX_BATCH_SIZE;

public class AlibabaCloudSearchService extends SenderService {
    public static final String NAME = AlibabaCloudSearchUtils.SERVICE_NAME;

    public AlibabaCloudSearchService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
        super(factory, serviceComponents);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void parseRequestConfig(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        ActionListener<Model> parsedModelListener
    ) {
        try {
            Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
            Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

            ChunkingSettings chunkingSettings = null;
            if (List.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING).contains(taskType)) {
                chunkingSettings = ChunkingSettingsBuilder.fromMap(
                    removeFromMapOrDefaultEmpty(config, ModelConfigurations.CHUNKING_SETTINGS)
                );
            }

            AlibabaCloudSearchModel model = createModel(
                inferenceEntityId,
                taskType,
                serviceSettingsMap,
                taskSettingsMap,
                chunkingSettings,
                serviceSettingsMap,
                TaskType.unsupportedTaskTypeErrorMsg(taskType, NAME),
                ConfigurationParseContext.REQUEST
            );

            throwIfNotEmptyMap(config, NAME);
            throwIfNotEmptyMap(serviceSettingsMap, NAME);
            throwIfNotEmptyMap(taskSettingsMap, NAME);

            parsedModelListener.onResponse(model);
        } catch (Exception e) {
            parsedModelListener.onFailure(e);
        }
    }

    private static AlibabaCloudSearchModel createModelWithoutLoggingDeprecations(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage
    ) {
        return createModel(
            inferenceEntityId,
            taskType,
            serviceSettings,
            taskSettings,
            chunkingSettings,
            secretSettings,
            failureMessage,
            ConfigurationParseContext.PERSISTENT
        );
    }

    private static AlibabaCloudSearchModel createModel(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        String failureMessage,
        ConfigurationParseContext context
    ) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> new AlibabaCloudSearchEmbeddingsModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                chunkingSettings,
                secretSettings,
                context
            );
            case SPARSE_EMBEDDING -> new AlibabaCloudSearchSparseModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                chunkingSettings,
                secretSettings,
                context
            );
            case RERANK -> new AlibabaCloudSearchRerankModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                secretSettings,
                context
            );
            case COMPLETION -> new AlibabaCloudSearchCompletionModel(
                inferenceEntityId,
                taskType,
                NAME,
                serviceSettings,
                taskSettings,
                secretSettings,
                context
            );
            default -> throw new ElasticsearchStatusException(failureMessage, RestStatus.BAD_REQUEST);
        };
    }

    @Override
    public AlibabaCloudSearchModel parsePersistedConfigWithSecrets(
        String inferenceEntityId,
        TaskType taskType,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);
        Map<String, Object> secretSettingsMap = removeFromMapOrThrowIfNull(secrets, ModelSecrets.SECRET_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (List.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING).contains(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMap(config, ModelConfigurations.CHUNKING_SETTINGS));
        }

        return createModelWithoutLoggingDeprecations(
            inferenceEntityId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            chunkingSettings,
            secretSettingsMap,
            parsePersistedConfigErrorMsg(inferenceEntityId, NAME)
        );
    }

    @Override
    public AlibabaCloudSearchModel parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
        Map<String, Object> serviceSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS);
        Map<String, Object> taskSettingsMap = removeFromMapOrDefaultEmpty(config, ModelConfigurations.TASK_SETTINGS);

        ChunkingSettings chunkingSettings = null;
        if (List.of(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING).contains(taskType)) {
            chunkingSettings = ChunkingSettingsBuilder.fromMap(removeFromMap(config, ModelConfigurations.CHUNKING_SETTINGS));
        }

        return createModelWithoutLoggingDeprecations(
            inferenceEntityId,
            taskType,
            serviceSettingsMap,
            taskSettingsMap,
            chunkingSettings,
            null,
            parsePersistedConfigErrorMsg(inferenceEntityId, NAME)
        );
    }

    @Override
    public void doInfer(
        Model model,
        InferenceInputs inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        if (model instanceof AlibabaCloudSearchModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        AlibabaCloudSearchModel alibabaCloudSearchModel = (AlibabaCloudSearchModel) model;
        var actionCreator = new AlibabaCloudSearchActionCreator(getSender(), getServiceComponents());

        var action = alibabaCloudSearchModel.accept(actionCreator, taskSettings, inputType);
        action.execute(inputs, timeout, listener);
    }

    @Override
    protected void doChunkedInfer(
        Model model,
        DocumentsOnlyInput inputs,
        Map<String, Object> taskSettings,
        InputType inputType,
        ChunkingOptions chunkingOptions,
        TimeValue timeout,
        ActionListener<List<ChunkedInferenceServiceResults>> listener
    ) {
        if (model instanceof AlibabaCloudSearchModel == false) {
            listener.onFailure(createInvalidModelException(model));
            return;
        }

        AlibabaCloudSearchModel alibabaCloudSearchModel = (AlibabaCloudSearchModel) model;
        var actionCreator = new AlibabaCloudSearchActionCreator(getSender(), getServiceComponents());

        List<EmbeddingRequestChunker.BatchRequestAndListener> batchedRequests = new EmbeddingRequestChunker(
            inputs.getInputs(),
            EMBEDDING_MAX_BATCH_SIZE,
            getEmbeddingTypeFromTaskType(alibabaCloudSearchModel.getTaskType()),
            alibabaCloudSearchModel.getConfigurations().getChunkingSettings()
        ).batchRequestsWithListeners(listener);

        for (var request : batchedRequests) {
            var action = alibabaCloudSearchModel.accept(actionCreator, taskSettings, inputType);
            action.execute(new DocumentsOnlyInput(request.batch().inputs()), timeout, request.listener());
        }
    }

    private EmbeddingRequestChunker.EmbeddingType getEmbeddingTypeFromTaskType(TaskType taskType) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> EmbeddingRequestChunker.EmbeddingType.FLOAT;
            case SPARSE_EMBEDDING -> EmbeddingRequestChunker.EmbeddingType.SPARSE;
            default -> throw new IllegalArgumentException("Unsupported task type for chunking: " + taskType);
        };
    }

    /**
     * For text embedding models get the embedding size and
     * update the service settings.
     *
     * @param model The new model
     * @param listener The listener
     */
    @Override
    public void checkModelConfig(Model model, ActionListener<Model> listener) {
        if (model instanceof AlibabaCloudSearchEmbeddingsModel embeddingsModel) {
            ServiceUtils.getEmbeddingSize(
                model,
                this,
                listener.delegateFailureAndWrap((l, size) -> l.onResponse(updateModelWithEmbeddingDetails(embeddingsModel, size)))
            );
        } else {
            checkAlibabaCloudSearchServiceConfig(model, this, listener);
        }
    }

    private AlibabaCloudSearchEmbeddingsModel updateModelWithEmbeddingDetails(AlibabaCloudSearchEmbeddingsModel model, int embeddingSize) {
        AlibabaCloudSearchEmbeddingsServiceSettings serviceSettings = new AlibabaCloudSearchEmbeddingsServiceSettings(
            new AlibabaCloudSearchServiceSettings(
                model.getServiceSettings().getCommonSettings().modelId(),
                model.getServiceSettings().getCommonSettings().getHost(),
                model.getServiceSettings().getCommonSettings().getWorkspaceName(),
                model.getServiceSettings().getCommonSettings().getHttpSchema(),
                model.getServiceSettings().getCommonSettings().rateLimitSettings()
            ),
            SimilarityMeasure.DOT_PRODUCT,
            embeddingSize,
            model.getServiceSettings().getMaxInputTokens()
        );

        return new AlibabaCloudSearchEmbeddingsModel(model, serviceSettings);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_ALIBABACLOUD_SEARCH_ADDED;
    }

    /**
     * For other models except of text embedding
     * check the model's service settings and task settings
     *
     * @param model The new model
     * @param service The inferenceService
     * @param listener The listener
     */
    private void checkAlibabaCloudSearchServiceConfig(Model model, InferenceService service, ActionListener<Model> listener) {
        String input = ALIBABA_CLOUD_SEARCH_SERVICE_CONFIG_INPUT;
        String query = model.getTaskType().equals(TaskType.RERANK) ? ALIBABA_CLOUD_SEARCH_SERVICE_CONFIG_QUERY : null;

        service.infer(
            model,
            query,
            List.of(input),
            false,
            Map.of(),
            InputType.INGEST,
            DEFAULT_TIMEOUT,
            listener.delegateFailureAndWrap((delegate, r) -> {
                listener.onResponse(model);
            })
        );
    }

    private static final String ALIBABA_CLOUD_SEARCH_SERVICE_CONFIG_INPUT = "input";
    private static final String ALIBABA_CLOUD_SEARCH_SERVICE_CONFIG_QUERY = "query";
}

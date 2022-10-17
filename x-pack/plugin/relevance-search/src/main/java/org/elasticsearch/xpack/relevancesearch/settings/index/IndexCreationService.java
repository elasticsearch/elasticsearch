/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.settings.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.relevancesearch.settings.AbstractSettingsService;
import org.elasticsearch.xpack.relevancesearch.settings.relevance.RelevanceSettingsService;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.RELEVANCE_SETTINGS_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class IndexCreationService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(RelevanceSettingsService.class);

    private final ClusterService clusterService;

    private final Client client;

    public IndexCreationService(final Client client, final ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait for state recovered
            return;
        }
        if (this.clusterService.state().metadata().hasIndex(AbstractSettingsService.ENT_SEARCH_INDEX)) {
            // no need to re-create
            this.clusterService.removeListener(this);
            return;
        }
        ensureInternalIndex(this.client);
        this.clusterService.removeListener(this);
    }

    public static void ensureInternalIndex(Client client) {
        CreateIndexRequest request = new CreateIndexRequest(AbstractSettingsService.ENT_SEARCH_INDEX).mapping(getInternalIndexMapping())
            .settings(getInternalIndexSettings())
            .origin(RELEVANCE_SETTINGS_ORIGIN);
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            RELEVANCE_SETTINGS_ORIGIN,
            request,
            new ActionListener<CreateIndexResponse>() {
                public void onResponse(CreateIndexResponse createIndexResponse) {
                    logger.info("Created " + AbstractSettingsService.ENT_SEARCH_INDEX + " index.");
                }

                public void onFailure(Exception e) {
                    final Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof ResourceAlreadyExistsException) {
                        logger.info("Index " + AbstractSettingsService.ENT_SEARCH_INDEX + " already exists.");
                    } else {
                        logger.info("Failed to create " + AbstractSettingsService.ENT_SEARCH_INDEX + " index " + e.toString());
                    }
                }
            },
            client.admin().indices()::create
        );
    }

    private static Settings getInternalIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
            .build();
    }

    private static XContentBuilder getInternalIndexMapping() {
        try {
            final XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.startObject(SINGLE_MAPPING_NAME);
                {
                    builder.startObject("_meta");
                    {
                        builder.field("version", Version.CURRENT);
                    }
                    builder.endObject();
                    builder.startArray("dynamic_templates");
                    {
                        builder.startObject();
                        {
                            builder.startObject("string_as_keyword");
                            {
                                builder.field("match_mapping_type", "string");
                                builder.startObject("mapping");
                                {
                                    builder.field("type", "keyword");
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endArray();
                    builder.field("dynamic", "strict");
                    builder.startObject("properties");
                    {
                        builder.startObject("name");
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject("type");
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject("group_name");
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject("query_type");
                        {
                            builder.field("type", "keyword");
                        }
                        builder.endObject();
                        builder.startObject("query_configuration");
                        {
                            builder.startObject("properties");
                            {
                                builder.startObject("fields");
                                {
                                    builder.field("type", "keyword");
                                }
                                builder.endObject();
                                builder.startObject("boosts");
                                {
                                    builder.field("type", "object");
                                    builder.field("dynamic", "true");
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                        builder.startObject("conditions");
                        {
                            builder.startObject("properties");
                            {
                                builder.startObject("context");
                                {
                                    builder.field("type", "keyword");
                                }
                                builder.endObject();
                                builder.startObject("value");
                                {
                                    builder.field("type", "keyword");
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                        builder.startObject("pinned_document_ids");
                        {
                            builder.startObject("properties");
                            {
                                builder.startObject("_id");
                                {
                                    builder.field("type", "keyword");
                                }
                                builder.endObject();
                                builder.startObject("_index");
                                {
                                    builder.field("type", "keyword");
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                        builder.startObject("excluded_document_ids");
                        {
                            builder.startObject("properties");
                            {
                                builder.startObject("_id");
                                {
                                    builder.field("type", "keyword");
                                }
                                builder.endObject();
                                builder.startObject("_index");
                                {
                                    builder.field("type", "keyword");
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for " + AbstractSettingsService.ENT_SEARCH_INDEX, e);
        }
    }
}

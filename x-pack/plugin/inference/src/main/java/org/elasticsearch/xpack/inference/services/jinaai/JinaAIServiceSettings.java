/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

public class JinaAIServiceSettings extends FilteredXContentObject implements ServiceSettings, JinaAIRateLimitServiceSettings {

    public static final String NAME = "jinaai_service_settings";
    public static final String OLD_MODEL_ID_FIELD = "model";
    public static final String MODEL_ID = "model_id";
    private static final Logger logger = LogManager.getLogger(JinaAIServiceSettings.class);
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(2_000); // TODO(JoanFM): Check value

    public static JinaAIServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String url = extractOptionalString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        URI uri = convertToUri(url, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String oldModelId = extractOptionalString(map, OLD_MODEL_ID_FIELD, ModelConfigurations.SERVICE_SETTINGS, validationException);
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            JinaAIService.NAME,
            context
        );

        String modelId = extractOptionalString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        // TODO(JoanFM): Review this
        if (context == ConfigurationParseContext.REQUEST && oldModelId != null) {
            logger.info("The JinaAI [service_settings.model] field is deprecated. Please use [service_settings.model_id] instead.");
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new JinaAIServiceSettings(uri, modelId(oldModelId, modelId), rateLimitSettings);
    }

    private static String modelId(@Nullable String model, @Nullable String modelId) {
        return modelId != null ? modelId : model;
    }

    private final URI uri;
    private final String modelId;
    private final RateLimitSettings rateLimitSettings;

    public JinaAIServiceSettings(
        @Nullable URI uri,
        @Nullable String modelId,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.uri = uri;
        this.modelId = modelId;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public JinaAIServiceSettings(
        @Nullable String url,
        @Nullable String modelId,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this(createOptionalUri(url), modelId, rateLimitSettings);
    }

    public JinaAIServiceSettings(StreamInput in) throws IOException {
        uri = createOptionalUri(in.readOptionalString());
        modelId = in.readOptionalString();

        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            rateLimitSettings = new RateLimitSettings(in);
        } else {
            rateLimitSettings = DEFAULT_RATE_LIMIT_SETTINGS;
        }
    }

    // should only be used for testing, public because it's accessed outside of the package
    public JinaAIServiceSettings() {
        this((URI) null, null, null);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    public URI uri() {
        return uri;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragment(builder, params);

        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentFragment(XContentBuilder builder, Params params) throws IOException {
        return toXContentFragmentOfExposedFields(builder, params);
    }

    @Override
    public XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        if (uri != null) {
            builder.field(URL, uri.toString());
        }
        if (modelId != null) {
            builder.field(MODEL_ID, modelId);
        }
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_13_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        var uriToWrite = uri != null ? uri.toString() : null;
        out.writeOptionalString(uriToWrite);
        out.writeOptionalString(modelId);

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            rateLimitSettings.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JinaAIServiceSettings that = (JinaAIServiceSettings) o;
        return Objects.equals(uri, that.uri)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, modelId, rateLimitSettings);
    }
}

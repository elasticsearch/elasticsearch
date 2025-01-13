/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaifoundry.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.azureaifoundry.AzureAiFoundryConstants;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AzureAiFoundryEmbeddingsTaskSettingsTests extends AbstractBWCWireSerializationTestCase<AzureAiFoundryEmbeddingsTaskSettings> {
    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        Map<String, Object> newSettingsMap = new HashMap<>();
        if (newSettings.user() != null) {
            newSettingsMap.put(AzureAiFoundryConstants.USER_FIELD, newSettings.user());
        }
        AzureAiFoundryEmbeddingsTaskSettings updatedSettings = (AzureAiFoundryEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(newSettingsMap)
        );
        if (newSettings.user() == null) {
            assertEquals(initialSettings.user(), updatedSettings.user());
        } else {
            assertEquals(newSettings.user(), updatedSettings.user());
        }
    }

    public void testFromMap_WithUser() {
        assertEquals(
            new AzureAiFoundryEmbeddingsTaskSettings("user"),
            AzureAiFoundryEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(AzureAiFoundryConstants.USER_FIELD, "user")))
        );
    }

    public void testFromMap_UserIsEmptyString() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiFoundryEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(AzureAiFoundryConstants.USER_FIELD, "")))
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [task_settings] Invalid value empty string. [user] must be a non-empty string;"))
        );
    }

    public void testFromMap_MissingUser_DoesNotThrowException() {
        var taskSettings = AzureAiFoundryEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertNull(taskSettings.user());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var taskSettings = AzureAiFoundryEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(AzureAiFoundryConstants.USER_FIELD, "user")));

        var overriddenTaskSettings = AzureAiFoundryEmbeddingsTaskSettings.of(
            taskSettings,
            AzureAiFoundryEmbeddingsRequestTaskSettings.EMPTY_SETTINGS
        );
        MatcherAssert.assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOverrideWith_UsesOverriddenSettings() {
        var taskSettings = AzureAiFoundryEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(AzureAiFoundryConstants.USER_FIELD, "user")));

        var requestTaskSettings = AzureAiFoundryEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(Map.of(AzureAiFoundryConstants.USER_FIELD, "user2"))
        );

        var overriddenTaskSettings = AzureAiFoundryEmbeddingsTaskSettings.of(taskSettings, requestTaskSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new AzureAiFoundryEmbeddingsTaskSettings("user2")));
    }

    public void testToXContent_WithoutParameters() throws IOException {
        var settings = AzureAiFoundryEmbeddingsTaskSettings.fromMap(getTaskSettingsMap(null));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("{}"));
    }

    public void testToXContent_WithParameters() throws IOException {
        var settings = AzureAiFoundryEmbeddingsTaskSettings.fromMap(getTaskSettingsMap("testuser"));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"user":"testuser"}"""));
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable String user) {
        Map<String, Object> map = new HashMap<>();
        if (user != null) {
            map.put(AzureAiFoundryConstants.USER_FIELD, user);
        }
        return map;
    }

    @Override
    protected Writeable.Reader<AzureAiFoundryEmbeddingsTaskSettings> instanceReader() {
        return AzureAiFoundryEmbeddingsTaskSettings::new;
    }

    @Override
    protected AzureAiFoundryEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureAiFoundryEmbeddingsTaskSettings mutateInstance(AzureAiFoundryEmbeddingsTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, AzureAiFoundryEmbeddingsTaskSettingsTests::createRandom);
    }

    @Override
    protected AzureAiFoundryEmbeddingsTaskSettings mutateInstanceForVersion(
        AzureAiFoundryEmbeddingsTaskSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static AzureAiFoundryEmbeddingsTaskSettings createRandom() {
        return new AzureAiFoundryEmbeddingsTaskSettings(randomFrom(new String[] { null, randomAlphaOfLength(15) }));
    }
}

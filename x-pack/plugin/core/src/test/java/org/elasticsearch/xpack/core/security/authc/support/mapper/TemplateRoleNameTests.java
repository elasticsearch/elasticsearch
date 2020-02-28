/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authc.support.mapper;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptMetaData;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.StoredScriptSource;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName.Format;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionModel;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TemplateRoleNameTests extends ESTestCase {

    public void testParseRoles() throws Exception {
        final TemplateRoleName role1 = parse("{ \"template\": { \"source\": \"_user_{{username}}\" } }");
        assertThat(role1, Matchers.instanceOf(TemplateRoleName.class));
        assertThat(role1.getTemplate().utf8ToString(), equalTo("{\"source\":\"_user_{{username}}\"}"));
        assertThat(role1.getFormat(), equalTo(Format.STRING));

        final TemplateRoleName role2 = parse(
            "{ \"template\": \"{\\\"source\\\":\\\"{{#tojson}}groups{{/tojson}}\\\"}\", \"format\":\"json\" }");
        assertThat(role2, Matchers.instanceOf(TemplateRoleName.class));
        assertThat(role2.getTemplate().utf8ToString(),
            equalTo("{\"source\":\"{{#tojson}}groups{{/tojson}}\"}"));
        assertThat(role2.getFormat(), equalTo(Format.JSON));
    }

    public void testToXContent() throws Exception {
        final String json = "{" +
            "\"template\":\"{\\\"source\\\":\\\"" + randomAlphaOfLengthBetween(8, 24) + "\\\"}\"," +
            "\"format\":\"" + randomFrom(Format.values()).formatName() + "\"" +
            "}";
        assertThat(Strings.toString(parse(json)), equalTo(json));
    }

    public void testSerializeTemplate() throws Exception {
        trySerialize(new TemplateRoleName(new BytesArray(randomAlphaOfLengthBetween(12, 60)), randomFrom(Format.values())));
    }

    public void testEqualsAndHashCode() throws Exception {
        tryEquals(new TemplateRoleName(new BytesArray(randomAlphaOfLengthBetween(12, 60)), randomFrom(Format.values())));
    }

    public void testEvaluateRoles() throws Exception {
        final ScriptService scriptService = new ScriptService(Settings.EMPTY,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()), ScriptModule.CORE_CONTEXTS);
        final ExpressionModel model = new ExpressionModel();
        model.defineField("username", "hulk");
        model.defineField("groups", Arrays.asList("avengers", "defenders", "panthenon"));

        final TemplateRoleName plainString = new TemplateRoleName(new BytesArray("{ \"source\":\"heroes\" }"), Format.STRING);
        assertThat(plainString.getRoleNames(scriptService, model), contains("heroes"));

        final TemplateRoleName user = new TemplateRoleName(new BytesArray("{ \"source\":\"_user_{{username}}\" }"), Format.STRING);
        assertThat(user.getRoleNames(scriptService, model), contains("_user_hulk"));

        final TemplateRoleName groups = new TemplateRoleName(new BytesArray("{ \"source\":\"{{#tojson}}groups{{/tojson}}\" }"),
            Format.JSON);
        assertThat(groups.getRoleNames(scriptService, model), contains("avengers", "defenders", "panthenon"));
    }

    private TemplateRoleName parse(String json) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        final TemplateRoleName role = TemplateRoleName.parse(parser);
        assertThat(role, notNullValue());
        return role;
    }

    public void trySerialize(TemplateRoleName original) throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        final StreamInput rawInput = ByteBufferStreamInput.wrap(BytesReference.toBytes(output.bytes()));
        final TemplateRoleName serialized = new TemplateRoleName(rawInput);
        assertEquals(original, serialized);
    }

    public void tryEquals(TemplateRoleName original) {
        final EqualsHashCodeTestUtils.CopyFunction<TemplateRoleName> copy =
            rmt -> new TemplateRoleName(rmt.getTemplate(), rmt.getFormat());
        final EqualsHashCodeTestUtils.MutateFunction<TemplateRoleName> mutate = rmt -> {
            if (randomBoolean()) {
                return new TemplateRoleName(rmt.getTemplate(),
                    randomValueOtherThan(rmt.getFormat(), () -> randomFrom(Format.values())));
            } else {
                final String templateStr = rmt.getTemplate().utf8ToString();
                return new TemplateRoleName(new BytesArray(templateStr.substring(randomIntBetween(1, templateStr.length() / 2))),
                    rmt.getFormat());
            }
        };
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(original, copy, mutate);
    }

    public void testValidate() {
        final ScriptService scriptService = new ScriptService(Settings.EMPTY,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()), ScriptModule.CORE_CONTEXTS);

        final TemplateRoleName plainString = new TemplateRoleName(new BytesArray("{ \"source\":\"heroes\" }"), Format.STRING);
        plainString.validate(scriptService);

        final TemplateRoleName user = new TemplateRoleName(new BytesArray("{ \"source\":\"_user_{{username}}\" }"), Format.STRING);
        user.validate(scriptService);

        final TemplateRoleName groups = new TemplateRoleName(new BytesArray("{ \"source\":\"{{#tojson}}groups{{/tojson}}\" }"),
            Format.JSON);
        groups.validate(scriptService);

        final TemplateRoleName notObject = new TemplateRoleName(new BytesArray("heroes"), Format.STRING);
        expectThrows(IllegalArgumentException.class, () -> notObject.validate(scriptService));

        final TemplateRoleName invalidField = new TemplateRoleName(new BytesArray("{ \"foo\":\"heroes\" }"), Format.STRING);
        expectThrows(IllegalArgumentException.class, () -> invalidField.validate(scriptService));
    }

    public void testValidateWillPassWithEmptyContext() {
        final ScriptService scriptService = new ScriptService(Settings.EMPTY,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()), ScriptModule.CORE_CONTEXTS);

        final BytesReference template = new BytesArray("{ \"source\":\"" +
            "{{username}}/{{dn}}/{{realm}}/{{metadata}}" +
            "{{#realm}}" +
            "  {{name}}/{{type}}" +
            "{{/realm}}" +
            "{{#toJson}}groups{{/toJson}}" +
            "{{^groups}}{{.}}{{/groups}}" +
            "{{#metadata}}" +
            "  {{#first}}" +
            "    <li><strong>{{name}}</strong></li>" +
            "  {{/first}}" +
            "  {{#link}}" +
            "    <li><a href=\\\"{{url}}\\\">{{name}}</a></li>" +
            "  {{/link}}" +
            "  {{#toJson}}subgroups{{/toJson}}" +
            "  {{something-else}}" +
            "{{/metadata}}\" }");
        final TemplateRoleName templateRoleName = new TemplateRoleName(template, Format.STRING);
        templateRoleName.validate(scriptService);
    }

    public void testValidateWillFailForSyntaxError() {
        final ScriptService scriptService = new ScriptService(Settings.EMPTY,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()), ScriptModule.CORE_CONTEXTS);

        final BytesReference template = new BytesArray("{ \"source\":\" {{#not-closed}} {{other-variable}} \" }");

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new TemplateRoleName(template, Format.STRING).validate(scriptService));
        assertTrue(e.getCause() instanceof ScriptException);
    }

    public void testValidationWillFailWhenInlineScriptIsNotEnabled() {
        final Settings settings = Settings.builder().put("script.allowed_types", ScriptService.ALLOW_NONE).build();
        final ScriptService scriptService = new ScriptService(settings,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()), ScriptModule.CORE_CONTEXTS);
        final BytesReference inlineScript = new BytesArray("{ \"source\":\"\" }");
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new TemplateRoleName(inlineScript, Format.STRING).validate(scriptService));
        assertThat(e.getMessage(), containsString("[inline]"));
    }

    public void testValidateWillFailawhenStoredScriptIsNotEnabled() {
        final Settings settings = Settings.builder().put("script.allowed_types", ScriptService.ALLOW_NONE).build();
        final ScriptService scriptService = new ScriptService(settings,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()), ScriptModule.CORE_CONTEXTS);
        final ClusterChangedEvent clusterChangedEvent = mock(ClusterChangedEvent.class);
        final ClusterState clusterState = mock(ClusterState.class);
        final MetaData metaData = mock(MetaData.class);
        final StoredScriptSource storedScriptSource = mock(StoredScriptSource.class);
        final ScriptMetaData scriptMetaData = new ScriptMetaData.Builder(null).storeScript("foo", storedScriptSource).build();
        when(clusterChangedEvent.state()).thenReturn(clusterState);
        when(clusterState.metaData()).thenReturn(metaData);
        when(metaData.custom(ScriptMetaData.TYPE)).thenReturn(scriptMetaData);
        when(storedScriptSource.getLang()).thenReturn("mustache");
        when(storedScriptSource.getSource()).thenReturn("");
        when(storedScriptSource.getOptions()).thenReturn(Collections.emptyMap());
        scriptService.applyClusterState(clusterChangedEvent);

        final BytesReference storedScript = new BytesArray("{ \"id\":\"foo\" }");
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new TemplateRoleName(storedScript, Format.STRING).validate(scriptService));
        assertThat(e.getMessage(), containsString("[stored]"));
    }
}

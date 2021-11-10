/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptService;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.StoredScriptSource;
import org.elasticsearch.test.ESTestCase;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConditionalProcessorTests extends ESTestCase {

    private static final String scriptName = "conditionalScript";

    public void testChecksCondition() throws Exception {
        String conditionalField = "field1";
        String scriptName = "conditionalScript";
        String trueValue = "truthy";
        ScriptService scriptService = new ScriptService(
            Settings.builder().build(),
            Collections.singletonMap(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(
                    Script.DEFAULT_SCRIPT_LANG,
                    Collections.singletonMap(scriptName, ctx -> trueValue.equals(ctx.get(conditionalField))),
                    Collections.emptyMap()
                )
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );
        Map<String, Object> document = new HashMap<>();
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(1), 0L, TimeUnit.MILLISECONDS.toNanos(2));
        ConditionalProcessor processor = new ConditionalProcessor(
            randomAlphaOfLength(10),
            "description",
            new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap()),
            scriptService,
            new Processor() {
                @Override
                public IngestDocument execute(final IngestDocument ingestDocument) {
                    if (ingestDocument.hasField("error")) {
                        throw new RuntimeException("error");
                    }
                    ingestDocument.setFieldValue("foo", "bar");
                    return ingestDocument;
                }

                @Override
                public String getType() {
                    return null;
                }

                @Override
                public String getTag() {
                    return null;
                }

                @Override
                public String getDescription() {
                    return null;
                }
            },
            relativeTimeProvider
        );

        // false, never call processor never increments metrics
        String falseValue = "falsy";
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument.setFieldValue(conditionalField, falseValue);
        processor.execute(ingestDocument, (result, e) -> {});
        assertThat(ingestDocument.getSourceAndMetadata().get(conditionalField), is(falseValue));
        assertThat(ingestDocument.getSourceAndMetadata(), not(hasKey("foo")));
        assertStats(processor, 0, 0, 0);
        assertEquals(scriptName, processor.getCondition());

        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument.setFieldValue(conditionalField, falseValue);
        ingestDocument.setFieldValue("error", true);
        processor.execute(ingestDocument, (result, e) -> {});
        assertStats(processor, 0, 0, 0);

        // true, always call processor and increments metrics
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument.setFieldValue(conditionalField, trueValue);
        processor.execute(ingestDocument, (result, e) -> {});
        assertThat(ingestDocument.getSourceAndMetadata().get(conditionalField), is(trueValue));
        assertThat(ingestDocument.getSourceAndMetadata().get("foo"), is("bar"));
        assertStats(processor, 1, 0, 1);

        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument.setFieldValue(conditionalField, trueValue);
        ingestDocument.setFieldValue("error", true);
        IngestDocument finalIngestDocument = ingestDocument;
        Exception holder[] = new Exception[1];
        processor.execute(finalIngestDocument, (result, e) -> { holder[0] = e; });
        assertThat(holder[0], instanceOf(RuntimeException.class));
        assertStats(processor, 2, 1, 2);
    }

    @SuppressWarnings("unchecked")
    public void testActsOnImmutableData() throws Exception {
        assertMutatingCtxThrows(ctx -> ctx.remove("foo"));
        assertMutatingCtxThrows(ctx -> ctx.put("foo", "bar"));
        assertMutatingCtxThrows(ctx -> ((List<Object>) ctx.get("listField")).add("bar"));
        assertMutatingCtxThrows(ctx -> ((List<Object>) ctx.get("listField")).remove("bar"));
    }

    public void testTypeDeprecation() throws Exception {

        ScriptService scriptService = new ScriptService(
            Settings.builder().build(),
            Collections.singletonMap(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Collections.singletonMap(scriptName, ctx -> {
                    ctx.get("_type");
                    return true;
                }), Collections.emptyMap())
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(1), 0L, TimeUnit.MILLISECONDS.toNanos(2));
        ConditionalProcessor processor = new ConditionalProcessor(
            randomAlphaOfLength(10),
            "description",
            new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap()),
            scriptService,
            new Processor() {
                @Override
                public IngestDocument execute(final IngestDocument ingestDocument) {
                    return ingestDocument;
                }

                @Override
                public String getType() {
                    return null;
                }

                @Override
                public String getTag() {
                    return null;
                }

                @Override
                public String getDescription() {
                    return null;
                }
            },
            relativeTimeProvider
        );

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        processor.execute(ingestDocument, (result, e) -> {});
        assertWarnings("[types removal] Looking up doc types [_type] in scripts is deprecated.");
    }

    public void testPrecompiledError() {
        ScriptService scriptService = MockScriptService.singleContext(
            IngestConditionalScript.CONTEXT,
            code -> {
                throw new ScriptException("bad script", new ParseException("error", 0), org.elasticsearch.core.List.of(), "", "lang", null);
            },
            org.elasticsearch.core.Map.of()
        );
        Script script = new Script(ScriptType.INLINE, "lang", "foo", org.elasticsearch.core.Map.of());
        ScriptException e = expectThrows(ScriptException.class, () -> new ConditionalProcessor(null, null, script, scriptService, null));
        assertThat(e.getMessage(), equalTo("bad script"));
    }

    public void testRuntimeCompileError() {
        AtomicBoolean fail = new AtomicBoolean(false);
        Map<String, StoredScriptSource> storedScripts = new HashMap<>();
        storedScripts.put("foo", new StoredScriptSource("lang", "", org.elasticsearch.core.Map.of()));
        ScriptService scriptService = MockScriptService.singleContext(IngestConditionalScript.CONTEXT, code -> {
            if (fail.get()) {
                throw new ScriptException("bad script", new ParseException("error", 0), org.elasticsearch.core.List.of(), "", "lang", null);
            } else {
                return params -> new IngestConditionalScript(params) {
                    @Override
                    public boolean execute(Map<String, Object> ctx) {
                        return false;
                    }
                };
            }
        }, storedScripts);
        Script script = new Script(ScriptType.STORED, null, "foo", org.elasticsearch.core.Map.of());
        ConditionalProcessor processor = new ConditionalProcessor(null, null, script, scriptService, null);
        fail.set(true);
        // must change the script source or the cached version will be used
        storedScripts.put("foo", new StoredScriptSource("lang", "changed", org.elasticsearch.core.Map.of()));
        IngestDocument ingestDoc = new IngestDocument(org.elasticsearch.core.Map.of(), org.elasticsearch.core.Map.of());
        processor.execute(ingestDoc, (doc, e) -> { assertThat(e.getMessage(), equalTo("bad script")); });
    }

    public void testRuntimeError() {
        ScriptService scriptService = MockScriptService.singleContext(
            IngestConditionalScript.CONTEXT,
            code -> params -> new IngestConditionalScript(params) {
                @Override
                public boolean execute(Map<String, Object> ctx) {
                    throw new IllegalArgumentException("runtime problem");
                }
            },
            org.elasticsearch.core.Map.of()
        );
        Script script = new Script(ScriptType.INLINE, "lang", "foo", org.elasticsearch.core.Map.of());
        ConditionalProcessor processor = new ConditionalProcessor(null, null, script, scriptService, null);
        IngestDocument ingestDoc = new IngestDocument(org.elasticsearch.core.Map.of(), org.elasticsearch.core.Map.of());
        processor.execute(ingestDoc, (doc, e) -> { assertThat(e.getMessage(), equalTo("runtime problem")); });
    }

    private static void assertMutatingCtxThrows(Consumer<Map<String, Object>> mutation) throws Exception {
        String scriptName = "conditionalScript";
        CompletableFuture<Exception> expectedException = new CompletableFuture<>();
        ScriptService scriptService = new ScriptService(
            Settings.builder().build(),
            Collections.singletonMap(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Collections.singletonMap(scriptName, ctx -> {
                    try {
                        mutation.accept(ctx);
                    } catch (Exception e) {
                        expectedException.complete(e);
                    }
                    return false;
                }), Collections.emptyMap())
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );
        Map<String, Object> document = new HashMap<>();
        ConditionalProcessor processor = new ConditionalProcessor(
            randomAlphaOfLength(10),
            "desription",
            new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap()),
            scriptService,
            null
        );
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument.setFieldValue("listField", new ArrayList<>());
        processor.execute(ingestDocument, (result, e) -> {});
        Exception e = expectedException.get();
        assertThat(e, instanceOf(UnsupportedOperationException.class));
        assertEquals("Mutating ingest documents in conditionals is not supported", e.getMessage());
        assertStats(processor, 0, 0, 0);
    }

    private static void assertStats(ConditionalProcessor conditionalProcessor, long count, long failed, long time) {
        IngestStats.Stats stats = conditionalProcessor.getMetric().createStats();
        assertThat(stats.getIngestCount(), equalTo(count));
        assertThat(stats.getIngestCurrent(), equalTo(0L));
        assertThat(stats.getIngestFailedCount(), equalTo(failed));
        assertThat(stats.getIngestTimeInMillis(), greaterThanOrEqualTo(time));
    }
}

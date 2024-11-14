/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation.impl;

import org.elasticsearch.entitlement.bridge.InstrumentationTarget;
import org.elasticsearch.entitlement.instrumentation.CheckerMethod;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

@ESTestCase.WithoutSecurityManager
public class InstrumentationServiceImplTests extends ESTestCase {

    final InstrumentationService instrumentationService = new InstrumentationServiceImpl();

    static class TestTargetClass {}

    interface TestChecker {
        @InstrumentationTarget(className = "org/example/TestTargetClass", methodName = "staticMethod", isStatic = true)
        void checkStaticMethod(Class<?> clazz, int arg0, String arg1, Object arg2);

        @InstrumentationTarget(className = "org/example/TestTargetClass", methodName = "someMethod")
        void checkInstanceMethodNoArgs(Class<?> clazz, TestTargetClass that);

        @InstrumentationTarget(className = "org/example/TestTargetClass2", methodName = "someMethod2")
        void checkInstanceMethodWithArgs(Class<?> clazz, TestTargetClass that, int x, int y);
    }

    interface TestCheckerOverloads {
        @InstrumentationTarget(className = "org/example/TestTargetClass", methodName = "someOverloadedMethod", isStatic = true)
        void checkInstanceMethodWithOverload(Class<?> clazz, int x, int y);

        @InstrumentationTarget(className = "org/example/TestTargetClass", methodName = "someOverloadedMethod", isStatic = true)
        void checkInstanceMethodWithOverload(Class<?> clazz, int x, String y);
    }

    public void testInstrumentationTargetLookup() throws IOException, ClassNotFoundException {
        Map<MethodKey, CheckerMethod> methodsMap = instrumentationService.lookupMethodsToInstrument(TestChecker.class.getName());

        assertThat(methodsMap, aMapWithSize(3));
        assertThat(
            methodsMap,
            hasEntry(
                equalTo(
                    new MethodKey("org/example/TestTargetClass", "staticMethod", List.of("I", "java/lang/String", "java/lang/Object"), true)
                ),
                equalTo(
                    new CheckerMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestChecker",
                        "checkStaticMethod",
                        List.of("Ljava/lang/Class;", "I", "Ljava/lang/String;", "Ljava/lang/Object;")
                    )
                )
            )
        );
        assertThat(
            methodsMap,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "someMethod", List.of(), false)),
                equalTo(
                    new CheckerMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestChecker",
                        "checkInstanceMethodNoArgs",
                        List.of(
                            "Ljava/lang/Class;",
                            "Lorg/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetClass;"
                        )
                    )
                )
            )
        );
        assertThat(
            methodsMap,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass2", "someMethod2", List.of("I", "I"), false)),
                equalTo(
                    new CheckerMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestChecker",
                        "checkInstanceMethodWithArgs",
                        List.of(
                            "Ljava/lang/Class;",
                            "Lorg/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestTargetClass;",
                            "I",
                            "I"
                        )
                    )
                )
            )
        );
    }

    public void testInstrumentationTargetLookupWithOverloads() throws IOException, ClassNotFoundException {
        Map<MethodKey, CheckerMethod> methodsMap = instrumentationService.lookupMethodsToInstrument(TestCheckerOverloads.class.getName());

        assertThat(methodsMap, aMapWithSize(2));
        assertThat(
            methodsMap,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "someOverloadedMethod", List.of("I", "java/lang/String"), true)),
                equalTo(
                    new CheckerMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestCheckerOverloads",
                        "checkInstanceMethodWithOverload",
                        List.of("Ljava/lang/Class;", "I", "Ljava/lang/String;")
                    )
                )
            )
        );
        assertThat(
            methodsMap,
            hasEntry(
                equalTo(new MethodKey("org/example/TestTargetClass", "someOverloadedMethod", List.of("I", "I"), true)),
                equalTo(
                    new CheckerMethod(
                        "org/elasticsearch/entitlement/instrumentation/impl/InstrumentationServiceImplTests$TestCheckerOverloads",
                        "checkInstanceMethodWithOverload",
                        List.of("Ljava/lang/Class;", "I", "I")
                    )
                )
            )
        );
    }
}

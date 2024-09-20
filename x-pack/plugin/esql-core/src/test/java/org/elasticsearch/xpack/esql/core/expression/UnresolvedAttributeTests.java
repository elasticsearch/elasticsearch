/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

public class UnresolvedAttributeTests extends AbstractNodeTestCase<UnresolvedAttribute, Expression> {
    public static UnresolvedAttribute randomUnresolvedAttribute() {
        Source source = SourceTests.randomSource();
        String name = randomAlphaOfLength(5);
        NameId id = randomBoolean() ? null : new NameId();
        String unresolvedMessage = randomUnresolvedMessage();
        return new UnresolvedAttribute(source, name, randomAlphaOfLength(4), id, unresolvedMessage);
    }

    /**
     * A random qualifier. It is important that this be distinct
     * from the name and the qualifier for testing transform.
     */
    private static String randomUnresolvedMessage() {
        return randomAlphaOfLength(7);
    }

    @Override
    protected UnresolvedAttribute randomInstance() {
        return randomUnresolvedAttribute();
    }

    @Override
    protected UnresolvedAttribute mutate(UnresolvedAttribute a) {
        Supplier<UnresolvedAttribute> option = randomFrom(
            Arrays.asList(
                () -> new UnresolvedAttribute(
                    a.source(),
                    a.qualifier(),
                    randomValueOtherThan(a.name(), () -> randomAlphaOfLength(5)),
                    a.id(),
                    a.unresolvedMessage()
                ),
                () -> new UnresolvedAttribute(
                    a.source(),
                    a.qualifier(),
                    a.name(),
                    a.id(),
                    randomValueOtherThan(a.unresolvedMessage(), UnresolvedAttributeTests::randomUnresolvedMessage)
                ),
                () -> new UnresolvedAttribute(
                    a.source(),
                    randomValueOtherThan(a.qualifier(), () -> randomAlphaOfLength(5)),
                    a.name(),
                    a.id(),
                    a.unresolvedMessage()
                )
            )
        );
        return option.get();
    }

    @Override
    protected UnresolvedAttribute copy(UnresolvedAttribute a) {
        return new UnresolvedAttribute(a.source(), a.qualifier(), a.name(), a.id(), a.unresolvedMessage());
    }

    @Override
    public void testTransform() {
        UnresolvedAttribute a = randomUnresolvedAttribute();

        String newQualifier = randomValueOtherThan(a.qualifier(), () -> randomAlphaOfLength(3));
        assertEquals(
            new UnresolvedAttribute(a.source(), a.name(), newQualifier, a.id(), a.unresolvedMessage()),
            a.transformPropertiesOnly(Object.class, v -> Objects.equals(v, a.qualifier()) ? newQualifier : v)
        );

        String newName = randomValueOtherThan(a.name(), () -> randomAlphaOfLength(5));
        assertEquals(
            new UnresolvedAttribute(a.source(), newName, a.qualifier(), a.id(), a.unresolvedMessage()),
            a.transformPropertiesOnly(Object.class, v -> Objects.equals(v, a.name()) ? newName : v)
        );

        NameId newId = new NameId();
        assertEquals(
            new UnresolvedAttribute(a.source(), a.qualifier(), a.name(), newId, a.unresolvedMessage()),
            a.transformPropertiesOnly(Object.class, v -> Objects.equals(v, a.id()) ? newId : v)
        );

        String newMessage = randomValueOtherThan(a.unresolvedMessage(), UnresolvedAttributeTests::randomUnresolvedMessage);
        assertEquals(
            new UnresolvedAttribute(a.source(), a.qualifier(), a.name(), a.id(), newMessage),
            a.transformPropertiesOnly(Object.class, v -> Objects.equals(v, a.unresolvedMessage()) ? newMessage : v)
        );
    }

    @Override
    public void testReplaceChildren() {
        // UnresolvedAttribute doesn't have any children
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public enum ChunkedToXContentHelper {
    ;

    public static Iterator<ToXContent> startObject() {
        return Iterators.single(((builder, params) -> builder.startObject()));
    }

    public static Iterator<ToXContent> startObject(String name) {
        return Iterators.single(((builder, params) -> builder.startObject(name)));
    }

    public static Iterator<ToXContent> endObject() {
        return Iterators.single(((builder, params) -> builder.endObject()));
    }

    public static Iterator<ToXContent> startArray(String name) {
        return Iterators.single(((builder, params) -> builder.startArray(name)));
    }

    public static Iterator<ToXContent> endArray() {
        return Iterators.single(((builder, params) -> builder.endArray()));
    }

    public static <T> Iterator<ToXContent> map(String name, Map<String, T> map, Function<Map.Entry<String, T>, ToXContent> toXContent) {
        return Iterators.concat(startObject(name), map.entrySet().stream().map(toXContent).iterator(), endObject());
    }

    public static Iterator<ToXContent> map(String name, Map<String, ?> map) {
        return map(name, map, entry -> (ToXContent) (builder, params) -> builder.field(entry.getKey(), entry.getValue()));
    }

    public static Iterator<ToXContent> field(String name, boolean value) {
        return Iterators.single(((builder, params) -> builder.field(name, value)));
    }
}

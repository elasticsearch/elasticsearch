/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.common;

import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Output formats supported by cartesian fields.
 */
public class CartesianFormatsFactory {

    public static final String GEOJSON = "geojson";
    public static final String WKT = "wkt";

    private static final Map<String, Function<Geometry, Object>> FORMATS = new HashMap<>();
    static {
        FORMATS.put(GEOJSON, GeoJson::toMap);
        FORMATS.put(WKT, WellKnownText.INSTANCE::toWKT);
    }

    /**
     * Returns a formatter by name
     */
    public static Function<Geometry, Object> getFormat(String name) {
        Function<Geometry, Object> format = FORMATS.get(name);
        if (format == null) {
            throw new IllegalArgumentException("Unrecognized geometry format [" + format + "].");
        }
        return format;
    }
}

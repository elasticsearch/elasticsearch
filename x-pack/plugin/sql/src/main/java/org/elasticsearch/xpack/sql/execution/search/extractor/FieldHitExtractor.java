/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.GeoShape;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Extractor for ES fields. Works for both 'normal' fields but also nested ones (which require hitName to be set).
 * The latter is used as metadata in assembling the results in the tabular response.
 */
public class FieldHitExtractor implements HitExtractor {

    /**
     * Stands for {@code field}. We try to use short names for {@link HitExtractor}s
     * to save a few bytes when when we send them back to the user.
     */
    static final String NAME = "f";

    /**
     * Source extraction requires only the (relative) field name, without its parent path.
     */
    private static String[] sourcePath(String name, boolean useDocValue, String hitName) {
        return useDocValue ? Strings.EMPTY_ARRAY : Strings
                .tokenizeToStringArray(hitName == null ? name : name.substring(hitName.length() + 1), ".");
    }

    private final String fieldName, hitName;
    private final DataType dataType;
    private final boolean useDocValue;
    private final boolean arrayLeniency;
    private final String[] path;

    public FieldHitExtractor(String name, DataType dataType, boolean useDocValue) {
        this(name, dataType, useDocValue, null, false);
    }

    public FieldHitExtractor(String name, DataType dataType, boolean useDocValue, boolean arrayLeniency) {
        this(name, dataType, useDocValue, null, arrayLeniency);
    }

    public FieldHitExtractor(String name, DataType dataType, boolean useDocValue, String hitName, boolean arrayLeniency) {
        this.fieldName = name;
        this.dataType = dataType;
        this.useDocValue = useDocValue;
        this.arrayLeniency = arrayLeniency;
        this.hitName = hitName;

        if (hitName != null) {
            if (!name.contains(hitName)) {
                throw new SqlIllegalArgumentException("Hitname [{}] specified but not part of the name [{}]", hitName, name);
            }
        }

        this.path = sourcePath(fieldName, useDocValue, hitName);
    }

    FieldHitExtractor(StreamInput in) throws IOException {
        fieldName = in.readString();
        String esType = in.readOptionalString();
        dataType = esType != null ? DataType.fromTypeName(esType) : null;
        useDocValue = in.readBoolean();
        hitName = in.readOptionalString();
        arrayLeniency = in.readBoolean();
        path = sourcePath(fieldName, useDocValue, hitName);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeOptionalString(dataType == null ? null : dataType.typeName);
        out.writeBoolean(useDocValue);
        out.writeOptionalString(hitName);
        out.writeBoolean(arrayLeniency);
    }

    @Override
    public Object extract(SearchHit hit) {
        Object value = null;
        if (useDocValue) {
            DocumentField field = hit.field(fieldName);
            if (field != null) {
                value = unwrapMultiValue(field.getValues());
            }
        } else {
            Map<String, Object> source = hit.getSourceAsMap();
            if (source != null) {
                value = extractFromSource(source);
            }
        }
        return value;
    }

    private Object unwrapMultiValue(Object values) {
        if (values == null) {
            return null;
        }
        if (dataType == DataType.GEO_POINT) {
            Object value;
            if (values instanceof List && ((List<?>) values).size() == 1) {
                    value = ((List<?>) values).get(0);
            } else {
                value = values;
            }
            try {
                GeoPoint geoPoint = GeoUtils.parseGeoPoint(value, true);
                return new GeoShape(geoPoint.lon(), geoPoint.lat());
            } catch (ElasticsearchParseException ex) {
                throw new SqlIllegalArgumentException("Cannot parse geo_point value (returned by [{}])", fieldName);
            }
        }
        if (dataType == DataType.GEO_SHAPE) {
            Object value;
            if (values instanceof List && ((List<?>) values).size() == 1) {
                value = ((List<?>) values).get(0);
            } else {
                value = values;
            }
            try {
                return new GeoShape(value);
            } catch (IOException ex) {
                throw new SqlIllegalArgumentException("Cannot read geo_shape value (returned by [{}])", fieldName);
            }
        }
        if (values instanceof List) {
            List<?> list = (List<?>) values;
            if (list.isEmpty()) {
                return null;
            } else {
                if (arrayLeniency || list.size() == 1) {
                    return unwrapMultiValue(list.get(0));
                } else {
                    throw new SqlIllegalArgumentException("Arrays (returned by [{}]) are not supported", fieldName);
                }
            }
        }
        if (values instanceof Map) {
            throw new SqlIllegalArgumentException("Objects (returned by [{}]) are not supported", fieldName);
        }
        if (dataType == DataType.DATETIME) {
            if (values instanceof String) {
                return DateUtils.asDateTime(Long.parseLong(values.toString()));
            }
        }
        if (values instanceof Long || values instanceof Double || values instanceof String || values instanceof Boolean) {
            return values;
        }
        throw new SqlIllegalArgumentException("Type {} (returned by [{}]) is not supported", values.getClass().getSimpleName(), fieldName);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    Object extractFromSource(Map<String, Object> map) {
        Object value = null;

        // Used to avoid recursive method calls
        // Holds the sub-maps in the document hierarchy that are pending to be inspected along with the current index of the `path`.
        Deque<Tuple<Integer, Map<String, Object>>> queue = new ArrayDeque<>();
        queue.add(new Tuple<>(-1, map));

        while (!queue.isEmpty()) {
            Tuple<Integer, Map<String, Object>> tuple = queue.removeLast();
            int idx = tuple.v1();
            Map<String, Object> subMap = tuple.v2();

            // Find all possible entries by examining all combinations under the current level ("idx") of the "path"
            // e.g.: If the path == "a.b.c.d" and the idx == 0, we need to check the current subMap against the keys:
            //       "b", "b.c" and "b.c.d"
            StringJoiner sj = new StringJoiner(".");
            for (int i = idx + 1; i < path.length; i++) {
                sj.add(path[i]);
                Object node = subMap.get(sj.toString());
                
                if (node instanceof List) {
                    List listOfValues = (List) node;
                    if (listOfValues.size() == 1) {
                        // this is a List with a size of 1 e.g.: {"a" : [{"b" : "value"}]} meaning the JSON is a list with one element
                        // or a list of values with one element e.g.: {"a": {"b" : ["value"]}}
                        node = listOfValues.get(0);
                    } else {
                        // a List of elements with more than one value. Break early and let unwrapMultiValue deal with the list
                        return unwrapMultiValue(node);
                    }
                }
                
                if (node instanceof Map) {
                    if (i < path.length - 1) {
                        // Add the sub-map to the queue along with the current path index
                        queue.add(new Tuple<>(i, (Map<String, Object>) node));
                    } else {
                        // We exhausted the path and got a map
                        // If it is an object - it will be handled in the value extractor
                        value = node;
                    }
                } else if (node != null) {
                    if (i < path.length - 1) {
                        // If we reach a concrete value without exhausting the full path, something is wrong with the mapping
                        // e.g.: map is {"a" : { "b" : "value }} and we are looking for a path: "a.b.c.d"
                        throw new SqlIllegalArgumentException("Cannot extract value [{}] from source", fieldName);
                    }
                    if (value != null) {
                        // A value has already been found so this means that there are more than one
                        // values in the document for the same path but different hierarchy.
                        // e.g.: {"a" : {"b" : {"c" : "value"}}}, {"a.b" : {"c" : "value"}}, ...
                        throw new SqlIllegalArgumentException("Multiple values (returned by [{}]) are not supported", fieldName);
                    }
                    value = node;
                }
            }
        }
        return unwrapMultiValue(value);
    }

    @Override
    public String hitName() {
        return hitName;
    }

    public String fieldName() {
        return fieldName;
    }

    @Override
    public String toString() {
        return fieldName + "@" + hitName;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        FieldHitExtractor other = (FieldHitExtractor) obj;
        return fieldName.equals(other.fieldName)
                && hitName.equals(other.hitName)
                && useDocValue == other.useDocValue
                && arrayLeniency == other.arrayLeniency;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, useDocValue, hitName, arrayLeniency);
    }
}

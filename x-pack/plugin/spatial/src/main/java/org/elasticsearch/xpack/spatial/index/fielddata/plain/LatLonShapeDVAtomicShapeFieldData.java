/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.script.field.ToScriptField;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

final class LatLonShapeDVAtomicShapeFieldData extends AbstractAtomicGeoShapeShapeFieldData {
    private final LeafReader reader;
    private final String fieldName;

    LatLonShapeDVAtomicShapeFieldData(LeafReader reader, String fieldName, ToScriptField<GeoShapeValues> toScriptField) {
        super(toScriptField);
        this.reader = reader;
        this.fieldName = fieldName;
    }

    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by lucene
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    public void close() {
        // noop
    }

    @Override
    public GeoShapeValues getGeoShapeValues() {
        try {
            final BinaryDocValues binaryValues = DocValues.getBinary(reader, fieldName);
            final GeoShapeValues.GeoShapeValue geoShapeValue = new GeoShapeValues.GeoShapeValue();
            return new GeoShapeValues() {

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return binaryValues.advanceExact(doc);
                }

                @Override
                public ValuesSourceType valuesSourceType() {
                    return GeoShapeValuesSourceType.instance();
                }

                @Override
                public GeoShapeValue value() throws IOException {
                    geoShapeValue.reset(binaryValues.binaryValue());
                    return geoShapeValue;
                }
            };
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }
}

/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.geo;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.xcontent.XContentGeoPointFieldMapper;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
// LUCENE MONITOR: Monitor against FieldComparator.Double
public class GeoDistanceDataComparator extends FieldComparator {

    public static FieldComparatorSource comparatorSource(String fieldName, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                                                         FieldDataCache fieldDataCache, MapperService mapperService) {
        return new InnerSource(fieldName, lat, lon, unit, geoDistance, fieldDataCache, mapperService);
    }

    private static class InnerSource extends FieldComparatorSource {

        protected final String fieldName;

        protected final double lat;

        protected final double lon;

        protected final DistanceUnit unit;

        protected final GeoDistance geoDistance;

        protected final FieldDataCache fieldDataCache;

        private final MapperService mapperService;

        private InnerSource(String fieldName, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                            FieldDataCache fieldDataCache, MapperService mapperService) {
            this.fieldName = fieldName;
            this.lat = lat;
            this.lon = lon;
            this.unit = unit;
            this.geoDistance = geoDistance;
            this.fieldDataCache = fieldDataCache;
            this.mapperService = mapperService;
        }

        @Override public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
            return new GeoDistanceDataComparator(numHits, fieldname, lat, lon, unit, geoDistance, fieldDataCache, mapperService);
        }
    }

    protected final String fieldName;

    protected final String indexLatFieldName;

    protected final String indexLonFieldName;

    protected final double lat;

    protected final double lon;

    protected final DistanceUnit unit;

    protected final GeoDistance geoDistance;

    protected final FieldDataCache fieldDataCache;

    protected final FieldData.Type fieldDataType;

    protected NumericFieldData latFieldData;

    protected NumericFieldData lonFieldData;


    private final double[] values;
    private double bottom;

    public GeoDistanceDataComparator(int numHits, String fieldName, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                                     FieldDataCache fieldDataCache, MapperService mapperService) {
        values = new double[numHits];

        this.fieldName = fieldName;
        this.lat = lat;
        this.lon = lon;
        this.unit = unit;
        this.geoDistance = geoDistance;
        this.fieldDataCache = fieldDataCache;

        FieldMapper mapper = mapperService.smartNameFieldMapper(fieldName + XContentGeoPointFieldMapper.Names.LAT_SUFFIX);
        if (mapper == null) {
            throw new ElasticSearchIllegalArgumentException("No mapping found for field [" + fieldName + "] for geo distance sort");
        }
        this.indexLatFieldName = mapper.names().indexName();

        mapper = mapperService.smartNameFieldMapper(fieldName + XContentGeoPointFieldMapper.Names.LON_SUFFIX);
        if (mapper == null) {
            throw new ElasticSearchIllegalArgumentException("No mapping found for field [" + fieldName + "] for geo distance sort");
        }
        this.indexLonFieldName = mapper.names().indexName();
        this.fieldDataType = mapper.fieldDataType();
    }

    @Override public void setNextReader(IndexReader reader, int docBase) throws IOException {
        latFieldData = (NumericFieldData) fieldDataCache.cache(fieldDataType, reader, indexLatFieldName);
        lonFieldData = (NumericFieldData) fieldDataCache.cache(fieldDataType, reader, indexLonFieldName);
    }

    @Override public int compare(int slot1, int slot2) {
        final double v1 = values[slot1];
        final double v2 = values[slot2];
        if (v1 > v2) {
            return 1;
        } else if (v1 < v2) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override public int compareBottom(int doc) {
        double distance;
        if (!latFieldData.hasValue(doc) || !lonFieldData.hasValue(doc)) {
            // is this true? push this to the "end"
            distance = Double.MAX_VALUE;
        } else {
            distance = geoDistance.calculate(lat, lon, latFieldData.doubleValue(doc), lonFieldData.doubleValue(doc), unit);
        }
        final double v2 = distance;
        if (bottom > v2) {
            return 1;
        } else if (bottom < v2) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override public void copy(int slot, int doc) {
        double distance;
        if (!latFieldData.hasValue(doc) || !lonFieldData.hasValue(doc)) {
            // is this true? push this to the "end"
            distance = Double.MAX_VALUE;
        } else {
            distance = geoDistance.calculate(lat, lon, latFieldData.doubleValue(doc), lonFieldData.doubleValue(doc), unit);
        }
        values[slot] = distance;
    }

    @Override public void setBottom(final int bottom) {
        this.bottom = values[bottom];
    }

    @Override public Comparable value(int slot) {
        return Double.valueOf(values[slot]);
    }
}

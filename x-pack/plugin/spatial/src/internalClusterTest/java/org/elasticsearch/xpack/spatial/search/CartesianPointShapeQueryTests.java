/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class CartesianPointShapeQueryTests extends CartesianPointShapeQueryTestCase {

    @Override
    protected void createMapping(String indexName, String fieldName, Settings settings) throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(fieldName)
            .field("type", "point")
            .endObject()
            .endObject()
            .endObject();
        client().admin().indices().prepareCreate(indexName).setMapping(xcb).setSettings(settings).get();
    }

    public void testFieldAlias() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject(defaultFieldName)
                .field("type", "point")
                .endObject()
                .startObject("alias")
                .field("type", "alias")
                .field("path", defaultFieldName)
                .endObject()
                .endObject()
                .endObject()
        );

        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        Point point = GeometryTestUtils.randomPoint(false);
        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, WellKnownText.toWKT(point)).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName).setQuery(geoShapeQuery("alias", point)).get();
        assertEquals(1, response.getHits().getTotalHits().value);
    }

    /**
     * Produce an array of objects each representing a single point in a variety of
     * supported point formats. For `geo_shape` we only support GeoJSON and WKT,
     * while for `geo_point` we support a variety of additional special case formats.
     * Therefor we define here sample data for <code>double[]{lon,lat}</code> as well as
     * a string "lat,lon".
     */
    @Override
    protected Object[] samplePointDataMultiFormat(Point pointA, Point pointB, Point pointC, Point pointD) {
        String str = "" + pointA.getLat() + ", " + pointA.getLon();
        String wkt = WellKnownText.toWKT(pointB);
        double[] pointDoubles = new double[] { pointC.getLon(), pointC.getLat() };
        Map<String, Object> geojson = GeoJson.toMap(pointD);
        return new Object[] { str, wkt, pointDoubles, geojson };
    }
}

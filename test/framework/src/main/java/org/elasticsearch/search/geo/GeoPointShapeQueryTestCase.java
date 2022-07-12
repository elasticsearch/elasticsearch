/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.geo;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;

import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public abstract class GeoPointShapeQueryTestCase extends BasePointShapeQueryTestCase<GeoShapeQueryBuilder> {

    private final SpatialQueryBuilders<GeoShapeQueryBuilder> geoShapeQueryBuilder = SpatialQueryBuilders.GEO;

    @Override
    protected SpatialQueryBuilders<GeoShapeQueryBuilder> queryBuilder() {
        return geoShapeQueryBuilder;
    }

    @Override
    protected String fieldTypeName() {
        return "geo_shape";
    }

    public void testRectangleSpanningDateline() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-169 0)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex(defaultIndexName)
            .setId("2")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-179 0)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex(defaultIndexName)
            .setId("3")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(171 0)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Rectangle rectangle = new Rectangle(169, -178, 1, -1);

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery(defaultFieldName, rectangle);
        SearchResponse response = client().prepareSearch(defaultIndexName).setQuery(geoShapeQueryBuilder).get();
        SearchHits searchHits = response.getHits();
        assertEquals(2, searchHits.getTotalHits().value);
        assertNotEquals("1", searchHits.getAt(0).getId());
        assertNotEquals("1", searchHits.getAt(1).getId());
    }

    public void testPolygonSpanningDateline() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-169 7)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex(defaultIndexName)
            .setId("2")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-179 7)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex(defaultIndexName)
            .setId("3")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(179 7)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex(defaultIndexName)
            .setId("4")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(171 7)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Polygon polygon = new Polygon(new LinearRing(new double[] { -177, 177, 177, -177, -177 }, new double[] { 10, 10, 5, 5, 10 }));

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery(defaultFieldName, polygon);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse response = client().prepareSearch(defaultIndexName).setQuery(geoShapeQueryBuilder).get();
        SearchHits searchHits = response.getHits();
        assertEquals(2, searchHits.getTotalHits().value);
        assertNotEquals("1", searchHits.getAt(0).getId());
        assertNotEquals("4", searchHits.getAt(0).getId());
        assertNotEquals("1", searchHits.getAt(1).getId());
        assertNotEquals("4", searchHits.getAt(1).getId());
    }

    public void testMultiPolygonSpanningDateline() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-169 7)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex(defaultIndexName)
            .setId("2")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(-179 7)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex(defaultIndexName)
            .setId("3")
            .setSource(jsonBuilder().startObject().field(defaultFieldName, "POINT(171 7)").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Polygon polygon1 = new Polygon(new LinearRing(new double[] { -167, -171, 171, -167, -167 }, new double[] { 10, 10, 5, 5, 10 }));

        Polygon polygon2 = new Polygon(new LinearRing(new double[] { -177, 177, 177, -177, -177 }, new double[] { 10, 10, 5, 5, 10 }));

        MultiPolygon multiPolygon = new MultiPolygon(List.of(polygon1, polygon2));

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery(defaultFieldName, multiPolygon);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse response = client().prepareSearch(defaultIndexName).setQuery(geoShapeQueryBuilder).get();
        SearchHits searchHits = response.getHits();
        assertEquals(2, searchHits.getTotalHits().value);
        assertNotEquals("3", searchHits.getAt(0).getId());
        assertNotEquals("3", searchHits.getAt(1).getId());
    }
}

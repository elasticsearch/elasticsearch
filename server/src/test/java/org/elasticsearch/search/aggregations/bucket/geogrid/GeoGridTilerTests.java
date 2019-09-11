/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoShapeCoordinateEncoder;
import org.elasticsearch.common.geo.GeometryTreeReader;
import org.elasticsearch.common.geo.GeometryTreeWriter;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

// TODO(talevy): more tests
public class GeoGridTilerTests extends ESTestCase {
    private static final GeoGridTiler.GeoTileGridTiler GEOTILE = GeoGridTiler.GeoTileGridTiler.INSTANCE;
    private static final GeoGridTiler.GeoHashGridTiler GEOHASH = GeoGridTiler.GeoHashGridTiler.INSTANCE;

    public void testGeoTile() throws Exception {
        double x = randomDouble();
        double y = randomDouble();
        int precision = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        assertThat(GEOTILE.encode(x, y, precision), equalTo(GeoTileUtils.longEncode(x, y, precision)));

        // create rectangle within tile and check bound counts
        Rectangle tile = GeoTileUtils.toBoundingBox(1309, 3166, 13);
        GeometryTreeWriter writer = new GeometryTreeWriter(
            new Rectangle(tile.getMinX() + 0.00001, tile.getMaxX() - 0.00001,
                tile.getMaxY() - 0.00001,  tile.getMinY() + 0.00001), GeoShapeCoordinateEncoder.INSTANCE);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef(), GeoShapeCoordinateEncoder.INSTANCE);
        MultiGeoValues.GeoShapeValue value =  new MultiGeoValues.GeoShapeValue(reader);

        long[] values = new long[16];

        // test shape within tile bounds
        {
            int count = GEOTILE.setValues(values, value, 13);
            assertThat(GEOTILE.getBoundingTileCount(value, 13), equalTo(1));
            assertThat(count, equalTo(1));
        }
        {
            int count = GEOTILE.setValues(values, value, 14);
            assertThat(GEOTILE.getBoundingTileCount(value, 14), equalTo(4));
            assertThat(count, equalTo(4));
        }
        {
            int count = GEOTILE.setValues(values, value, 15);
            assertThat(GEOTILE.getBoundingTileCount(value, 15), equalTo(16));
            assertThat(count, equalTo(16));
        }
    }

    public void testGeoHash() throws Exception {
        double x = randomDouble();
        double y = randomDouble();
        int precision = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        assertThat(GEOHASH.encode(x, y, precision), equalTo(Geohash.longEncode(x, y, precision)));

        Rectangle tile = Geohash.toBoundingBox(Geohash.stringEncode(x, y, 5));

        GeometryTreeWriter writer = new GeometryTreeWriter(
            new Rectangle(tile.getMinX() + 0.00001, tile.getMaxX() - 0.00001,
                tile.getMaxY() - 0.00001,  tile.getMinY() + 0.00001), GeoShapeCoordinateEncoder.INSTANCE);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef(), GeoShapeCoordinateEncoder.INSTANCE);
        MultiGeoValues.GeoShapeValue value =  new MultiGeoValues.GeoShapeValue(reader);

        long[] values = new long[1024];

        // test shape within tile bounds
        {
            int count = GEOHASH.setValues(values, value, 5);
            assertThat(GEOHASH.getBoundingTileCount(value, 5), equalTo(1));
            assertThat(count, equalTo(1));
        }
        {
            int count = GEOHASH.setValues(values, value, 6);
            assertThat(GEOHASH.getBoundingTileCount(value, 6), equalTo(32));
            assertThat(count, equalTo(32));
        }
        {
            int count = GEOHASH.setValues(values, value, 7);
            assertThat(GEOHASH.getBoundingTileCount(value, 7), equalTo(1024));
            assertThat(count, equalTo(1024));
        }
    }
}

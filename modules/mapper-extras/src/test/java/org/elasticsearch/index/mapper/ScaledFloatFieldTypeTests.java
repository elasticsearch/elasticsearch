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

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.io.IOException;
import java.util.Arrays;

public class ScaledFloatFieldTypeTests extends FieldTypeTestCase {


    public void testTermQuery() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft
            = new ScaledFloatFieldMapper.ScaledFloatFieldType("scaled_float", 0.1 + randomDouble() * 100);
        double value = (randomDouble() * 2 - 1) * 10000;
        long scaledValue = Math.round(value * ft.getScalingFactor());
        assertEquals(LongPoint.newExactQuery("scaled_float", scaledValue), ft.termQuery(value, null));
    }

    public void testTermsQuery() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft
            = new ScaledFloatFieldMapper.ScaledFloatFieldType("scaled_float", 0.1 + randomDouble() * 100);
        double value1 = (randomDouble() * 2 - 1) * 10000;
        long scaledValue1 = Math.round(value1 * ft.getScalingFactor());
        double value2 = (randomDouble() * 2 - 1) * 10000;
        long scaledValue2 = Math.round(value2 * ft.getScalingFactor());
        assertEquals(
                LongPoint.newSetQuery("scaled_float", scaledValue1, scaledValue2),
                ft.termsQuery(Arrays.asList(value1, value2), null));
    }

    public void testRangeQuery() throws IOException {
        // make sure the accuracy loss of scaled floats only occurs at index time
        // this test checks that searching scaled floats yields the same results as
        // searching doubles that are rounded to the closest half float
        ScaledFloatFieldMapper.ScaledFloatFieldType ft
            = new ScaledFloatFieldMapper.ScaledFloatFieldType("scaled_float", 0.1 + randomDouble() * 100);
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        final int numDocs = 1000;
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            double value = (randomDouble() * 2 - 1) * 10000;
            long scaledValue = Math.round(value * ft.getScalingFactor());
            double rounded = scaledValue / ft.getScalingFactor();
            doc.add(new LongPoint("scaled_float", scaledValue));
            doc.add(new DoublePoint("double", rounded));
            w.addDocument(doc);
        }
        final DirectoryReader reader = DirectoryReader.open(w);
        w.close();
        IndexSearcher searcher = newSearcher(reader);
        final int numQueries = 1000;
        for (int i = 0; i < numQueries; ++i) {
            Double l = randomBoolean() ? null : (randomDouble() * 2 - 1) * 10000;
            Double u = randomBoolean() ? null : (randomDouble() * 2 - 1) * 10000;
            boolean includeLower = randomBoolean();
            boolean includeUpper = randomBoolean();
            Query doubleQ = NumberFieldMapper.NumberType.DOUBLE.rangeQuery("double", l, u, includeLower, includeUpper, false, MOCK_QSC);
            Query scaledFloatQ = ft.rangeQuery(l, u, includeLower, includeUpper, MOCK_QSC);
            assertEquals(searcher.count(doubleQ), searcher.count(scaledFloatQ));
        }
        IOUtils.close(reader, dir);
    }

    public void testRoundsUpperBoundCorrectly() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft
            = new ScaledFloatFieldMapper.ScaledFloatFieldType("scaled_float", 100);
        Query scaledFloatQ = ft.rangeQuery(null, 0.1, true, false, MOCK_QSC);
        assertEquals("scaled_float:[-9223372036854775808 TO 9]", scaledFloatQ.toString());
        scaledFloatQ = ft.rangeQuery(null, 0.1, true, true, MOCK_QSC);
        assertEquals("scaled_float:[-9223372036854775808 TO 10]", scaledFloatQ.toString());
        scaledFloatQ = ft.rangeQuery(null, 0.095, true, false, MOCK_QSC);
        assertEquals("scaled_float:[-9223372036854775808 TO 9]", scaledFloatQ.toString());
        scaledFloatQ = ft.rangeQuery(null, 0.095, true, true, MOCK_QSC);
        assertEquals("scaled_float:[-9223372036854775808 TO 9]", scaledFloatQ.toString());
        scaledFloatQ = ft.rangeQuery(null, 0.105, true, false, MOCK_QSC);
        assertEquals("scaled_float:[-9223372036854775808 TO 10]", scaledFloatQ.toString());
        scaledFloatQ = ft.rangeQuery(null, 0.105, true, true, MOCK_QSC);
        assertEquals("scaled_float:[-9223372036854775808 TO 10]", scaledFloatQ.toString());
        scaledFloatQ = ft.rangeQuery(null, 79.99, true, true, MOCK_QSC);
        assertEquals("scaled_float:[-9223372036854775808 TO 7999]", scaledFloatQ.toString());
    }

    public void testRoundsLowerBoundCorrectly() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft
            = new ScaledFloatFieldMapper.ScaledFloatFieldType("scaled_float", 100);
        Query scaledFloatQ = ft.rangeQuery(-0.1, null, false, true, MOCK_QSC);
        assertEquals("scaled_float:[-9 TO 9223372036854775807]", scaledFloatQ.toString());
        scaledFloatQ = ft.rangeQuery(-0.1, null, true, true, MOCK_QSC);
        assertEquals("scaled_float:[-10 TO 9223372036854775807]", scaledFloatQ.toString());
        scaledFloatQ = ft.rangeQuery(-0.095, null, false, true, MOCK_QSC);
        assertEquals("scaled_float:[-9 TO 9223372036854775807]", scaledFloatQ.toString());
        scaledFloatQ = ft.rangeQuery(-0.095, null, true, true, MOCK_QSC);
        assertEquals("scaled_float:[-9 TO 9223372036854775807]", scaledFloatQ.toString());
        scaledFloatQ = ft.rangeQuery(-0.105, null, false, true, MOCK_QSC);
        assertEquals("scaled_float:[-10 TO 9223372036854775807]", scaledFloatQ.toString());
        scaledFloatQ = ft.rangeQuery(-0.105, null, true, true, MOCK_QSC);
        assertEquals("scaled_float:[-10 TO 9223372036854775807]", scaledFloatQ.toString());
    }

    public void testValueForSearch() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft
            = new ScaledFloatFieldMapper.ScaledFloatFieldType("scaled_float", 0.1 + randomDouble() * 100);
        assertNull(ft.valueForDisplay(null));
        assertEquals(10/ft.getScalingFactor(), ft.valueForDisplay(10L));
    }

    public void testFieldData() throws IOException {
        double scalingFactor = 0.1 + randomDouble() * 100;
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("scaled_float1", 10));
        doc.add(new SortedNumericDocValuesField("scaled_float2", 5));
        doc.add(new SortedNumericDocValuesField("scaled_float2", 12));
        w.addDocument(doc);
        try (DirectoryReader reader = DirectoryReader.open(w)) {
            IndexMetadata indexMetadata = new IndexMetadata.Builder("index").settings(
                    Settings.builder()
                    .put("index.version.created", Version.CURRENT)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0).build()).build();
            IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

            // single-valued
            ScaledFloatFieldMapper.ScaledFloatFieldType f1
                = new ScaledFloatFieldMapper.ScaledFloatFieldType("scaled_float1", scalingFactor);
            IndexNumericFieldData fielddata = (IndexNumericFieldData) f1.fielddataBuilder("index")
                .build(indexSettings, f1, null, null, null);
            assertEquals(fielddata.getNumericType(), IndexNumericFieldData.NumericType.DOUBLE);
            LeafNumericFieldData leafFieldData = fielddata.load(reader.leaves().get(0));
            SortedNumericDoubleValues values = leafFieldData.getDoubleValues();
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(10/f1.getScalingFactor(), values.nextValue(), 10e-5);

            // multi-valued
            ScaledFloatFieldMapper.ScaledFloatFieldType f2
                = new ScaledFloatFieldMapper.ScaledFloatFieldType("scaled_float2", scalingFactor);
            fielddata = (IndexNumericFieldData) f2.fielddataBuilder("index").build(indexSettings, f2, null, null, null);
            leafFieldData = fielddata.load(reader.leaves().get(0));
            values = leafFieldData.getDoubleValues();
            assertTrue(values.advanceExact(0));
            assertEquals(2, values.docValueCount());
            assertEquals(5/f2.getScalingFactor(), values.nextValue(), 10e-5);
            assertEquals(12/f2.getScalingFactor(), values.nextValue(), 10e-5);
        }
        IOUtils.close(w, dir);
    }
}

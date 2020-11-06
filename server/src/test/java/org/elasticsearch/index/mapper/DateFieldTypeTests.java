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

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.query.DateRangeIncludingNowQuery;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;

public class DateFieldTypeTests extends FieldTypeTestCase {

    private static final long nowInMillis = 0;

    public void testIsFieldWithinRangeEmptyReader() throws IOException {
        QueryRewriteContext context = new QueryRewriteContext(xContentRegistry(), writableRegistry(), null, () -> nowInMillis);
        IndexReader reader = new MultiReader();
        DateFieldType ft = new DateFieldType("my_date");
        assertEquals(Relation.DISJOINT, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03",
                randomBoolean(), randomBoolean(), null, null, context, false));
    }

    public void testIsFieldWithinQueryDateMillis() throws IOException {
        DateFieldType ft = new DateFieldType("my_date", Resolution.MILLISECONDS);
        isFieldWithinRangeTestCase(ft);
    }

    public void testIsFieldWithinQueryDateNanos() throws IOException {
        DateFieldType ft = new DateFieldType("my_date", Resolution.NANOSECONDS);
        isFieldWithinRangeTestCase(ft);
    }

    public void isFieldWithinRangeTestCase(DateFieldType ft) throws IOException {

        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        //Document doc = new Document();
        ParseContext.Document doc = new ParseContext.Document();
        ft.resolution().index(doc, "my_date", ft.parse("2015-10-12"), true);
        //LongPoint field = new LongPoint("my_date", ft.parse("2015-10-12"));
        //doc.add(field);
        w.addDocument(doc);
        doc = new ParseContext.Document();
        ft.resolution().index(doc, "my_date", ft.parse("2016-04-03"), true);
        //field.setLongValue(ft.parse("2016-04-03"));
        w.addDocument(doc);
        DirectoryReader reader = DirectoryReader.open(w);

        DateMathParser alternateFormat = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.toDateMathParser();
        doTestIsFieldWithinQuery(ft, reader, null, null);
        doTestIsFieldWithinQuery(ft, reader, null, alternateFormat);
        doTestIsFieldWithinQuery(ft, reader, DateTimeZone.UTC, null);
        doTestIsFieldWithinQuery(ft, reader, DateTimeZone.UTC, alternateFormat);

        QueryRewriteContext context = new QueryRewriteContext(xContentRegistry(), writableRegistry(), null, () -> nowInMillis);

        // Fields with no value indexed.
        DateFieldType ft2 = new DateFieldType("my_date2");

        assertEquals(Relation.DISJOINT,
            ft2.isFieldWithinQuery(reader, "2015-10-09", "2016-01-02", false, false, null, null, context, false));

        IOUtils.close(reader, w, dir);
    }

    private void doTestIsFieldWithinQuery(DateFieldType ft, DirectoryReader reader,
            DateTimeZone zone, DateMathParser alternateFormat) throws IOException {
        QueryRewriteContext context = new QueryRewriteContext(xContentRegistry(), writableRegistry(), null, () -> nowInMillis);
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2015-10-09", "2016-01-02",
                randomBoolean(), randomBoolean(), null, null, context, false));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2016-01-02", "2016-06-20",
                randomBoolean(), randomBoolean(), null, null, context, false));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2016-01-02", "2016-02-12",
                randomBoolean(), randomBoolean(), null, null, context, false));
        assertEquals(Relation.DISJOINT, ft.isFieldWithinQuery(reader, "2014-01-02", "2015-02-12",
                randomBoolean(), randomBoolean(), null, null, context, false));
        assertEquals(Relation.DISJOINT, ft.isFieldWithinQuery(reader, "2016-05-11", "2016-08-30",
                randomBoolean(), randomBoolean(), null, null, context, false));
        assertEquals(Relation.WITHIN, ft.isFieldWithinQuery(reader, "2015-09-25", "2016-05-29",
                randomBoolean(), randomBoolean(), null, null, context, false));
        assertEquals(Relation.WITHIN, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03",
                true, true, null, null, context, false));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03",
                false, false, null, null, context, false));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03",
                false, true, null, null, context, false));
        assertEquals(Relation.INTERSECTS, ft.isFieldWithinQuery(reader, "2015-10-12", "2016-04-03",
                true, false, null, null, context, false));
    }

    public void testValueFormat() {
        MappedFieldType ft = new DateFieldType("field");
        long instant = DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse("2015-10-12T14:10:55"))
            .toInstant().toEpochMilli();

        assertEquals("2015-10-12T14:10:55.000Z",
                ft.docValueFormat(null, ZoneOffset.UTC).format(instant));
        assertEquals("2015-10-12T15:10:55.000+01:00",
                ft.docValueFormat(null, ZoneOffset.ofHours(1)).format(instant));
        assertEquals("2015",
                new DateFieldType("field").docValueFormat("YYYY", ZoneOffset.UTC).format(instant));
        assertEquals(instant,
                ft.docValueFormat(null, ZoneOffset.UTC).parseLong("2015-10-12T14:10:55", false, null));
        assertEquals(instant + 999,
                ft.docValueFormat(null, ZoneOffset.UTC).parseLong("2015-10-12T14:10:55", true, null));
        long i = DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse("2015-10-13")).toInstant().toEpochMilli();
        assertEquals(i - 1, ft.docValueFormat(null, ZoneOffset.UTC).parseLong("2015-10-12||/d", true, null));
    }

    public void testValueForSearch() {
        MappedFieldType ft = new DateFieldType("field");
        String date = "2015-10-12T12:09:55.000Z";
        long instant = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(date);
        assertEquals(date, ft.valueForDisplay(instant));
    }

    public void testTermQuery() {
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();
        QueryShardContext context = new QueryShardContext(0,
                new IndexSettings(IndexMetadata.builder("foo").settings(indexSettings).build(), indexSettings),
                BigArrays.NON_RECYCLING_INSTANCE, null, null, null, null, null,
                xContentRegistry(), writableRegistry(), null, null, () -> nowInMillis, null, null, () -> true, null);
        DateFieldType ft = new DateFieldType("field");
        String date = "2015-10-12T14:10:55";
        long instant = DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(date)).toInstant().toEpochMilli();
        Query expected = new IndexOrDocValuesQuery(
                ft.resolution().rangeQuery("field", instant, instant + 999, true),
                SortedNumericDocValuesField.newSlowRangeQuery("field", instant, instant + 999));
        assertEquals(expected, ft.termQuery(date, context));

        MappedFieldType unsearchable = new DateFieldType("field", false, false, true, DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            Resolution.MILLISECONDS, null, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> unsearchable.termQuery(date, context));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testRangeQuery() throws IOException {
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();
        QueryShardContext context = new QueryShardContext(0,
                new IndexSettings(IndexMetadata.builder("foo").settings(indexSettings).build(), indexSettings),
                BigArrays.NON_RECYCLING_INSTANCE, null, null, null, null, null, xContentRegistry(), writableRegistry(),
                null, null, () -> nowInMillis, null, null, () -> true, null);
        DateFieldType ft = new DateFieldType("field");
        String date1 = "2015-10-12T14:10:55";
        String date2 = "2016-04-28T11:33:52";
        long instant1 = DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(date1)).toInstant().toEpochMilli();
        long instant2 =
            DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(date2)).toInstant().toEpochMilli() + 999;
        Query expected = new IndexOrDocValuesQuery(
                ft.resolution().rangeQuery("field", instant1, instant2, true),
                SortedNumericDocValuesField.newSlowRangeQuery("field", instant1, instant2));
        assertEquals(expected,
                ft.rangeQuery(date1, date2, true, true, null, null, null, context).rewrite(new MultiReader()));

        instant1 = nowInMillis;
        instant2 = instant1 + 100;
        expected = new DateRangeIncludingNowQuery(new IndexOrDocValuesQuery(
            ft.resolution().rangeQuery("field", instant1, instant2, true),
            SortedNumericDocValuesField.newSlowRangeQuery("field", instant1, instant2)
        ));
        assertEquals(expected,
            ft.rangeQuery("now", instant2, true, true, null, null, null, context));

        MappedFieldType unsearchable = new DateFieldType("field", false, false, true, DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            Resolution.MILLISECONDS, null, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> unsearchable.rangeQuery(date1, date2, true, true, null, null, null, context));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testRangeQueryWithIndexSort() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put("index.sort.field", "field")
            .build();

        IndexMetadata indexMetadata = new IndexMetadata.Builder("index")
            .settings(settings)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);

        QueryShardContext context = new QueryShardContext(0, indexSettings,
            BigArrays.NON_RECYCLING_INSTANCE, null, null, null, null, null, xContentRegistry(), writableRegistry(),
            null, null, () -> 0L, null, null, () -> true, null);

        DateFieldType ft = new DateFieldType("field");
        String date1 = "2015-10-12T14:10:55";
        String date2 = "2016-04-28T11:33:52";
        long instant1 = DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(date1)).toInstant().toEpochMilli();
        long instant2 = DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(date2)).toInstant().toEpochMilli() + 999;

        Query pointQuery = ft.resolution().rangeQuery("field", instant1, instant2, true);
        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery("field", instant1, instant2);
        Query expected = new IndexSortSortedNumericDocValuesRangeQuery("field",  instant1, instant2,
            new IndexOrDocValuesQuery(pointQuery, dvQuery));
        assertEquals(expected, ft.rangeQuery(date1, date2, true, true, null, null, null, context));
    }

    public void testDateNanoDocValues() throws IOException {
        // Create an index with some docValues
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        Document doc = new Document();
        NumericDocValuesField docValuesField = new NumericDocValuesField("my_date", 1444608000000L);
        doc.add(docValuesField);
        w.addDocument(doc);
        docValuesField.setLongValue(1459641600000L);
        w.addDocument(doc);
        // Create the doc values reader
        SortedNumericIndexFieldData fieldData = new SortedNumericIndexFieldData("my_date",
            IndexNumericFieldData.NumericType.DATE_NANOSECONDS);
        // Read index and check the doc values
        DirectoryReader reader = DirectoryReader.open(w);
        assertTrue(reader.leaves().size() > 0);
        LeafNumericFieldData a = fieldData.load(reader.leaves().get(0).reader().getContext());
        SortedNumericDocValues docValues = a.getLongValues();
        assertEquals(0, docValues.nextDoc());
        assertEquals(1, docValues.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, docValues.nextDoc());
        reader.close();
        w.close();
        dir.close();
    }

    private Instant instant(String str) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(str)).toInstant();
    }

    private static DateFieldType fieldType(Resolution resolution, String format, String nullValue) {
        DateFormatter formatter = DateFormatter.forPattern(format);
        return new DateFieldType("field", true, false, true, formatter, resolution, nullValue, Collections.emptyMap());
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType fieldType = new DateFieldType("field", Resolution.MILLISECONDS);
        String date = "2020-05-15T21:33:02.000Z";
        assertEquals(List.of(date), fetchSourceValue(fieldType, date));
        assertEquals(List.of(date), fetchSourceValue(fieldType, 1589578382000L));

        MappedFieldType fieldWithFormat = fieldType(Resolution.MILLISECONDS, "yyyy/MM/dd||epoch_millis", null);
        String dateInFormat = "1990/12/29";
        assertEquals(List.of(dateInFormat), fetchSourceValue(fieldWithFormat, dateInFormat));
        assertEquals(List.of(dateInFormat), fetchSourceValue(fieldWithFormat, 662428800000L));

        MappedFieldType millis = fieldType(Resolution.MILLISECONDS, "epoch_millis", null);
        String dateInMillis = "662428800000";
        assertEquals(List.of(dateInMillis), fetchSourceValue(millis, dateInMillis));
        assertEquals(List.of(dateInMillis), fetchSourceValue(millis, 662428800000L));

        String nullValueDate = "2020-05-15T21:33:02.000Z";
        MappedFieldType nullFieldType = fieldType(Resolution.MILLISECONDS, "strict_date_time", nullValueDate);
        assertEquals(List.of(nullValueDate), fetchSourceValue(nullFieldType, null));
    }

    public void testParseSourceValueWithFormat() throws IOException {
        MappedFieldType mapper = fieldType(Resolution.NANOSECONDS, "strict_date_time", "1970-12-29T00:00:00.000Z");
        String date = "1990-12-29T00:00:00.000Z";
        assertEquals(List.of("1990/12/29"), fetchSourceValue(mapper, date, "yyyy/MM/dd"));
        assertEquals(List.of("662428800000"), fetchSourceValue(mapper, date, "epoch_millis"));
        assertEquals(List.of("1970/12/29"), fetchSourceValue(mapper, null, "yyyy/MM/dd"));
    }

    public void testParseSourceValueNanos() throws IOException {
        MappedFieldType mapper = fieldType(Resolution.NANOSECONDS, "strict_date_time||epoch_millis", null);
        String date = "2020-05-15T21:33:02.123456789Z";
        assertEquals(List.of("2020-05-15T21:33:02.123456789Z"), fetchSourceValue(mapper, date));
        assertEquals(List.of("2020-05-15T21:33:02.123Z"), fetchSourceValue(mapper, 1589578382123L));

        String nullValueDate = "2020-05-15T21:33:02.123456789Z";
        MappedFieldType nullValueMapper = fieldType(Resolution.NANOSECONDS, "strict_date_time||epoch_millis", nullValueDate);
        assertEquals(List.of(nullValueDate), fetchSourceValue(nullValueMapper, null));
    }
}

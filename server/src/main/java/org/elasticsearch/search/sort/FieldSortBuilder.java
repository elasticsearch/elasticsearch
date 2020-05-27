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

package org.elasticsearch.search.sort;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.SortField;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.search.SearchSortValuesAndFormats;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.index.mapper.DateFieldMapper.Resolution.MILLISECONDS;
import static org.elasticsearch.index.mapper.DateFieldMapper.Resolution.NANOSECONDS;
import static org.elasticsearch.index.search.NestedHelper.parentObject;
import static org.elasticsearch.search.sort.NestedSortBuilder.NESTED_FIELD;

/**
 * A sort builder to sort based on a document field.
 */
public class FieldSortBuilder extends SortBuilder<FieldSortBuilder> {

    public static final String NAME = "field_sort";
    public static final ParseField MISSING = new ParseField("missing");
    public static final ParseField SORT_MODE = new ParseField("mode");
    public static final ParseField UNMAPPED_TYPE = new ParseField("unmapped_type");
    public static final ParseField NUMERIC_TYPE = new ParseField("numeric_type");

    /**
     * special field name to sort by index order
     */
    public static final String DOC_FIELD_NAME = "_doc";
    private static final SortFieldAndFormat SORT_DOC = new SortFieldAndFormat(
            new SortField(null, SortField.Type.DOC), DocValueFormat.RAW);
    private static final SortFieldAndFormat SORT_DOC_REVERSE = new SortFieldAndFormat(
            new SortField(null, SortField.Type.DOC, true), DocValueFormat.RAW);

    private final String fieldName;

    private Object missing;

    private String unmappedType;

    private String numericType;

    private SortMode sortMode;

    private NestedSortBuilder nestedSort;

    /** Copy constructor. */
    public FieldSortBuilder(FieldSortBuilder template) {
        this(template.fieldName);
        this.order(template.order());
        this.missing(template.missing());
        this.unmappedType(template.unmappedType());
        if (template.sortMode != null) {
            this.sortMode(template.sortMode());
        }
        if (template.getNestedSort() != null) {
            this.setNestedSort(template.getNestedSort());
        }
        this.numericType = template.numericType;
    }

    /**
     * Constructs a new sort based on a document field.
     *
     * @param fieldName
     *            The field name.
     */
    public FieldSortBuilder(String fieldName) {
        if (fieldName == null) {
            throw new IllegalArgumentException("fieldName must not be null");
        }
        this.fieldName = fieldName;
    }

    /**
     * Read from a stream.
     */
    public FieldSortBuilder(StreamInput in) throws IOException {
        fieldName = in.readString();
        if (in.getVersion().before(Version.V_8_0_0)) {
            if (in.readOptionalNamedWriteable(QueryBuilder.class) != null || in.readOptionalString() != null) {
                throw new IOException("the [sort] options [nested_path] and [nested_filter] are removed in 8.x, " +
                    "please use [nested] instead");
            }
        }
        missing = in.readGenericValue();
        order = in.readOptionalWriteable(SortOrder::readFromStream);
        sortMode = in.readOptionalWriteable(SortMode::readFromStream);
        unmappedType = in.readOptionalString();
        nestedSort = in.readOptionalWriteable(NestedSortBuilder::new);
        if (in.getVersion().onOrAfter(Version.V_7_2_0)) {
            numericType = in.readOptionalString();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        if (out.getVersion().before(Version.V_8_0_0)) {
            out.writeOptionalNamedWriteable(null);
            out.writeOptionalString(null);
        }
        out.writeGenericValue(missing);
        out.writeOptionalWriteable(order);
        out.writeOptionalWriteable(sortMode);
        out.writeOptionalString(unmappedType);
        out.writeOptionalWriteable(nestedSort);
        if (out.getVersion().onOrAfter(Version.V_7_2_0)) {
            out.writeOptionalString(numericType);
        }
    }

    /** Returns the document field this sort should be based on. */
    public String getFieldName() {
        return this.fieldName;
    }

    /**
     * Sets the value when a field is missing in a doc. Can also be set to {@code _last} or
     * {@code _first} to sort missing last or first respectively.
     */
    public FieldSortBuilder missing(Object missing) {
        this.missing = missing;
        return this;
    }

    /** Returns the value used when a field is missing in a doc. */
    public Object missing() {
        return missing;
    }

    /**
     * Set the type to use in case the current field is not mapped in an index.
     * Specifying a type tells Elasticsearch what type the sort values should
     * have, which is important for cross-index search, if there are sort fields
     * that exist on some indices only. If the unmapped type is {@code null}
     * then query execution will fail if one or more indices don't have a
     * mapping for the current field.
     */
    public FieldSortBuilder unmappedType(String type) {
        this.unmappedType = type;
        return this;
    }

    /**
     * Returns the type to use in case the current field is not mapped in an
     * index.
     */
    public String unmappedType() {
        return this.unmappedType;
    }

    /**
     * Defines what values to pick in the case a document contains multiple
     * values for the targeted sort field. Possible values: min, max, sum and
     * avg
     *
     * <p>
     * The last two values are only applicable for number based fields.
     */
    public FieldSortBuilder sortMode(SortMode sortMode) {
        Objects.requireNonNull(sortMode, "sort mode cannot be null");
        this.sortMode = sortMode;
        return this;
    }

    /**
     * Returns what values to pick in the case a document contains multiple
     * values for the targeted sort field.
     */
    public SortMode sortMode() {
        return this.sortMode;
    }

    /**
     * Returns the {@link NestedSortBuilder}
     */
    public NestedSortBuilder getNestedSort() {
        return this.nestedSort;
    }

    /**
     * Sets the {@link NestedSortBuilder} to be used for fields that are inside a nested
     * object. The {@link NestedSortBuilder} takes a `path` argument and an optional
     * nested filter that the nested objects should match with in
     * order to be taken into account for sorting.
     */
    public FieldSortBuilder setNestedSort(final NestedSortBuilder nestedSort) {
        this.nestedSort = nestedSort;
        return this;
    }

    /**
     * Returns the numeric type that values should translated to or null
     * if the original numeric type should be preserved.
     */
    public String getNumericType() {
        return numericType;
    }

    /**
     * Forces the numeric type to use for the field. The query will fail if this option
     * is set on a field that is not mapped as a numeric in some indices.
     * Specifying a numeric type tells Elasticsearch what type the sort values should
     * have, which is important for cross-index search, if a field does not have
     * the same type on all indices.
     * Allowed values are <code>long</code> and <code>double</code>.
     */
    public FieldSortBuilder setNumericType(String numericType) {
        String lowerCase = numericType.toLowerCase(Locale.ENGLISH);
        switch (lowerCase) {
            case "long":
            case "double":
            case "date":
            case "date_nanos":
                break;

            default:
                throw new IllegalArgumentException("invalid value for [numeric_type], " +
                    "must be [long, double, date, date_nanos], got " + lowerCase);
        }
        this.numericType = lowerCase;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(fieldName);
        builder.field(ORDER_FIELD.getPreferredName(), order);
        if (missing != null) {
            builder.field(MISSING.getPreferredName(), missing);
        }
        if (unmappedType != null) {
            builder.field(UNMAPPED_TYPE.getPreferredName(), unmappedType);
        }
        if (sortMode != null) {
            builder.field(SORT_MODE.getPreferredName(), sortMode);
        }
        if (nestedSort != null) {
            builder.field(NESTED_FIELD.getPreferredName(), nestedSort);
        }
        if (numericType != null) {
            builder.field(NUMERIC_TYPE.getPreferredName(), numericType);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    private static NumericType resolveNumericType(String value) {
        switch (value) {
            case "long":
                return NumericType.LONG;
            case "double":
                return NumericType.DOUBLE;
            case "date":
                return NumericType.DATE;
            case "date_nanos":
                return NumericType.DATE_NANOSECONDS;

            default:
                throw new IllegalArgumentException("invalid value for [numeric_type], " +
                    "must be [long, double, date, date_nanos], got " + value);
        }
    }

    @Override
    public SortFieldAndFormat build(QueryShardContext context) throws IOException {
        if (DOC_FIELD_NAME.equals(fieldName)) {
            return order == SortOrder.DESC ? SORT_DOC_REVERSE : SORT_DOC;
        }

        MappedFieldType fieldType = context.fieldMapper(fieldName);
        Nested nested = nested(context, fieldType);
        if (fieldType == null) {
            fieldType = resolveUnmappedType(context);
        }

        boolean reverse = order == SortOrder.DESC;
        IndexFieldData<?> fieldData = context.getForField(fieldType);
        if (fieldData instanceof IndexNumericFieldData == false
                && (sortMode == SortMode.SUM || sortMode == SortMode.AVG || sortMode == SortMode.MEDIAN)) {
            throw new QueryShardException(context, "we only support AVG, MEDIAN and SUM on number based fields");
        }
        final SortField field;
        if (numericType != null) {
            if (fieldData instanceof IndexNumericFieldData == false) {
                throw new QueryShardException(context,
                    "[numeric_type] option cannot be set on a non-numeric field, got " + fieldType.typeName());
            }
            SortedNumericIndexFieldData numericFieldData = (SortedNumericIndexFieldData) fieldData;
            NumericType resolvedType = resolveNumericType(numericType);
            field = numericFieldData.sortField(resolvedType, missing, localSortMode(), nested, reverse);
        } else {
            field = fieldData.sortField(missing, localSortMode(), nested, reverse);
        }
        return new SortFieldAndFormat(field, fieldType.docValueFormat(null, null));
    }

    public boolean canRewriteToMatchNone() {
        return nestedSort == null && (missing == null || "_last".equals(missing));
    }

    /**
     * Returns whether some values of the given {@link QueryShardContext#getIndexReader()} are within the
     * primary sort value provided in the <code>bottomSortValues</code>.
     */
    public boolean isBottomSortShardDisjoint(QueryShardContext context, SearchSortValuesAndFormats bottomSortValues) throws IOException {
        if (bottomSortValues == null || bottomSortValues.getRawSortValues().length == 0) {
            return false;
        }

        if (canRewriteToMatchNone() == false) {
            return false;
        }
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            // unmapped
            return false;
        }
        if (fieldType.indexOptions() == IndexOptions.NONE) {
            return false;
        }
        DocValueFormat docValueFormat = bottomSortValues.getSortValueFormats()[0];
        final DateMathParser dateMathParser;
        if (docValueFormat instanceof DocValueFormat.DateTime) {
            if (fieldType instanceof DateFieldType && ((DateFieldType) fieldType).resolution() == NANOSECONDS) {
                // we parse the formatted value with the resolution of the local field because
                // the provided format can use a different one (date vs date_nanos).
                docValueFormat = DocValueFormat.withNanosecondResolution(docValueFormat);
            }
            dateMathParser = ((DocValueFormat.DateTime) docValueFormat).getDateMathParser();
        } else {
            dateMathParser = null;
        }
        Object bottomSortValue =  bottomSortValues.getFormattedSortValues()[0];
        Object minValue = order() == SortOrder.DESC ? bottomSortValue : null;
        Object maxValue = order() == SortOrder.DESC ? null : bottomSortValue;
        try {
            MappedFieldType.Relation relation = fieldType.isFieldWithinQuery(context.getIndexReader(), minValue, maxValue,
                true, true, null, dateMathParser, context);
            return relation == MappedFieldType.Relation.DISJOINT;
        } catch (ElasticsearchParseException exc) {
            // can happen if the sort field is mapped differently in another search index
            return false;
        }
    }

    @Override
    public BucketedSort buildBucketedSort(QueryShardContext context, int bucketSize, BucketedSort.ExtraData extra) throws IOException {
        if (DOC_FIELD_NAME.equals(fieldName)) {
            throw new IllegalArgumentException("sorting by _doc is not supported");
        }

        MappedFieldType fieldType = context.fieldMapper(fieldName);
        Nested nested = nested(context, fieldType);
        if (fieldType == null) {
            fieldType = resolveUnmappedType(context);
        }

        IndexFieldData<?> fieldData = context.getForField(fieldType);
        if (fieldData instanceof IndexNumericFieldData == false
                && (sortMode == SortMode.SUM || sortMode == SortMode.AVG || sortMode == SortMode.MEDIAN)) {
            throw new QueryShardException(context, "we only support AVG, MEDIAN and SUM on number based fields");
        }
        if (numericType != null) {
            SortedNumericIndexFieldData numericFieldData = (SortedNumericIndexFieldData) fieldData;
            NumericType resolvedType = resolveNumericType(numericType);
            return numericFieldData.newBucketedSort(resolvedType, context.bigArrays(), missing, localSortMode(), nested, order,
                    fieldType.docValueFormat(null, null), bucketSize, extra);
        }
        try {
            return fieldData.newBucketedSort(context.bigArrays(), missing, localSortMode(), nested, order,
                    fieldType.docValueFormat(null, null), bucketSize, extra);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("error building sort for field [" + fieldName + "] of type ["
                    + fieldType.typeName() + "] in index [" + context.index().getName() + "]: " + e.getMessage(), e);
        }
    }

    private MappedFieldType resolveUnmappedType(QueryShardContext context) {
        if (unmappedType == null) {
            throw new QueryShardException(context, "No mapping found for [" + fieldName + "] in order to sort on");
        }
        return context.getMapperService().unmappedFieldType(unmappedType);
    }

    private MultiValueMode localSortMode() {
        if (sortMode != null) {
            return MultiValueMode.fromString(sortMode.toString());
        }

        return order == SortOrder.DESC ? MultiValueMode.MAX : MultiValueMode.MIN;
    }

    private Nested nested(QueryShardContext context, MappedFieldType fieldType) throws IOException {
        if (fieldType == null) {
            return null;
        }
        if (nestedSort == null) {
            validateMissingNestedPath(context, fieldName);
            return null;
        }
        validateMaxChildrenExistOnlyInTopLevelNestedSort(context, nestedSort);
        return resolveNested(context, nestedSort);
    }

    /**
     * Return true if the primary sort in the provided <code>source</code>
     * is an instance of {@link FieldSortBuilder}.
     */
    public static boolean hasPrimaryFieldSort(SearchSourceBuilder source) {
        return getPrimaryFieldSortOrNull(source) != null;
    }

    /**
     * Return the {@link FieldSortBuilder} if the primary sort in the provided <code>source</code>
     * is an instance of this class, null otherwise.
     */
    public static FieldSortBuilder getPrimaryFieldSortOrNull(SearchSourceBuilder source) {
        if (source == null || source.sorts() == null || source.sorts().isEmpty()) {
            return null;
        }
        return source.sorts().get(0) instanceof FieldSortBuilder ? (FieldSortBuilder) source.sorts().get(0) : null;
    }

    /**
     * Return the {@link MinAndMax} indexed value from the provided {@link FieldSortBuilder} or <code>null</code> if unknown.
     * The value can be extracted on non-nested indexed mapped fields of type keyword, numeric or date, other fields
     * and configurations return <code>null</code>.
     */
    public static MinAndMax<?> getMinMaxOrNull(QueryShardContext context, FieldSortBuilder sortBuilder) throws IOException {
        SortAndFormats sort = SortBuilder.buildSort(Collections.singletonList(sortBuilder), context).get();
        SortField sortField = sort.sort.getSort()[0];
        if (sortField.getField() == null) {
            return null;
        }
        IndexReader reader = context.getIndexReader();
        MappedFieldType fieldType = context.fieldMapper(sortField.getField());
        if (reader == null || (fieldType == null || fieldType.indexOptions() == IndexOptions.NONE)) {
            return null;
        }
        switch (IndexSortConfig.getSortFieldType(sortField)) {
            case LONG:
            case INT:
            case DOUBLE:
            case FLOAT:
                return extractNumericMinAndMax(reader, sortField, fieldType, sortBuilder);
            case STRING:
            case STRING_VAL:
                if (fieldType instanceof KeywordFieldMapper.KeywordFieldType) {
                    Terms terms = MultiTerms.getTerms(reader, fieldType.name());
                    if (terms == null) {
                        return null;
                    }
                    return terms.getMin() != null ? new MinAndMax<>(terms.getMin(), terms.getMax()) : null;
                }
                break;
        }
        return null;
    }

    private static MinAndMax<?> extractNumericMinAndMax(IndexReader reader,
                                                        SortField sortField,
                                                        MappedFieldType fieldType,
                                                        FieldSortBuilder sortBuilder) throws IOException {
        String fieldName = fieldType.name();
        if (PointValues.size(reader, fieldName) == 0) {
            return null;
        }
        if (fieldType instanceof NumberFieldType) {
            NumberFieldType numberFieldType = (NumberFieldType) fieldType;
            Number minPoint = numberFieldType.parsePoint(PointValues.getMinPackedValue(reader, fieldName));
            Number maxPoint = numberFieldType.parsePoint(PointValues.getMaxPackedValue(reader, fieldName));
            switch (IndexSortConfig.getSortFieldType(sortField)) {
                case LONG:
                    return new MinAndMax<>(minPoint.longValue(), maxPoint.longValue());
                case INT:
                    return new MinAndMax<>(minPoint.intValue(), maxPoint.intValue());
                case DOUBLE:
                    return new MinAndMax<>(minPoint.doubleValue(), maxPoint.doubleValue());
                case FLOAT:
                    return new MinAndMax<>(minPoint.floatValue(), maxPoint.floatValue());
                default:
                    return null;
            }
        } else if (fieldType instanceof DateFieldType) {
            DateFieldType dateFieldType = (DateFieldType) fieldType;
            Function<byte[], Long> dateConverter = createDateConverter(sortBuilder, dateFieldType);
            Long min = dateConverter.apply(PointValues.getMinPackedValue(reader, fieldName));
            Long max = dateConverter.apply(PointValues.getMaxPackedValue(reader, fieldName));
            return new MinAndMax<>(min, max);
        }
        return null;
    }

    private static Function<byte[], Long> createDateConverter(FieldSortBuilder sortBuilder, DateFieldType dateFieldType) {
        String numericTypeStr = sortBuilder.getNumericType();
        if (numericTypeStr != null) {
            NumericType numericType = resolveNumericType(numericTypeStr);
            if (dateFieldType.resolution() == MILLISECONDS && numericType == NumericType.DATE_NANOSECONDS) {
                return v -> DateUtils.toNanoSeconds(LongPoint.decodeDimension(v, 0));
            } else if (dateFieldType.resolution() == NANOSECONDS && numericType == NumericType.DATE) {
                return v -> DateUtils.toMilliSeconds(LongPoint.decodeDimension(v, 0));
            }
        }
        return v -> LongPoint.decodeDimension(v, 0);
    }

    /**
     * Throws an exception if max children is not located at top level nested sort.
     */
    static void validateMaxChildrenExistOnlyInTopLevelNestedSort(QueryShardContext context, NestedSortBuilder nestedSort) {
        for (NestedSortBuilder child = nestedSort.getNestedSort(); child != null; child = child.getNestedSort()) {
            if (child.getMaxChildren() != Integer.MAX_VALUE) {
                throw new QueryShardException(context,
                    "max_children is only supported on top level of nested sort");
            }
        }
    }

    /**
     * Throws an exception if the provided <code>field</code> requires a nested context.
     */
    static void validateMissingNestedPath(QueryShardContext context, String field) {
        ObjectMapper contextMapper = context.nestedScope().getObjectMapper();
        if (contextMapper != null && contextMapper.nested().isNested() == false) {
            // already in nested context
            return;
        }
        for (String parent = parentObject(field); parent != null; parent = parentObject(parent)) {
            ObjectMapper parentMapper = context.getObjectMapper(parent);
            if (parentMapper != null && parentMapper.nested().isNested()) {
                if (contextMapper != null && contextMapper.fullPath().equals(parentMapper.fullPath())) {
                    // we are in a nested context that matches the path of the provided field so the nested path
                    // is not required
                    return ;
                }
                if (parentMapper.nested().isIncludeInRoot() == false) {
                    throw new QueryShardException(context,
                        "it is mandatory to set the [nested] context on the nested sort field: [" + field + "].");
                }
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        FieldSortBuilder builder = (FieldSortBuilder) other;
        return (Objects.equals(this.fieldName, builder.fieldName) && Objects.equals(this.missing, builder.missing)
                && Objects.equals(this.order, builder.order) && Objects.equals(this.sortMode, builder.sortMode)
                && Objects.equals(this.unmappedType, builder.unmappedType) && Objects.equals(this.nestedSort, builder.nestedSort))
                && Objects.equals(this.numericType, builder.numericType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.fieldName, this.nestedSort, this.missing, this.order, this.sortMode,
            this.unmappedType, this.numericType);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Creates a new {@link FieldSortBuilder} from the query held by the {@link XContentParser} in
     * {@link org.elasticsearch.common.xcontent.XContent} format.
     *
     * @param parser the input parser. The state on the parser contained in this context will be changed as a side effect of this
     *        method call
     * @param fieldName in some sort syntax variations the field name precedes the xContent object that specifies further parameters, e.g.
     *        in '{ "foo": { "order" : "asc"} }'. When parsing the inner object, the field name can be passed in via this argument
     */
    public static FieldSortBuilder fromXContent(XContentParser parser, String fieldName) throws IOException {
        return PARSER.parse(parser, new FieldSortBuilder(fieldName), null);
    }

    private static final ObjectParser<FieldSortBuilder, Void> PARSER = new ObjectParser<>(NAME);

    static {
        PARSER.declareField(FieldSortBuilder::missing, XContentParser::objectText,  MISSING, ValueType.VALUE);
        PARSER.declareString(FieldSortBuilder::unmappedType , UNMAPPED_TYPE);
        PARSER.declareString((b, v) -> b.order(SortOrder.fromString(v)) , ORDER_FIELD);
        PARSER.declareString((b, v) -> b.sortMode(SortMode.fromString(v)), SORT_MODE);
        PARSER.declareObject(FieldSortBuilder::setNestedSort, (p, c) -> NestedSortBuilder.fromXContent(p), NESTED_FIELD);
        PARSER.declareString(FieldSortBuilder::setNumericType, NUMERIC_TYPE);
    }

    @Override
    public FieldSortBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        if (nestedSort == null) {
            return this;
        }
        NestedSortBuilder rewrite = nestedSort.rewrite(ctx);
        if (nestedSort == rewrite) {
            return this;
        }
        return new FieldSortBuilder(this).setNestedSort(rewrite);
    }
}

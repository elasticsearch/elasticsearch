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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Objects;

/**
 * A Query that matches documents within an range of terms.
 */
public class RangeQueryBuilder extends MultiTermQueryBuilder implements Streamable, BoostableQueryBuilder<RangeQueryBuilder> {

    private String fieldname;

    private Object from;

    private Object to;
    private String timeZone;

    private boolean includeLower = true;

    private boolean includeUpper = true;

    private float boost = 1.0f;

    private String queryName;

    private String format;

    /**
     * A Query that matches documents within an range of terms.
     *
     * @param fieldname The field name
     */
    public RangeQueryBuilder(String fieldname) {
        this.fieldname = fieldname;
    }

    public RangeQueryBuilder() {
        // for serialization
    }

    /**
     * Get the field name for this query.
     */
    public String fieldname() {
        return this.fieldname;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder from(Object from, boolean includeLower) {
        if (from instanceof String) {
            this.from = BytesRefs.toBytesRef(from);
        } else {
            this.from = from;
        }
        this.includeLower = includeLower;
        return this;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder from(Object from) {
        return from(from, this.includeLower);
    }

    /**
     * Gets the lower range value for this query.
     */
    public Object from() {
        return this.from;
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gt(Object from) {
        return from(from, false);
    }

    /**
     * The from part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder gte(Object from) {
        return from(from, true);
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder to(Object to, boolean includeUpper) {
        if (to instanceof String) {
            this.to = BytesRefs.toBytesRef(to);
        } else {
            this.to = to;
        }
        this.includeUpper = includeUpper;
        return this;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder to(Object to) {
        return to(to, this.includeUpper);
    }

    /**
     * Gets the upper range value for this query.
     */
    public Object to() {
        return this.to;
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lt(Object to) {
        return to(to, false);
    }

    /**
     * The to part of the range query. Null indicates unbounded.
     */
    public RangeQueryBuilder lte(Object to) {
        return to(to, true);
    }

    /**
     * Should the lower bound be included or not. Defaults to <tt>true</tt>.
     */
    public RangeQueryBuilder includeLower(boolean includeLower) {
        this.includeLower = includeLower;
        return this;
    }

    /**
     * Gets the includeLower flag for this query.
     */
    public boolean includeLower() {
        return this.includeLower;
    }

    /**
     * Should the upper bound be included or not. Defaults to <tt>true</tt>.
     */
    public RangeQueryBuilder includeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
        return this;
    }

    /**
     * Gets the includeUpper flag for this query.
     */
    public boolean includeUpper() {
        return this.includeLower;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public RangeQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Gets the boost factor for the query.
     */
    public float boost() {
        return this.boost;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public RangeQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    /**
     * Gets the query name for the query.
     */
    public String queryName() {
        return this.queryName;
    }

    /**
     * In case of date field, we can adjust the from/to fields using a timezone
     */
    public RangeQueryBuilder timeZone(String timezone) {
        this.timeZone = timezone;
        return this;
    }

    /**
     * In case of format field, we can parse the from/to fields using this time format
     */
    public RangeQueryBuilder format(String format) {
        this.format = format;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(RangeQueryParser.NAME);
        builder.startObject(fieldname);
        if (this.from instanceof BytesRef) {
            builder.field("from", ((BytesRef) this.from).utf8ToString());
        } else {
            builder.field("from", from);
        }
        if (this.to instanceof BytesRef) {
            builder.field("to", ((BytesRef) this.to).utf8ToString());
        } else {
            builder.field("to", to);
        }
        if (timeZone != null) {
            builder.field("time_zone", timeZone);
        }
        if (format != null) {
            builder.field("format", format);
        }
        builder.field("include_lower", includeLower);
        builder.field("include_upper", includeUpper);
        if (boost != 1.0f) {
            builder.field("boost", boost);
        }
        builder.endObject();
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }

    @Override
    protected String parserName() {
        return RangeQueryParser.NAME;
    }

    @Override
    public Query toQuery(QueryParseContext parseContext) throws QueryParsingException, IOException {
        Query query = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(this.fieldname);
        if (smartNameFieldMappers != null) {
            if (smartNameFieldMappers.hasMapper()) {
                FieldMapper mapper = smartNameFieldMappers.mapper();
                if (mapper instanceof DateFieldMapper) {
                    if ((from instanceof Number || to instanceof Number) && timeZone != null) {
                        throw new QueryParsingException(parseContext,
                                "[range] time_zone when using ms since epoch format as it's UTC based can not be applied to [" + this.fieldname
                                        + "]");
                    }
                    DateMathParser forcedDateParser = null;
                    if (this.format  != null) {
                        forcedDateParser = new DateMathParser(Joda.forPattern(this.format), DateFieldMapper.Defaults.TIME_UNIT);
                    }
                    DateTimeZone dateTimeZone = null;
                    if (this.timeZone != null) {
                        dateTimeZone = DateTimeZone.forID(this.timeZone);
                    }
                    query = ((DateFieldMapper) mapper).rangeQuery(from, to, includeLower, includeUpper, dateTimeZone, forcedDateParser, parseContext);
                } else  {
                    if (timeZone != null) {
                        throw new QueryParsingException(parseContext, "[range] time_zone can not be applied to non date field ["
                                + this.fieldname + "]");
                    }
                    //LUCENE 4 UPGRADE Mapper#rangeQuery should use bytesref as well?
                    query = mapper.rangeQuery(from, to, includeLower, includeUpper, parseContext);
                }

            }
        }
        if (query == null) {
            query = new TermRangeQuery(this.fieldname, BytesRefs.toBytesRef(from), BytesRefs.toBytesRef(to), includeLower, includeUpper);
        }
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException validationException = null;
        if (this.fieldname == null || this.fieldname.isEmpty()) {
            validationException = QueryValidationException.addValidationError("field name cannot be null or empty.", validationException);
        }
        if (this.timeZone != null) {
            try {
                DateTimeZone.forID(this.timeZone);
            } catch (Exception e) {
                validationException = QueryValidationException.addValidationError("error parsing timezone." + e.getMessage(),
                        validationException);
            }
        }
        if (this.format != null) {
            try {
                Joda.forPattern(this.format);
            } catch (Exception e) {
                validationException = QueryValidationException.addValidationError("error parsing format." + e.getMessage(),
                        validationException);
            }
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.fieldname = in.readString();
        this.from = in.readGenericValue();
        this.to = in.readGenericValue();
        this.includeLower = in.readBoolean();
        this.includeLower = in.readBoolean();
        this.timeZone = in.readOptionalString();
        this.format = in.readOptionalString();
        this.boost = in.readFloat();
        this.queryName = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldname);
        out.writeGenericValue(this.from);
        out.writeGenericValue(this.to);
        out.writeBoolean(this.includeLower);
        out.writeBoolean(this.includeUpper);
        out.writeOptionalString(this.timeZone);
        out.writeOptionalString(this.format);
        out.writeFloat(this.boost);
        out.writeOptionalString(this.queryName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldname, from, to, timeZone, includeLower, includeUpper,
                boost, queryName, format);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RangeQueryBuilder other = (RangeQueryBuilder) obj;
        return Objects.equals(fieldname, other.fieldname) &&
               Objects.equals(from, other.from) &&
               Objects.equals(to, other.to) &&
               Objects.equals(timeZone, other.timeZone) &&
               Objects.equals(includeLower, other.includeLower) &&
               Objects.equals(includeUpper, other.includeUpper) &&
               Objects.equals(boost, other.boost) &&
               Objects.equals(queryName, other.queryName) &&
               Objects.equals(format, other.format);
    }
}

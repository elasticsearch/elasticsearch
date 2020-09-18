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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ConstantFieldType;

import java.io.IOException;
import java.util.Objects;

/**
 * A Query that matches documents containing a term.
 */
public class TermQueryBuilder extends BaseTermQueryBuilder<TermQueryBuilder> {
    public static final String NAME = "term";
    public static final boolean DEFAULT_CASE_INSENSITIVITY = false;
    private static final ParseField CASE_INSENSITIVE_FIELD = new ParseField("case_insensitive");
    
    
    private boolean caseInsensitive = DEFAULT_CASE_INSENSITIVITY;
    

    private static final ParseField TERM_FIELD = new ParseField("term");
    private static final ParseField VALUE_FIELD = new ParseField("value");

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, String) */
    public TermQueryBuilder(String fieldName, String value) {
        super(fieldName, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, int) */
    public TermQueryBuilder(String fieldName, int value) {
        super(fieldName, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, long) */
    public TermQueryBuilder(String fieldName, long value) {
        super(fieldName, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, float) */
    public TermQueryBuilder(String fieldName, float value) {
        super(fieldName, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, double) */
    public TermQueryBuilder(String fieldName, double value) {
        super(fieldName, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, boolean) */
    public TermQueryBuilder(String fieldName, boolean value) {
        super(fieldName, (Object) value);
    }

    /** @see BaseTermQueryBuilder#BaseTermQueryBuilder(String, Object) */
    public TermQueryBuilder(String fieldName, Object value) {
        super(fieldName, value);
    }
    
    public TermQueryBuilder caseInsensitive(boolean caseInsensitive) {
        if (caseInsensitive == false) {
            throw new IllegalArgumentException("The case insensitive setting cannot be set to false.");
        }
        this.caseInsensitive = caseInsensitive;
        return this;
    }    

    public boolean caseInsensitive() {
        return this.caseInsensitive;
    }
    

    /**
     * Read from a stream.
     */
    public TermQueryBuilder(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            caseInsensitive = in.readBoolean();
        }        
    }
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeBoolean(caseInsensitive);
        }    
    }    

    public static TermQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String queryName = null;
        String fieldName = null;
        Object value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        boolean caseInsensitive = DEFAULT_CASE_INSENSITIVITY;        
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if (TERM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            value = maybeConvertToBytesRef(parser.objectBytes());
                        } else if (VALUE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            value = maybeConvertToBytesRef(parser.objectBytes());
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (CASE_INSENSITIVE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            caseInsensitive = parser.booleanValue();
                            if (caseInsensitive == false) {
                                throw new ParsingException(parser.getTokenLocation(),
                                    "[term] query does not support [" + currentFieldName + "] = false");
                            }
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "[term] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = currentFieldName;
                value = maybeConvertToBytesRef(parser.objectBytes());
            } else if (token == XContentParser.Token.START_ARRAY) {
                throw new ParsingException(parser.getTokenLocation(), "[term] query does not support array of values");
            }
        }

        TermQueryBuilder termQuery = new TermQueryBuilder(fieldName, value);
        termQuery.boost(boost);
        if (queryName != null) {
            termQuery.queryName(queryName);
        }
        if (caseInsensitive) {
            termQuery.caseInsensitive(caseInsensitive);
        }
        return termQuery;
    }
    
    @Override
    protected void addExtraXContent(XContentBuilder builder, Params params) throws IOException {
        if (caseInsensitive != DEFAULT_CASE_INSENSITIVITY) {
            builder.field(CASE_INSENSITIVE_FIELD.getPreferredName(), caseInsensitive);
        }
    }    
    
    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryShardContext context = queryRewriteContext.convertToShardContext();
        if (context != null) {
            MappedFieldType fieldType = context.fieldMapper(this.fieldName);
            if (fieldType == null) {
                return new MatchNoneQueryBuilder();
            } else if (fieldType instanceof ConstantFieldType) {
                // This logic is correct for all field types, but by only applying it to constant
                // fields we also have the guarantee that it doesn't perform I/O, which is important
                // since rewrites might happen on a network thread.
                Query query = null;
                if (caseInsensitive) {
                    query = fieldType.termQueryCaseInsensitive(value, context);
                } else {
                    query = fieldType.termQuery(value, context);
                }
                    
                if (query instanceof MatchAllDocsQuery) {
                    return new MatchAllQueryBuilder();
                } else if (query instanceof MatchNoDocsQuery) {
                    return new MatchNoneQueryBuilder();
                } else {
                    assert false : "Constant fields must produce match-all or match-none queries, got " + query ;
                }
            }
        }
        return super.doRewrite(queryRewriteContext);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType mapper = context.fieldMapper(this.fieldName);
        if (mapper == null) {
            throw new IllegalStateException("Rewrite first");
        }
        if (caseInsensitive) {
            return mapper.termQueryCaseInsensitive(value, context);
        }
        return mapper.termQuery(value, context);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
    

    @Override
    protected final int doHashCode() {
        return Objects.hash(super.doHashCode(), caseInsensitive);
    }

    @Override
    protected final boolean doEquals(TermQueryBuilder other) {
        return super.doEquals(other) &&
               Objects.equals(caseInsensitive, other.caseInsensitive);
    }    
    
}

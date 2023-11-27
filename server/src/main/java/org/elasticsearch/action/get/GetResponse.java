/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * The response of a get action.
 *
 * @see GetRequest
 * @see org.elasticsearch.client.internal.Client#get(GetRequest)
 */
public class GetResponse extends ActionResponse implements Iterable<DocumentField>, ToXContentObject {

    GetResult getResult;

    GetResponse(StreamInput in) throws IOException {
        super(in);
        getResult = new GetResult(in);
    }

    public GetResponse(GetResult getResult) {
        this.getResult = getResult;
    }

    /**
     * Does the document exists.
     */
    public boolean isExists() {
        return getResult.isExists();
    }

    /**
     * The index the document was fetched from.
     */
    public String getIndex() {
        return getResult.getIndex();
    }

    /**
     * The id of the document.
     */
    public String getId() {
        return getResult.getId();
    }

    /**
     * The version of the doc.
     */
    public long getVersion() {
        return getResult.getVersion();
    }

    /**
     * The sequence number assigned to the last operation that has changed this document, if found.
     */
    public long getSeqNo() {
        return getResult.getSeqNo();
    }

    /**
     * The primary term of the last primary that has changed this document, if found.
     */
    public long getPrimaryTerm() {
        return getResult.getPrimaryTerm();
    }

    /**
     * Returns the internal source bytes, as they are returned without munging (for example,
     * might still be compressed).
     */
    public BytesReference getSourceInternal() {
        return getResult.internalSourceRef();
    }

    /**
     * Returns bytes reference, also un compress the source if needed.
     */
    public BytesReference getSourceAsBytesRef() {
        return getResult.sourceRef();
    }

    /**
     * Is the source empty (not available) or not.
     */
    public boolean isSourceEmpty() {
        return getResult.isSourceEmpty();
    }

    /**
     * The source of the document (as a string).
     */
    public String getSourceAsString() {
        return getResult.sourceAsString();
    }

    /**
     * The source of the document (As a map).
     */
    public Map<String, Object> getSourceAsMap() throws ElasticsearchParseException {
        return getResult.sourceAsMap();
    }

    public Map<String, Object> getSource() {
        return getResult.sourceAsMap();
    }

    public Map<String, DocumentField> getFields() {
        return getResult.getFields();
    }

    public DocumentField getField(String name) {
        return getResult.field(name);
    }

    /**
     * @deprecated Use {@link GetResponse#getSource()} instead
     */
    @Deprecated
    public Iterator<DocumentField> iterator() {
        return getResult.iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return getResult.toXContent(builder, params);
    }

    /**
     * This method can be used to parse a {@link GetResponse} object when it has been printed out
     * as a xcontent using the {@link #toXContent(XContentBuilder, Params)} method.
     * <p>
     * For forward compatibility reason this method might not fail if it tries to parse a field it
     * doesn't know. But before returning the result it will check that enough information were
     * parsed to return a valid {@link GetResponse} instance and throws a {@link ParsingException}
     * otherwise. This is the case when we get a 404 back, which can be parsed as a normal
     * {@link GetResponse} with found set to false, or as an elasticsearch exception. The caller
     * of this method needs a way to figure out whether we got back a valid get response, which
     * can be done by catching ParsingException.
     *
     * @param parser {@link XContentParser} to parse the response from
     * @return a {@link GetResponse}
     * @throws IOException is an I/O exception occurs during the parsing
     */
    public static GetResponse fromXContent(XContentParser parser) throws IOException {
        GetResult getResult = GetResult.fromXContent(parser);

        // At this stage we ensure that we parsed enough information to return
        // a valid GetResponse instance. If it's not the case, we throw an
        // exception so that callers know it and can handle it correctly.
        if (getResult.getIndex() == null && getResult.getId() == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(Locale.ROOT, "Missing required fields [%s,%s]", GetResult._INDEX, GetResult._ID)
            );
        }
        return new GetResponse(getResult);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        getResult.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetResponse getResponse = (GetResponse) o;
        return Objects.equals(getResult, getResponse.getResult);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getResult);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}

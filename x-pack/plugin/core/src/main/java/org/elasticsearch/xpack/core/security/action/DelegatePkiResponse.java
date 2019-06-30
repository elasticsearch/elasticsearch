/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class DelegatePkiResponse extends ActionResponse implements ToXContentObject {

    private String tokenString;
    private TimeValue expiresIn;

    DelegatePkiResponse() { }

    public DelegatePkiResponse(String tokenString, TimeValue expiresIn) {
        this.tokenString = Objects.requireNonNull(tokenString);
        this.expiresIn = Objects.requireNonNull(expiresIn);
    }

    public String getTokenString() {
        return tokenString;
    }

    public TimeValue getExpiresIn() {
        return expiresIn;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(tokenString);
        out.writeTimeValue(expiresIn);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tokenString = in.readString();
        expiresIn = in.readTimeValue();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("access_token", tokenString)
            .field("type", "Bearer")
            .field("expires_in", expiresIn.seconds());
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DelegatePkiResponse that = (DelegatePkiResponse) o;
        return Objects.equals(tokenString, that.tokenString) &&
            Objects.equals(expiresIn, that.expiresIn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tokenString, expiresIn);
    }
}

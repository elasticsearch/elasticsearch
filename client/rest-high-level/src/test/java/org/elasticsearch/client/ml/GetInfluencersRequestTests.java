/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class GetInfluencersRequestTests extends AbstractXContentTestCase<GetInfluencersRequest> {

    @Override
    protected GetInfluencersRequest createTestInstance() {
        GetInfluencersRequest request = new GetInfluencersRequest(randomAlphaOfLengthBetween(1, 20));

        if (randomBoolean()) {
            request.setStart(String.valueOf(randomLong()));
        }
        if (randomBoolean()) {
            request.setEnd(String.valueOf(randomLong()));
        }
        if (randomBoolean()) {
            request.setExcludeInterim(randomBoolean());
        }
        if (randomBoolean()) {
            request.setInfluencerScore(randomDouble());
        }
        if (randomBoolean()) {
            int from = randomInt(10000);
            int size = randomInt(10000);
            request.setPageParams(new PageParams(from, size));
        }
        if (randomBoolean()) {
            request.setSort("influencer_score");
        }
        if (randomBoolean()) {
            request.setDescending(randomBoolean());
        }
        if (randomBoolean()) {
            request.setExcludeInterim(randomBoolean());
        }
        return request;
    }

    @Override
    protected GetInfluencersRequest doParseInstance(XContentParser parser) throws IOException {
        return GetInfluencersRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}

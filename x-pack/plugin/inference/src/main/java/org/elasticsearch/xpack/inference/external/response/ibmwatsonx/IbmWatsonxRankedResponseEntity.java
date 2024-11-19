/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.inference.external.response.ibmwatsonx;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownField;
import static org.elasticsearch.common.xcontent.XContentParserUtils.throwUnknownToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class IbmWatsonxRankedResponseEntity {

    private static final Logger logger = LogManager.getLogger(IbmWatsonxRankedResponseEntity.class);

    /**
     * Parses the Ibm Watsonx ranked response.
     *
     * For a request like:
     *     "model": "rerank-english-v2.0",
     *     "query": "What is the capital of the United States?",
     *     "return_documents": true,
     *     "top_n": 3,
     *     "documents": ["Carson City is the capital city of the American state of Nevada.",
     *                   "The Commonwealth of the Northern Mariana ... Its capital is Saipan.",
     *                   "Washington, D.C. (also known as simply Washington or D.C., ... It is a federal district.",
     *                   "Capital punishment (the death penalty) ... As of 2017, capital punishment is legal in 30 of the 50 states."]
     * <p>
     *  The response will look like (without whitespace):
     *     {
     *     "id": "1983d114-a6e8-4940-b121-eb4ac3f6f703",
     *     "results": [
     *         {
     *             "document": {
     *                 "text": "Washington, D.C.  is the capital of the United States. It is a federal district."
     *             },
     *             "index": 2,
     *             "relevance_score": 0.98005307
     *         },
     *         {
     *             "document": {
     *                 "text": "Capital punishment (the death penalty) As of 2017, capital punishment is legal in 30 of the 50 states."
     *             },
     *             "index": 3,
     *             "relevance_score": 0.27904198
     *         },
     *         {
     *             "document": {
     *                 "text": "Carson City is the capital city of the American state of Nevada."
     *             },
     *             "index": 0,
     *             "relevance_score": 0.10194652
     *         }
     *     ],
     *     "meta": {
     *         "api_version": {
     *             "version": "1"
     *         },
     *         "billed_units": {
     *             "search_units": 1
     *         }
     *     }
     *
     * @param response the http response from ibm watsonx
     * @return the parsed response
     * @throws IOException if there is an error parsing the response
     */
    public static InferenceServiceResults fromResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "results", FAILED_TO_FIND_FIELD_TEMPLATE); // TODO error message

            token = jsonParser.currentToken();
            if (token == XContentParser.Token.START_ARRAY) {
                return new RankedDocsResults(parseList(jsonParser, IbmWatsonxRankedResponseEntity::parseRankedDocObject));
            } else {
                throwUnknownToken(token, jsonParser);
            }

            // This should never be reached. The above code should either return successfully or hit the throwUnknownToken
            // or throw a parsing exception
            throw new IllegalStateException("Reached an invalid state while parsing the Watsonx response");
        }
    }

    private static RankedDocsResults.RankedDoc parseRankedDocObject(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        int index = -1;
        float score = -1;
        String documentText = null;
        parser.nextToken();
        while (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                switch (parser.currentName()) {
                    case "index":
                        parser.nextToken(); // move to VALUE_NUMBER
                        index = parser.intValue();
                        parser.nextToken(); // move to next FIELD_NAME or END_OBJECT
                        break;
                    case "score":
                        parser.nextToken(); // move to VALUE_NUMBER
                        score = parser.floatValue();
                        parser.nextToken(); // move to next FIELD_NAME or END_OBJECT
                        break;
                    case "document":
                        parser.nextToken(); // move to START_OBJECT; document text is wrapped in an object
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                        do {
                            if (parser.currentToken() == XContentParser.Token.FIELD_NAME && parser.currentName().equals("text")) {
                                parser.nextToken(); // move to VALUE_STRING
                                documentText = parser.text();
                            }
                        } while (parser.nextToken() != XContentParser.Token.END_OBJECT);
                        parser.nextToken();// move past END_OBJECT
                        // parser should now be at the next FIELD_NAME or END_OBJECT
                        break;
                    default:
                        throwUnknownField(parser.currentName(), parser);
                }
            } else {
                parser.nextToken();
            }
        }

        if (index == -1) {
            logger.warn("Failed to find required field [index] in Watsonx rerank response");
        }
        if (score == -1) {
            logger.warn("Failed to find required field [relevance_score] in Watsonx rerank response");
        }
        // documentText may or may not be present depending on the request parameter

        return new RankedDocsResults.RankedDoc(index, score, documentText);
    }

    private IbmWatsonxRankedResponseEntity() {}

    static String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Watsonx rerank response";
}

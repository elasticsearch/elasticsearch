/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.suggest.SuggestContextParser;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.io.IOException;

import static org.elasticsearch.search.suggest.SuggestUtils.parseSuggestContext;

/**
 *
 */
public class CompletionSuggestParser implements SuggestContextParser {

    private CompletionSuggester completionSuggester;

    public CompletionSuggestParser(CompletionSuggester completionSuggester) {
        this.completionSuggester = completionSuggester;
    }

    @Override
    public SuggestionSearchContext.SuggestionContext parse(XContentParser parser, MapperService mapperService) throws IOException {
        XContentParser.Token token;
        String fieldName = null;
        CompletionSuggestionContext suggestion = new CompletionSuggestionContext(completionSuggester);
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (!parseSuggestContext(parser, mapperService, fieldName, suggestion))  {
                    if (token == XContentParser.Token.VALUE_BOOLEAN && "fuzzy".equals(fieldName)) {
                        suggestion.setFuzzy(parser.booleanValue());
                    } else if (token == XContentParser.Token.VALUE_STRING && "score_mode".equals(fieldName) || "scoreMode".equals(fieldName)) {
                        String sScoreMode = parser.text();
                        if ("max".equals(sScoreMode)) {
                            suggestion.setScoreMode(CompletionSuggestionContext.ScoreMode.Max);
                        } else if ("min".equals(sScoreMode)) {
                            suggestion.setScoreMode(CompletionSuggestionContext.ScoreMode.Min);
                        } else if ("total".equals(sScoreMode) || "sum".equals(sScoreMode)) {
                            suggestion.setScoreMode(CompletionSuggestionContext.ScoreMode.Total);
                        } else if ("product".equals(sScoreMode) || "multiply".equals(sScoreMode)) {
                            suggestion.setScoreMode(CompletionSuggestionContext.ScoreMode.Multiply);
                        } else {
                            throw new ElasticSearchIllegalArgumentException("[rescore] illegal score_mode [" + sScoreMode + "]");
                        }
                    }
                }
            } else if (token == XContentParser.Token.START_OBJECT && "fuzzy".equals(fieldName)) {
                suggestion.setFuzzy(true);
                String fuzzyConfigName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fuzzyConfigName = parser.currentName();
                    } else if (token.isValue()) {
                        if ("edit_distance".equals(fuzzyConfigName) || "editDistance".equals(fuzzyConfigName)) {
                            suggestion.setFuzzyEditDistance(parser.intValue());
                        } else if ("transpositions".equals(fuzzyConfigName)) {
                            suggestion.setFuzzyTranspositions(parser.booleanValue());
                        } else if ("min_length".equals(fuzzyConfigName) || "minLength".equals(fuzzyConfigName)) {
                            suggestion.setFuzzyMinLength(parser.intValue());
                        } else if ("prefix_length".equals(fuzzyConfigName) || "prefixLength".equals(fuzzyConfigName)) {
                            suggestion.setFuzzyPrefixLength(parser.intValue());
                        }
                    }
                }
            } else {
                throw new ElasticSearchIllegalArgumentException("suggester[completion]  doesn't support field [" + fieldName + "]");
            }
        }
        suggestion.mapper(mapperService.smartNameFieldMapper(suggestion.getField()));

        return suggestion;
    }

}

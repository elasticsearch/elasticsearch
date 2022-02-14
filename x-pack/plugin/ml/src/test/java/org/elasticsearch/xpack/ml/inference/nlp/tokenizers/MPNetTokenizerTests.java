/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.MPNetTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;

public class MPNetTokenizerTests extends ESTestCase {

    public static final List<String> TEST_CASED_VOCAB = List.of(
        "Elastic",
        "##search",
        "is",
        "fun",
        "my",
        "little",
        "red",
        "car",
        "God",
        "##zilla",
        ".",
        ",",
        MPNetTokenizer.CLASS_TOKEN,
        MPNetTokenizer.SEPARATOR_TOKEN,
        MPNetTokenizer.MASK_TOKEN,
        MPNetTokenizer.UNKNOWN_TOKEN,
        "day",
        "Pancake",
        "with",
        MPNetTokenizer.PAD_TOKEN
    );

    private List<String> tokenStrings(List<WordPieceTokenFilter.WordPieceToken> tokens) {
        return tokens.stream().map(WordPieceTokenFilter.WordPieceToken::toString).collect(Collectors.toList());
    }

    public void testTokenize() {
        try (
            BertTokenizer tokenizer = MPNetTokenizer.mpBuilder(
                TEST_CASED_VOCAB,
                new MPNetTokenization(null, false, null, Tokenization.Truncate.NONE)
            ).build()
        ) {
            TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE);
            assertThat(tokenStrings(tokenization.getTokens()), contains("Elastic", "##search", "fun"));
            assertArrayEquals(new int[] { 0, 1, 3 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { 0, 0, 1 }, tokenization.getTokenMap());
        }
    }

    public void testMultiSeqTokenization() {
        try (
            MPNetTokenizer tokenizer = MPNetTokenizer.mpBuilder(
                TEST_CASED_VOCAB,
                new MPNetTokenization(null, false, null, Tokenization.Truncate.NONE)
            ).setDoLowerCase(false).setWithSpecialTokens(true).build()
        ) {
            TokenizationResult.Tokenization tokenization = tokenizer.tokenize(
                "Elasticsearch is fun",
                "Godzilla my little red car",
                Tokenization.Truncate.NONE
            );

            var tokenStream = Arrays.stream(tokenization.getTokenIds()).mapToObj(TEST_CASED_VOCAB::get).collect(Collectors.toList());
            assertThat(
                tokenStream,
                contains(
                    MPNetTokenizer.CLASS_TOKEN,
                    "Elastic",
                    "##search",
                    "is",
                    "fun",
                    MPNetTokenizer.SEPARATOR_TOKEN,
                    MPNetTokenizer.SEPARATOR_TOKEN,
                    "God",
                    "##zilla",
                    "my",
                    "little",
                    "red",
                    "car",
                    MPNetTokenizer.SEPARATOR_TOKEN
                )
            );
            assertArrayEquals(new int[] { 12, 0, 1, 2, 3, 13, 13, 8, 9, 4, 5, 6, 7, 13 }, tokenization.getTokenIds());
        }
    }

}

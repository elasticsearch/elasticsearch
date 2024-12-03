/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlChunkedTextExpansionResults;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;
import org.elasticsearch.xpack.core.utils.FloatConversionUtils;
import org.elasticsearch.xpack.inference.model.TestModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper.INFERENCE_METADATA_FIELDS_FEATURE_FLAG;
import static org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_EMBEDDINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.toSemanticTextFieldChunks;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SemanticTextFieldTests extends AbstractXContentTestCase<SemanticTextField> {
    private static final String NAME = "field";

    private IndexVersion currentIndexVersion;

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return n -> n.endsWith(CHUNKED_EMBEDDINGS_FIELD);
    }

    @Override
    protected void assertEqualInstances(SemanticTextField expectedInstance, SemanticTextField newInstance) {
        assertThat(newInstance.indexCreatedVersion(), equalTo(newInstance.indexCreatedVersion()));
        assertThat(newInstance.fieldName(), equalTo(expectedInstance.fieldName()));
        assertThat(newInstance.originalValues(), equalTo(expectedInstance.originalValues()));
        assertThat(newInstance.inference().modelSettings(), equalTo(expectedInstance.inference().modelSettings()));
        assertThat(newInstance.inference().chunks().size(), equalTo(expectedInstance.inference().chunks().size()));
        SemanticTextField.ModelSettings modelSettings = newInstance.inference().modelSettings();
        for (var entry : newInstance.inference().chunks().entrySet()) {
            var expectedChunks = expectedInstance.inference().chunks().get(entry.getKey());
            assertNotNull(expectedChunks);
            assertThat(entry.getValue().size(), equalTo(expectedChunks.size()));
            for (int i = 0; i < entry.getValue().size(); i++) {
                var actualChunk = entry.getValue().get(i);
                assertThat(actualChunk.text(), equalTo(expectedChunks.get(i).text()));
                assertThat(actualChunk.startOffset(), equalTo(expectedChunks.get(i).startOffset()));
                assertThat(actualChunk.endOffset(), equalTo(expectedChunks.get(i).endOffset()));
                switch (modelSettings.taskType()) {
                    case TEXT_EMBEDDING -> {
                        double[] expectedVector = parseDenseVector(
                            expectedChunks.get(i).rawEmbeddings(),
                            modelSettings.dimensions(),
                            expectedInstance.contentType()
                        );
                        double[] newVector = parseDenseVector(
                            actualChunk.rawEmbeddings(),
                            modelSettings.dimensions(),
                            newInstance.contentType()
                        );
                        assertArrayEquals(expectedVector, newVector, 0.0000001f);
                    }
                    case SPARSE_EMBEDDING -> {
                        List<WeightedToken> expectedTokens = parseWeightedTokens(
                            expectedChunks.get(i).rawEmbeddings(),
                            expectedInstance.contentType()
                        );
                        List<WeightedToken> newTokens = parseWeightedTokens(actualChunk.rawEmbeddings(), newInstance.contentType());
                        assertThat(newTokens, equalTo(expectedTokens));
                    }
                    default -> throw new AssertionError("Invalid task type " + modelSettings.taskType());
                }
            }
        }
    }

    @Override
    protected SemanticTextField createTestInstance() {
        currentIndexVersion = randomFrom(
            IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersions.INFERENCE_METADATA_FIELDS),
            IndexVersionUtils.randomVersionBetween(random(), IndexVersions.INFERENCE_METADATA_FIELDS, IndexVersion.current())
        );
        List<String> rawValues = randomList(1, 5, () -> randomSemanticTextInput().toString());
        try { // try catch required for override
            return randomSemanticText(
                currentIndexVersion,
                NAME,
                TestModel.createRandomInstance(),
                rawValues,
                randomFrom(XContentType.values())
            );
        } catch (IOException e) {
            fail("Failed to create random SemanticTextField instance");
        }
        return null;
    }

    @Override
    protected SemanticTextField doParseInstance(XContentParser parser) throws IOException {
        return SemanticTextField.parse(parser, new SemanticTextField.ParserContext(currentIndexVersion, NAME, parser.contentType()));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testModelSettingsValidation() {
        NullPointerException npe = expectThrows(NullPointerException.class, () -> {
            new SemanticTextField.ModelSettings(null, 10, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT);
        });
        assertThat(npe.getMessage(), equalTo("task type must not be null"));

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
            new SemanticTextField.ModelSettings(
                TaskType.COMPLETION,
                10,
                SimilarityMeasure.COSINE,
                DenseVectorFieldMapper.ElementType.FLOAT
            );
        });
        assertThat(ex.getMessage(), containsString("Wrong [task_type]"));

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> { new SemanticTextField.ModelSettings(TaskType.SPARSE_EMBEDDING, 10, null, null); }
        );
        assertThat(ex.getMessage(), containsString("[dimensions] is not allowed"));

        ex = expectThrows(IllegalArgumentException.class, () -> {
            new SemanticTextField.ModelSettings(TaskType.SPARSE_EMBEDDING, null, SimilarityMeasure.COSINE, null);
        });
        assertThat(ex.getMessage(), containsString("[similarity] is not allowed"));

        ex = expectThrows(IllegalArgumentException.class, () -> {
            new SemanticTextField.ModelSettings(TaskType.SPARSE_EMBEDDING, null, null, DenseVectorFieldMapper.ElementType.FLOAT);
        });
        assertThat(ex.getMessage(), containsString("[element_type] is not allowed"));

        ex = expectThrows(IllegalArgumentException.class, () -> {
            new SemanticTextField.ModelSettings(
                TaskType.TEXT_EMBEDDING,
                null,
                SimilarityMeasure.COSINE,
                DenseVectorFieldMapper.ElementType.FLOAT
            );
        });
        assertThat(ex.getMessage(), containsString("required [dimensions] field is missing"));

        ex = expectThrows(IllegalArgumentException.class, () -> {
            new SemanticTextField.ModelSettings(TaskType.TEXT_EMBEDDING, 10, null, DenseVectorFieldMapper.ElementType.FLOAT);
        });
        assertThat(ex.getMessage(), containsString("required [similarity] field is missing"));

        ex = expectThrows(IllegalArgumentException.class, () -> {
            new SemanticTextField.ModelSettings(TaskType.TEXT_EMBEDDING, 10, SimilarityMeasure.COSINE, null);
        });
        assertThat(ex.getMessage(), containsString("required [element_type] field is missing"));
    }

    public static InferenceChunkedTextEmbeddingFloatResults randomInferenceChunkedTextEmbeddingFloatResults(
        Model model,
        List<String> inputs
    ) throws IOException {
        List<InferenceChunkedTextEmbeddingFloatResults.InferenceFloatEmbeddingChunk> chunks = new ArrayList<>();
        for (String input : inputs) {
            float[] values = new float[model.getServiceSettings().dimensions()];
            for (int j = 0; j < values.length; j++) {
                values[j] = (float) randomDouble();
            }
            chunks.add(new InferenceChunkedTextEmbeddingFloatResults.InferenceFloatEmbeddingChunk(input, values));
        }
        return new InferenceChunkedTextEmbeddingFloatResults(chunks);
    }

    public static InferenceChunkedSparseEmbeddingResults randomSparseEmbeddings(List<String> inputs) {
        List<MlChunkedTextExpansionResults.ChunkedResult> chunks = new ArrayList<>();
        for (String input : inputs) {
            var tokens = new ArrayList<WeightedToken>();
            for (var token : input.split("\\s+")) {
                tokens.add(new WeightedToken(token, randomFloat()));
            }
            chunks.add(new MlChunkedTextExpansionResults.ChunkedResult(input, tokens));
        }
        return new InferenceChunkedSparseEmbeddingResults(chunks);
    }

    public static SemanticTextField randomSemanticText(
        IndexVersion indexVersion,
        String fieldName,
        Model model,
        List<String> inputs,
        XContentType contentType
    ) throws IOException {
        ChunkedInferenceServiceResults results = switch (model.getTaskType()) {
            case TEXT_EMBEDDING -> randomInferenceChunkedTextEmbeddingFloatResults(model, inputs);
            case SPARSE_EMBEDDING -> randomSparseEmbeddings(inputs);
            default -> throw new AssertionError("invalid task type: " + model.getTaskType().name());
        };
        return semanticTextFieldFromChunkedInferenceResults(indexVersion, fieldName, model, inputs, results, contentType);
    }

    public static SemanticTextField semanticTextFieldFromChunkedInferenceResults(
        IndexVersion indexVersion,
        String fieldName,
        Model model,
        List<String> inputs,
        ChunkedInferenceServiceResults results,
        XContentType contentType
    ) {
        final boolean useInferenceMetadataFields = indexVersion.onOrAfter(IndexVersions.INFERENCE_METADATA_FIELDS)
            && INFERENCE_METADATA_FIELDS_FEATURE_FLAG.isEnabled();

        // TODO: Define separator char someplace common to semantic text & chunking code
        return new SemanticTextField(
            indexVersion,
            fieldName,
            useInferenceMetadataFields ? null : inputs,
            new SemanticTextField.InferenceResult(
                model.getInferenceEntityId(),
                new SemanticTextField.ModelSettings(model),
                Map.of(
                    fieldName,
                    toSemanticTextFieldChunks(
                        Strings.collectionToDelimitedString(inputs, String.valueOf(MULTIVAL_SEP_CHAR)),
                        List.of(results),
                        contentType,
                        useInferenceMetadataFields
                    )
                )
            ),
            contentType
        );
    }

    /**
     * Returns a randomly generated object for Semantic Text tests purpose.
     */
    public static Object randomSemanticTextInput() {
        if (rarely()) {
            return switch (randomIntBetween(0, 4)) {
                case 0 -> randomInt();
                case 1 -> randomLong();
                case 2 -> randomFloat();
                case 3 -> randomBoolean();
                case 4 -> randomDouble();
                default -> throw new IllegalStateException("Illegal state while generating random semantic text input");
            };
        } else {
            return randomAlphaOfLengthBetween(10, 20);
        }
    }

    public static ChunkedInferenceServiceResults toChunkedResult(SemanticTextField field) throws IOException {
        switch (field.inference().modelSettings().taskType()) {
            case SPARSE_EMBEDDING -> {
                List<MlChunkedTextExpansionResults.ChunkedResult> chunks = new ArrayList<>();
                for (var entry : field.inference().chunks().entrySet()) {
                    for (var chunk : entry.getValue()) {
                        var tokens = parseWeightedTokens(chunk.rawEmbeddings(), field.contentType());
                        chunks.add(new MlChunkedTextExpansionResults.ChunkedResult(chunk.text(), tokens));
                    }
                }
                return new InferenceChunkedSparseEmbeddingResults(chunks);
            }
            case TEXT_EMBEDDING -> {
                List<InferenceChunkedTextEmbeddingFloatResults.InferenceFloatEmbeddingChunk> chunks = new ArrayList<>();
                for (var entry : field.inference().chunks().entrySet()) {
                    for (var chunk : entry.getValue()) {
                        double[] values = parseDenseVector(
                            chunk.rawEmbeddings(),
                            field.inference().modelSettings().dimensions(),
                            field.contentType()
                        );
                        chunks.add(
                            new InferenceChunkedTextEmbeddingFloatResults.InferenceFloatEmbeddingChunk(
                                chunk.text(),
                                FloatConversionUtils.floatArrayOf(values)
                            )
                        );
                    }
                }
                return new InferenceChunkedTextEmbeddingFloatResults(chunks);
            }
            default -> throw new AssertionError("Invalid task_type: " + field.inference().modelSettings().taskType().name());
        }
    }

    private static double[] parseDenseVector(BytesReference value, int numDims, XContentType contentType) {
        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, value, contentType)) {
            parser.nextToken();
            assertThat(parser.currentToken(), equalTo(XContentParser.Token.START_ARRAY));
            double[] values = new double[numDims];
            for (int i = 0; i < numDims; i++) {
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
                values[i] = parser.doubleValue();
            }
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_ARRAY));
            return values;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<WeightedToken> parseWeightedTokens(BytesReference value, XContentType contentType) {
        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, value, contentType)) {
            Map<String, Object> map = parser.map();
            List<WeightedToken> weightedTokens = new ArrayList<>();
            for (var entry : map.entrySet()) {
                weightedTokens.add(new WeightedToken(entry.getKey(), ((Number) entry.getValue()).floatValue()));
            }
            return weightedTokens;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

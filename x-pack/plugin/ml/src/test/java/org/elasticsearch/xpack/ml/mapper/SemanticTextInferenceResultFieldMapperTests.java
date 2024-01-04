/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.mapper;

import org.apache.commons.math3.util.Pair;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MetadataMapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.LeafNestedDocuments;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SemanticTextInferenceResultFieldMapperTests extends MetadataMapperTestCase {
    private record SemanticTextInferenceResults(String fieldName, List<Pair<Map<String, Float>, String>> inferenceResults) {}
    private record VisitedChildDocInfo(String path, int sparseVectorDims) {}

    @Override
    protected String fieldName() {
        return SemanticTextInferenceResultFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {

    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MachineLearning(Settings.EMPTY));
    }

    public void testSuccessfulParse() throws IOException {
        final String fieldName1 = randomAlphaOfLengthBetween(5, 15);
        final String fieldName2 = randomAlphaOfLengthBetween(5, 15);

        DocumentMapper documentMapper = createDocumentMapper(mapping(b -> {
            addSemanticTextMapping(b, fieldName1, randomAlphaOfLength(8));
            addSemanticTextMapping(b, fieldName2, randomAlphaOfLength(8));
        }));
        ParsedDocument doc = documentMapper.parse(source(b -> addSemanticTextInferenceResults(b, List.of(
            new SemanticTextInferenceResults(fieldName1, List.of(
                new Pair<>(Map.of(
                    "a", randomFloat(),
                    "b", randomFloat()
                ),
                    "a b"
                ),
                new Pair<>(Map.of(
                    "c", randomFloat()
                ),
                    "c"
                )
            )),
            new SemanticTextInferenceResults(fieldName2, List.of(
                new Pair<>(Map.of(
                    "d", randomFloat(),
                    "e", randomFloat(),
                    "f", randomFloat()
                ),
                    "d e f"
                )
            ))
        ))));

        Set<VisitedChildDocInfo> visitedChildDocs = new HashSet<>();
        Set<VisitedChildDocInfo> expectedVisitedChildDocs = Set.of(
            new VisitedChildDocInfo(fieldName1, 2),
            new VisitedChildDocInfo(fieldName1, 1),
            new VisitedChildDocInfo(fieldName2, 3)
        );

        List<LuceneDocument> luceneDocs = doc.docs();
        assertEquals(4, luceneDocs.size());
        assertValidChildDoc(luceneDocs.get(0), doc.rootDoc(), visitedChildDocs);
        assertValidChildDoc(luceneDocs.get(1), doc.rootDoc(), visitedChildDocs);
        assertValidChildDoc(luceneDocs.get(2), doc.rootDoc(), visitedChildDocs);
        assertEquals(doc.rootDoc(), luceneDocs.get(3));
        assertNull(luceneDocs.get(3).getParent());
        assertEquals(expectedVisitedChildDocs, visitedChildDocs);

        // TODO: Need to test how the docs are indexed?
//        MapperService nestedMapperService = createMapperService(mapping(b -> {
//            addInferenceResultsNestedMapping(b, fieldName1);
//            addInferenceResultsNestedMapping(b, fieldName2);
//        }));
//        withLuceneIndex(nestedMapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
//            NestedDocuments nested = new NestedDocuments(
//                nestedMapperService.mappingLookup(),
//                QueryBitSetProducer::new,
//                IndexVersion.current());
//            LeafNestedDocuments leaf = nested.getLeafNestedDocuments(reader.leaves().get(0));
//
//            assertNotNull(leaf.advance(0));
//            assertEquals(0, leaf.doc());
//            assertEquals(2, leaf.rootDoc());
//            assertEquals(new SearchHit.NestedIdentity(fieldName1, 0, null), leaf.nestedIdentity());
//
//            assertNotNull(leaf.advance(1));
//            assertEquals(1, leaf.doc());
//            assertEquals(2, leaf.rootDoc());
//            assertEquals(new SearchHit.NestedIdentity(fieldName2, 1, null), leaf.nestedIdentity());
//
//            assertNull(leaf.advance(2));
//            assertEquals(2, leaf.doc());
//            assertEquals(2, leaf.rootDoc());
//            assertNull(leaf.nestedIdentity());
//        });
    }

    private static void addSemanticTextMapping(XContentBuilder mappingBuilder, String fieldName, String modelId) throws IOException {
        mappingBuilder.startObject(fieldName);
        mappingBuilder.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
        mappingBuilder.field("model_id", modelId);
        mappingBuilder.endObject();
    }

    private static void addSemanticTextInferenceResults(
        XContentBuilder sourceBuilder,
        List<SemanticTextInferenceResults> semanticTextInferenceResults) throws IOException {

        Map<String, List<Map<String, Object>>> inferenceResultsMap = new HashMap<>();
        for (SemanticTextInferenceResults semanticTextInferenceResult : semanticTextInferenceResults) {
            List<Map<String, Object>> parsedInferenceResults = new ArrayList<>(semanticTextInferenceResult.inferenceResults().size());
            for (Pair<Map<String, Float>, String> inferenceResults : semanticTextInferenceResult.inferenceResults()) {
                parsedInferenceResults.add(Map.of(
                    SemanticTextInferenceResultFieldMapper.SPARSE_VECTOR_SUBFIELD_NAME, inferenceResults.getFirst(),
                    SemanticTextInferenceResultFieldMapper.TEXT_SUBFIELD_NAME, inferenceResults.getSecond()
                ));
            }

            inferenceResultsMap.put(semanticTextInferenceResult.fieldName(), parsedInferenceResults);
        }

        sourceBuilder.field(SemanticTextInferenceResultFieldMapper.NAME, inferenceResultsMap);
    }

    private static void addInferenceResultsNestedMapping(XContentBuilder mappingBuilder, String semanticTextFieldName) throws IOException {
        mappingBuilder.startObject(semanticTextFieldName);
        mappingBuilder.field("type", "nested");
        mappingBuilder.startObject("properties");
        mappingBuilder.startObject(SemanticTextInferenceResultFieldMapper.SPARSE_VECTOR_SUBFIELD_NAME);
        mappingBuilder.field("type", "sparse_vector");
        mappingBuilder.endObject();
        mappingBuilder.startObject(SemanticTextInferenceResultFieldMapper.TEXT_SUBFIELD_NAME);
        mappingBuilder.field("type", "text");
        mappingBuilder.field("index", false);
        mappingBuilder.endObject();
        mappingBuilder.endObject();
        mappingBuilder.endObject();
    }

    private static void assertValidChildDoc(
        LuceneDocument childDoc,
        LuceneDocument expectedParent,
        Set<VisitedChildDocInfo> visitedChildDocs) {

        assertEquals(expectedParent, childDoc.getParent());
        visitedChildDocs.add(new VisitedChildDocInfo(childDoc.getPath(), childDoc.getFields(
            childDoc.getPath() + "." + SemanticTextInferenceResultFieldMapper.SPARSE_VECTOR_SUBFIELD_NAME).size()));
    }
}

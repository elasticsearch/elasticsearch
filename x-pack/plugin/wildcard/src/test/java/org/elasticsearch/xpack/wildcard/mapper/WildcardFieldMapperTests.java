/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xpack.wildcard.Wildcard;
import org.elasticsearch.xpack.wildcard.mapper.WildcardFieldMapper.Builder;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WildcardFieldMapperTests extends MapperTestCase {

    static SearchExecutionContext createMockSearchExecutionContext(boolean allowExpensiveQueries, Version version) {
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.allowExpensiveQueries()).thenReturn(allowExpensiveQueries);
        when(searchExecutionContext.indexVersionCreated()).thenReturn(version);
        return searchExecutionContext;
    }

    private static final String KEYWORD_FIELD_NAME = "keyword_field";
    private static final String WILDCARD_FIELD_NAME = "wildcard_field";
    public static final SearchExecutionContext MOCK_CONTEXT = createMockSearchExecutionContext(true, Version.CURRENT);

    static final int MAX_FIELD_LENGTH = 30;
    static WildcardFieldMapper wildcardFieldType;
    static WildcardFieldMapper wildcardFieldType79;
    static KeywordFieldMapper keywordFieldType;

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singleton(new Wildcard());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        Builder builder = new WildcardFieldMapper.Builder(WILDCARD_FIELD_NAME, Version.CURRENT);
        builder.ignoreAbove(MAX_FIELD_LENGTH);
        wildcardFieldType = builder.build(new ContentPath(0));

        Builder builder79 = new WildcardFieldMapper.Builder(WILDCARD_FIELD_NAME, Version.V_7_9_0);
        wildcardFieldType79 = builder79.build(new ContentPath(0));

        org.elasticsearch.index.mapper.KeywordFieldMapper.Builder kwBuilder = new KeywordFieldMapper.Builder(KEYWORD_FIELD_NAME);
        keywordFieldType = kwBuilder.build(new ContentPath(0));
        super.setUp();
    }

    public void testTooBigKeywordField() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(WildcardFieldMapper.WILDCARD_ANALYZER_7_10);
        iwc.setMergePolicy(newTieredMergePolicy(random()));
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        // Create a string that is too large and will not be indexed
        String docContent = randomABString(MAX_FIELD_LENGTH + 1);
        Document doc = new Document();
        ParseContext.Document parseDoc = new ParseContext.Document();
        addFields(parseDoc, doc, docContent);
        indexDoc(parseDoc, doc, iw);

        iw.forceMerge(1);
        DirectoryReader reader = iw.getReader();
        IndexSearcher searcher = newSearcher(reader);
        iw.close();

        Query wildcardFieldQuery = wildcardFieldType.fieldType().wildcardQuery("*a*", null, null);
        TopDocs wildcardFieldTopDocs = searcher.search(wildcardFieldQuery, 10, Sort.INDEXORDER);
        assertThat(wildcardFieldTopDocs.totalHits.value, equalTo(0L));

        reader.close();
        dir.close();
    }

    public void testBWCIndexVersion() throws IOException {
        // Create old format index using wildcard ngram analyzer used in 7.9 launch
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(WildcardFieldMapper.WILDCARD_ANALYZER_7_9);
        iwc.setMergePolicy(newTieredMergePolicy(random()));
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        Document doc = new Document();
        ParseContext.Document parseDoc = new ParseContext.Document();
        addFields(parseDoc, doc, "a b");
        indexDoc(parseDoc, doc, iw);

        iw.forceMerge(1);
        DirectoryReader reader = iw.getReader();
        IndexSearcher searcher = newSearcher(reader);
        iw.close();


        // Unnatural circumstance - testing we fail if we were to use the new analyzer on old index
        Query oldWildcardFieldQuery = wildcardFieldType.fieldType().wildcardQuery("a b", null, null);
        TopDocs oldWildcardFieldTopDocs = searcher.search(oldWildcardFieldQuery, 10, Sort.INDEXORDER);
        assertThat(oldWildcardFieldTopDocs.totalHits.value, equalTo(0L));


        // Natural circumstance test we revert to the old analyzer for old indices
        Query wildcardFieldQuery = wildcardFieldType79.fieldType().wildcardQuery("a b", null, null);
        TopDocs wildcardFieldTopDocs = searcher.search(wildcardFieldQuery, 10, Sort.INDEXORDER);
        assertThat(wildcardFieldTopDocs.totalHits.value, equalTo(1L));

        reader.close();
        dir.close();
    }

    //Test long query strings don't cause exceptions
    public void testTooBigQueryField() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(WildcardFieldMapper.WILDCARD_ANALYZER_7_10);
        iwc.setMergePolicy(newTieredMergePolicy(random()));
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        // Create a string that is too large and will not be indexed
        String docContent = randomABString(10);
        Document doc = new Document();
        ParseContext.Document parseDoc = new ParseContext.Document();
        addFields(parseDoc, doc, docContent);
        indexDoc(parseDoc, doc, iw);

        iw.forceMerge(1);
        DirectoryReader reader = iw.getReader();
        IndexSearcher searcher = newSearcher(reader);
        iw.close();

        // Test wildcard query
        String queryString = randomABString((BooleanQuery.getMaxClauseCount() * 2) + 1);
        Query wildcardFieldQuery = wildcardFieldType.fieldType().wildcardQuery(queryString, null, null);
        TopDocs wildcardFieldTopDocs = searcher.search(wildcardFieldQuery, 10, Sort.INDEXORDER);
        assertThat(wildcardFieldTopDocs.totalHits.value, equalTo(0L));

        // Test regexp query
        wildcardFieldQuery = wildcardFieldType.fieldType().regexpQuery(queryString, RegExp.ALL, 0, 20000, null, MOCK_CONTEXT);
        wildcardFieldTopDocs = searcher.search(wildcardFieldQuery, 10, Sort.INDEXORDER);
        assertThat(wildcardFieldTopDocs.totalHits.value, equalTo(0L));


        reader.close();
        dir.close();
    }


    public void testTermAndPrefixQueryIgnoreWildcardSyntax() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(WildcardFieldMapper.WILDCARD_ANALYZER_7_10);
        iwc.setMergePolicy(newTieredMergePolicy(random()));
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        Document doc = new Document();
        ParseContext.Document parseDoc = new ParseContext.Document();
        addFields(parseDoc, doc, "f*oo?");
        indexDoc(parseDoc, doc, iw);

        iw.forceMerge(1);
        DirectoryReader reader = iw.getReader();
        IndexSearcher searcher = newSearcher(reader);
        iw.close();

        expectTermMatch(searcher, "f*oo*", 0);
        expectTermMatch(searcher, "f*oo?", 1);
        expectTermMatch(searcher, "*oo?", 0);

        expectPrefixMatch(searcher, "f*o", 1);
        expectPrefixMatch(searcher, "f*oo?", 1);
        expectPrefixMatch(searcher, "f??o", 0);

        reader.close();
        dir.close();
    }

    private void expectTermMatch(IndexSearcher searcher, String term,long count) throws IOException {
        Query q = wildcardFieldType.fieldType().termQuery(term, MOCK_CONTEXT);
        TopDocs td = searcher.search(q, 10, Sort.RELEVANCE);
        assertThat(td.totalHits.value, equalTo(count));
    }

    private void expectPrefixMatch(IndexSearcher searcher, String term,long count) throws IOException {
        Query q = wildcardFieldType.fieldType().prefixQuery(term, null, MOCK_CONTEXT);
        TopDocs td = searcher.search(q, 10, Sort.RELEVANCE);
        assertThat(td.totalHits.value, equalTo(count));
    }


    public void testSearchResultsVersusKeywordField() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(WildcardFieldMapper.WILDCARD_ANALYZER_7_10);
        iwc.setMergePolicy(newTieredMergePolicy(random()));
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        int numDocs = 100;
        HashSet<String> values = new HashSet<>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            ParseContext.Document parseDoc = new ParseContext.Document();
            String docContent = randomABString(1 + randomInt(MAX_FIELD_LENGTH - 1));
            if (values.contains(docContent) == false) {
                addFields(parseDoc, doc, docContent);
                values.add(docContent);
            }
            // Occasionally add a multi-value field
            if (randomBoolean()) {
                docContent = randomABString(1 + randomInt(MAX_FIELD_LENGTH - 1));
                if (values.contains(docContent) == false) {
                    addFields(parseDoc, doc, docContent);
                    values.add(docContent);
                }
            }
            indexDoc(parseDoc, doc, iw);

        }

        iw.forceMerge(1);
        DirectoryReader reader = iw.getReader();
        IndexSearcher searcher = newSearcher(reader);
        iw.close();

        int numSearches = 100;
        for (int i = 0; i < numSearches; i++) {

            Query wildcardFieldQuery = null;
            Query keywordFieldQuery = null;
            String pattern = null;
            switch (randomInt(4)) {
            case 0:
                pattern = getRandomWildcardPattern();
                boolean caseInsensitive = randomBoolean();
                wildcardFieldQuery = wildcardFieldType.fieldType().wildcardQuery(pattern, null, caseInsensitive, MOCK_CONTEXT);
                keywordFieldQuery = keywordFieldType.fieldType().wildcardQuery(pattern, null, caseInsensitive, MOCK_CONTEXT);
                break;
            case 1:
                pattern = getRandomRegexPattern(values);
                int matchFlags = randomBoolean()? 0 : RegExp.ASCII_CASE_INSENSITIVE;
                wildcardFieldQuery = wildcardFieldType.fieldType().regexpQuery(pattern, RegExp.ALL, matchFlags, 20000, null, MOCK_CONTEXT);
                keywordFieldQuery = keywordFieldType.fieldType().regexpQuery(pattern, RegExp.ALL, matchFlags,20000, null, MOCK_CONTEXT);
                break;
            case 2:
                pattern = randomABString(5);
                boolean caseInsensitivePrefix = randomBoolean();
                wildcardFieldQuery = wildcardFieldType.fieldType().prefixQuery(pattern, null, caseInsensitivePrefix, MOCK_CONTEXT);
                keywordFieldQuery = keywordFieldType.fieldType().prefixQuery(pattern, null, caseInsensitivePrefix, MOCK_CONTEXT);
                break;
            case 3:
                int edits = randomInt(2);
                int prefixLength = randomInt(4);
                pattern = getRandomFuzzyPattern(values, edits, prefixLength);
                Fuzziness fuzziness = Fuzziness.AUTO;
                switch (edits) {
                    case 0:
                        fuzziness = Fuzziness.ZERO;
                        break;
                    case 1:
                        fuzziness = Fuzziness.ONE;
                        break;
                    case 2:
                        fuzziness = Fuzziness.TWO;
                        break;
                    default:
                        break;
                }
                // Prefix length shouldn't be longer than selected search string
                // BUT keyword field has a bug with prefix length when equal - see https://github.com/elastic/elasticsearch/issues/55790
                // so we opt for one less
                prefixLength = Math.min(pattern.length() - 1 , prefixLength);
                boolean transpositions = randomBoolean();

                wildcardFieldQuery = wildcardFieldType.fieldType().fuzzyQuery(pattern, fuzziness, prefixLength, 50,
                    transpositions, MOCK_CONTEXT);
                keywordFieldQuery = keywordFieldType.fieldType().fuzzyQuery(pattern, fuzziness, prefixLength, 50,
                    transpositions, MOCK_CONTEXT);
                break;
            case 4:
                TermRangeQuery trq = getRandomRange(values);
                wildcardFieldQuery = wildcardFieldType.fieldType().rangeQuery(trq.getLowerTerm(),trq.getUpperTerm(), trq.includesLower(),
                    trq.includesUpper(), null, null, null, MOCK_CONTEXT);
                keywordFieldQuery = keywordFieldType.fieldType().rangeQuery(trq.getLowerTerm(),trq.getUpperTerm(), trq.includesLower(),
                    trq.includesUpper(), null, null, null, MOCK_CONTEXT);
                break;

            }
            TopDocs kwTopDocs = searcher.search(keywordFieldQuery, values.size() + 1, Sort.RELEVANCE);
            TopDocs wildcardFieldTopDocs = searcher.search(wildcardFieldQuery, values.size() + 1, Sort.RELEVANCE);
            assertThat(keywordFieldQuery + "\n" + wildcardFieldQuery,
                wildcardFieldTopDocs.totalHits.value, equalTo(kwTopDocs.totalHits.value));

            HashSet<Integer> expectedDocs = new HashSet<>();
            for (ScoreDoc topDoc : kwTopDocs.scoreDocs) {
                expectedDocs.add(topDoc.doc);
            }
            for (ScoreDoc wcTopDoc : wildcardFieldTopDocs.scoreDocs) {
                assertTrue(expectedDocs.remove(wcTopDoc.doc));
            }
            assertThat(expectedDocs.size(), equalTo(0));
        }

        //Test keyword and wildcard sort operations are also equivalent
        SearchExecutionContext searchExecutionContext = createMockContext();

        FieldSortBuilder wildcardSortBuilder = new FieldSortBuilder(WILDCARD_FIELD_NAME);
        SortField wildcardSortField = wildcardSortBuilder.build(searchExecutionContext).field;
        ScoreDoc[] wildcardHits = searcher.search(new MatchAllDocsQuery(), numDocs, new Sort(wildcardSortField)).scoreDocs;

        FieldSortBuilder keywordSortBuilder = new FieldSortBuilder(KEYWORD_FIELD_NAME);
        SortField keywordSortField = keywordSortBuilder.build(searchExecutionContext).field;
        ScoreDoc[] keywordHits = searcher.search(new MatchAllDocsQuery(), numDocs, new Sort(keywordSortField)).scoreDocs;

        assertThat(wildcardHits.length, equalTo(keywordHits.length));
        for (int i = 0; i < wildcardHits.length; i++) {
            assertThat(wildcardHits[i].doc, equalTo(keywordHits[i].doc));
        }

        reader.close();
        dir.close();
    }

    private void indexDoc(RandomIndexWriter iw, String value) throws IOException {
        Document doc = new Document();
        ParseContext.Document parseDoc = new ParseContext.Document();
        addFields(parseDoc, doc, value);
        indexDoc(parseDoc, doc, iw);
    }

    public void testRangeQueryVersusKeywordField() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(WildcardFieldMapper.WILDCARD_ANALYZER_7_10);
        iwc.setMergePolicy(newTieredMergePolicy(random()));
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        // Tests for acceleration strategy based on long common prefix
        indexDoc(iw, "C:\\Program Files\\a.txt");
        indexDoc(iw, "C:\\Program Files\\n.txt");
        indexDoc(iw, "C:\\Program Files\\z.txt");

        // Tests for acceleration strategy based on no common prefix
        indexDoc(iw, "a.txt");
        indexDoc(iw, "n.txt");
        indexDoc(iw, "z.txt");
        indexDoc(iw, "A.txt");
        indexDoc(iw, "N.txt");
        indexDoc(iw, "^.txt");
        indexDoc(iw, "Z.txt");

        iw.forceMerge(1);
        DirectoryReader reader = iw.getReader();
        IndexSearcher searcher = newSearcher(reader);
        iw.close();


        String [][] rangeTests = {
            {"C:\\Program Files\\a", "C:\\Program Files\\z"},
            {"C:\\Program Files\\a", "C:\\Program Files\\n"},
            {null, "C:\\Program Files\\z"},
            {"C:\\Program Files\\a", null},

            {"a.txt", "z.txt"},
            {"a.txt", "n.txt"},
            {null, "z.txt"},
            {"a.txt", null},
            {"A.txt", "z.txt"}
        };

        for (String[] bounds : rangeTests) {
            BytesRef lower = bounds[0] == null ? null :new BytesRef(bounds[0]);
            BytesRef upper = bounds[1] == null ? null :new BytesRef(bounds[1]);
            TermRangeQuery trq = new TermRangeQuery(WILDCARD_FIELD_NAME, lower, upper, randomBoolean(), randomBoolean());
            Query wildcardFieldQuery = wildcardFieldType.fieldType().rangeQuery(trq.getLowerTerm(),trq.getUpperTerm(), trq.includesLower(),
                trq.includesUpper(), null, null, null, MOCK_CONTEXT);
            Query keywordFieldQuery = keywordFieldType.fieldType().rangeQuery(trq.getLowerTerm(),trq.getUpperTerm(), trq.includesLower(),
                trq.includesUpper(), null, null, null, MOCK_CONTEXT);


            TopDocs kwTopDocs = searcher.search(keywordFieldQuery, 10, Sort.RELEVANCE);
            TopDocs wildcardFieldTopDocs = searcher.search(wildcardFieldQuery, 10, Sort.RELEVANCE);
            assertThat(wildcardFieldTopDocs.totalHits.value, equalTo(kwTopDocs.totalHits.value));

            HashSet<Integer> expectedDocs = new HashSet<>();
            for (ScoreDoc topDoc : kwTopDocs.scoreDocs) {
                expectedDocs.add(topDoc.doc);
            }
            for (ScoreDoc wcTopDoc : wildcardFieldTopDocs.scoreDocs) {
                assertTrue(expectedDocs.remove(wcTopDoc.doc));
            }
            assertThat(expectedDocs.size(), equalTo(0));

        }
        reader.close();
        dir.close();
    }

    public void testRegexAcceleration() throws IOException, ParseException {
        // All these expressions should rewrite to a match all with no verification step required at all
        String superfastRegexes[]= { ".*",  "...*..", "(foo|bar|.*)", "@"};
        for (String regex : superfastRegexes) {
            Query wildcardFieldQuery = wildcardFieldType.fieldType().regexpQuery(regex, RegExp.ALL, 0, 20000, null, MOCK_CONTEXT);
            assertTrue(wildcardFieldQuery instanceof DocValuesFieldExistsQuery);
        }
        String matchNoDocsRegexes[]= { ""};
        for (String regex : matchNoDocsRegexes) {
            Query wildcardFieldQuery = wildcardFieldType.fieldType().regexpQuery(regex, RegExp.ALL, 0, 20000, null, MOCK_CONTEXT);
            assertTrue(wildcardFieldQuery instanceof MatchNoDocsQuery);
        }

        // All of these regexes should be accelerated as the equivalent of the given QueryString query
        String acceleratedTests[][] = {
            {".*foo.*", "eoo"},
            {"..foobar","+eoo +ooa +oaa +aaq +aq_ +q__"},
            {"(maynotexist)?foobar","+eoo +ooa +oaa +aaq +aq_ +q__"},
            {".*/etc/passw.*", "+\\/es +esc +sc\\/ +c\\/o +\\/oa +oas +ass +ssw"},
            {".*etc/passwd",  " +esc +sc\\/ +c\\/o +\\/oa +oas +ass +ssw +swc +wc_ +c__"},
            {"(http|ftp)://foo.*",  "+((+gss +sso) eso) +(+\\/\\/\\/ +\\/\\/e +\\/eo +eoo)"},
            {"[Pp][Oo][Ww][Ee][Rr][Ss][Hh][Ee][Ll][Ll]\\.[Ee][Xx][Ee]",
                "+_oo +oow +owe +weq +eqs +qsg +sge +gek +ekk +kk\\/ +k\\/e +\\/ew +ewe +we_ +e__"},
            {"foo<1-100>bar",  "+(+_eo +eoo) +(+aaq +aq_ +q__)"},
            {"(aaa.+&.+bbb)cat", "+cas +as_ +s__"},
            {".a", "a__"}
            };
        for (String[] test : acceleratedTests) {
            String regex = test[0];
            String expectedAccelerationQueryString = test[1].replaceAll("_", ""+WildcardFieldMapper.TOKEN_START_OR_END_CHAR);
            Query wildcardFieldQuery = wildcardFieldType.fieldType().regexpQuery(regex, RegExp.ALL, 0, 20000, null, MOCK_CONTEXT);
            testExpectedAccelerationQuery(regex, wildcardFieldQuery, expectedAccelerationQueryString);
        }

        // All these expressions should rewrite to just the verification query (there's no ngram acceleration)
        // TODO we can possibly improve on some of these
        String matchAllButVerifyTests[]= { "..", "(a)?","(a|b){0,3}", "((foo)?|(foo|bar)?)", "@&~(abc.+)", "aaa.+&.+bbb"};
        for (String regex : matchAllButVerifyTests) {
            Query wildcardFieldQuery = wildcardFieldType.fieldType().regexpQuery(regex, RegExp.ALL, 0, 20000, null, MOCK_CONTEXT);
            assertTrue(regex +" was not a pure verify query " +formatQuery(wildcardFieldQuery),
                wildcardFieldQuery instanceof AutomatonQueryOnBinaryDv);
        }



        // Documentation - regexes that do try accelerate but we would like to improve in future versions.
        String suboptimalTests[][] = {
            // TODO short wildcards like a* OR b* aren't great so we just drop them.
            // Ideally we would attach to successors to create (acd OR bcd)
            { "[ab]cd",  "+cc_ +c__"}
            };
        for (String[] test : suboptimalTests) {
            String regex = test[0];
            String expectedAccelerationQueryString = test[1].replaceAll("_", ""+WildcardFieldMapper.TOKEN_START_OR_END_CHAR);
            Query wildcardFieldQuery = wildcardFieldType.fieldType().regexpQuery(regex, RegExp.ALL, 0, 20000, null, MOCK_CONTEXT);

            testExpectedAccelerationQuery(regex, wildcardFieldQuery, expectedAccelerationQueryString);
        }

    }
    // Make error messages more readable
    String formatQuery(Query q) {
        return q.toString().replaceAll(WILDCARD_FIELD_NAME+":", "").replaceAll(WildcardFieldMapper.TOKEN_START_STRING, "_");
    }

    public void testWildcardAcceleration() throws IOException, ParseException {

        // All these expressions should rewrite to MatchAll with no verification step required at all
        String superfastPattern[] = { "*", "**", "*?" };
        for (String pattern : superfastPattern) {
            Query wildcardFieldQuery = wildcardFieldType.fieldType().wildcardQuery(pattern, null, MOCK_CONTEXT);
            assertTrue(
                pattern + " was not a pure match all query " + formatQuery(wildcardFieldQuery),
                wildcardFieldQuery instanceof DocValuesFieldExistsQuery
            );
        }

        // All of these patterns should be accelerated.
        String tests[][] = {
            { "*foobar", "+eoo +ooa +oaa +aaq +aq_ +q__" },
            { "foobar*", "+_eo +eoo +ooa +oaa +aaq" },
            { "foo\\*bar*", "+_eo +eoo +oo\\/ +o\\/a +\\/aa +aaq" },
            { "foo\\?bar*", "+_eo +eoo +oo\\/ +o\\/a +\\/aa +aaq" },
            { "foo*bar", "+_eo +eoo +aaq +aq_ +q__" },
            { "foo?bar", "+_eo +eoo +aaq +aq_ +q__" },
            { "?foo*bar?", "+eoo +aaq" },
            { "*c", "+c__" } };
        for (String[] test : tests) {
            String pattern = test[0];
            String expectedAccelerationQueryString = test[1].replaceAll("_", "" + WildcardFieldMapper.TOKEN_START_OR_END_CHAR);
            Query wildcardFieldQuery = wildcardFieldType.fieldType().wildcardQuery(pattern, null, MOCK_CONTEXT);
            testExpectedAccelerationQuery(pattern, wildcardFieldQuery, expectedAccelerationQueryString);
            assertTrue(unwrapAnyConstantScore(wildcardFieldQuery) instanceof BooleanQuery);
        }

        // TODO All these expressions have no acceleration at all and could be improved
        String slowPatterns[] = { "??" };
        for (String pattern : slowPatterns) {
            Query wildcardFieldQuery = wildcardFieldType.fieldType().wildcardQuery(pattern, null, MOCK_CONTEXT);
            wildcardFieldQuery = unwrapAnyConstantScore(wildcardFieldQuery);
            assertTrue(
                pattern + " was not as slow as we assumed " + formatQuery(wildcardFieldQuery),
                wildcardFieldQuery instanceof AutomatonQueryOnBinaryDv
            );
        }

    }

    public void testQueryCachingEquality() throws IOException, ParseException {
        String pattern = "A*b*B?a";
        // Case sensitivity matters when it comes to caching
        Automaton caseSensitiveAutomaton = WildcardQuery.toAutomaton(new Term("field", pattern));
        Automaton caseInSensitiveAutomaton = AutomatonQueries.toCaseInsensitiveWildcardAutomaton(
            new Term("field", pattern),
            Integer.MAX_VALUE
        );
        AutomatonQueryOnBinaryDv csQ = new AutomatonQueryOnBinaryDv("field", pattern, caseSensitiveAutomaton);
        AutomatonQueryOnBinaryDv ciQ = new AutomatonQueryOnBinaryDv("field", pattern, caseInSensitiveAutomaton);
        assertNotEquals(csQ, ciQ);
        assertNotEquals(csQ.hashCode(), ciQ.hashCode());

        // Same query should be equal
        Automaton caseSensitiveAutomaton2 = WildcardQuery.toAutomaton(new Term("field", pattern));
        AutomatonQueryOnBinaryDv csQ2 = new AutomatonQueryOnBinaryDv("field", pattern, caseSensitiveAutomaton2);
        assertEquals(csQ, csQ2);
        assertEquals(csQ.hashCode(), csQ2.hashCode());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "wildcard");
    }

    @Override
    protected Object getSampleValueForDocument() {
        return "test";
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("null_value", b -> b.field("null_value", "foo"));
        checker.registerUpdateCheck(b -> b.field("ignore_above", 256),
            m -> assertEquals(256, ((WildcardFieldMapper)m).ignoreAbove()));

    }

    static class FuzzyTest {
        String pattern;
        int prefixLength;
        Fuzziness fuzziness;
        String expectedPrefixQuery;
        int expectedMinShouldMatch;
        String ngrams;

        FuzzyTest(
            String pattern,
            int prefixLength,
            Fuzziness fuzziness,
            String expectedPrefixQuery,
            int expectedMinShouldMatch,
            String ngrams
        ) {
            super();
            this.pattern = pattern;
            this.prefixLength = prefixLength;
            this.fuzziness = fuzziness;
            this.expectedPrefixQuery = expectedPrefixQuery;
            this.expectedMinShouldMatch = expectedMinShouldMatch;
            this.ngrams = ngrams;
        }

        Query getFuzzyQuery() {
            return wildcardFieldType.fieldType().fuzzyQuery(pattern, fuzziness, prefixLength, 50, true, MOCK_CONTEXT);
        }

        Query getExpectedApproxQuery() throws ParseException {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            if (expectedPrefixQuery != null) {
                String[] tokens = expectedPrefixQuery.split(" ");
                Query prefixQuery = null;
                if (tokens.length == 1) {
                    prefixQuery = new TermQuery(
                        new Term(WILDCARD_FIELD_NAME, tokens[0].replaceAll("_", WildcardFieldMapper.TOKEN_START_STRING))
                    );
                } else {
                    BooleanQuery.Builder pqb = new BooleanQuery.Builder();
                    for (String token : tokens) {
                        Query ngramQuery = new TermQuery(
                            new Term(WILDCARD_FIELD_NAME, token.replaceAll("_", WildcardFieldMapper.TOKEN_START_STRING))
                        );
                        pqb.add(ngramQuery, Occur.MUST);
                    }
                    prefixQuery = pqb.build();
                }

                if (ngrams == null) {
                    return prefixQuery;
                }
                bq.add(prefixQuery, Occur.MUST);
            }

            if (ngrams != null) {
                BooleanQuery.Builder nq = new BooleanQuery.Builder();
                String[] tokens = ngrams.split(" ");
                for (String token : tokens) {
                    Query ngramQuery = new TermQuery(
                        new Term(WILDCARD_FIELD_NAME, token.replaceAll("_", WildcardFieldMapper.TOKEN_START_STRING))
                    );
                    nq.add(ngramQuery, Occur.SHOULD);
                }
                nq.setMinimumNumberShouldMatch(expectedMinShouldMatch);
                bq.add(nq.build(), Occur.MUST);
            }
            return bq.build();
        }
    }

    public void testFuzzyAcceleration() throws IOException, ParseException {

        FuzzyTest[] tests = {
            new FuzzyTest("123456", 0, Fuzziness.ONE, null, 1, "113 355"),
            new FuzzyTest("1234567890", 2, Fuzziness.ONE, "_11", 1, "335 577"),
            new FuzzyTest("12345678901", 2, Fuzziness.ONE, "_11", 2, "335 577 901"),
            new FuzzyTest("12345678", 4, Fuzziness.ONE, "_11 113 133", 0, null)
        };
        for (FuzzyTest test : tests) {
            Query wildcardFieldQuery = test.getFuzzyQuery();
            testExpectedAccelerationQuery(test.pattern, wildcardFieldQuery, test.getExpectedApproxQuery());
        }
    }

    static class RangeTest {
        String lower;
        String upper;
        String ngrams;

        RangeTest(
            String lower,
            String upper,
            String ngrams
        ) {
            super();
            this.lower = lower;
            this.upper = upper;
            this.ngrams = ngrams;
        }

        Query getRangeQuery() {
            return wildcardFieldType.fieldType().rangeQuery(lower, upper, true, true, null, null, null, MOCK_CONTEXT);
        }

        Query getExpectedApproxQuery() throws ParseException {
            BooleanQuery.Builder bq = new BooleanQuery.Builder();
            if (ngrams != null) {
                String[] tokens = ngrams.split(" ");
                for (String token : tokens) {
                    Query ngramQuery = new TermQuery(
                        new Term(WILDCARD_FIELD_NAME, token.replaceAll("_", WildcardFieldMapper.TOKEN_START_STRING))
                    );
                    bq.add(ngramQuery, Occur.MUST);
                }
            }
            return bq.build();
        }
    }

    public void testRangeAcceleration() throws IOException, ParseException {

        RangeTest[] tests = {
            new RangeTest("c:/a.txt", "c:/z.txt", "_c/ c//"),
            new RangeTest("C:/ProgramFiles/a.txt", "C:/ProgramFiles/z/txt", "_c/ c// //o /oq oqo qog ogq gqa qam ame mei eik ike kes es/"),
        };
        for (RangeTest test : tests) {
            Query wildcardFieldQuery = test.getRangeQuery();
            testExpectedAccelerationQuery(test.lower + "-" + test.upper, wildcardFieldQuery, test.getExpectedApproxQuery());
        }
    }

    void testExpectedAccelerationQuery(String regex, Query combinedQuery, String expectedAccelerationQueryString) throws ParseException {

        QueryParser qsp = new QueryParser(WILDCARD_FIELD_NAME, new KeywordAnalyzer());
        Query expectedAccelerationQuery = qsp.parse(expectedAccelerationQueryString);
        testExpectedAccelerationQuery(regex, combinedQuery, expectedAccelerationQuery);
    }

    private Query unwrapAnyConstantScore(Query q) {
        if (q instanceof ConstantScoreQuery) {
            ConstantScoreQuery csq = (ConstantScoreQuery) q;
            return csq.getQuery();
        } else {
            return q;
        }
    }

    void testExpectedAccelerationQuery(String regex, Query combinedQuery, Query expectedAccelerationQuery) throws ParseException {
        BooleanQuery cq = (BooleanQuery) unwrapAnyConstantScore(combinedQuery);
        assert cq.clauses().size() == 2;
        Query approximationQuery = null;
        boolean verifyQueryFound = false;
        for (BooleanClause booleanClause : cq.clauses()) {
            Query q = booleanClause.getQuery();
            if (q instanceof AutomatonQueryOnBinaryDv) {
                verifyQueryFound = true;
            } else {
                approximationQuery = q;
            }
        }
        assert verifyQueryFound;

        String message = "regex: "+ regex +"\nactual query: " + formatQuery(approximationQuery) +
            "\nexpected query: " + formatQuery(expectedAccelerationQuery) + "\n";
        assertEquals(message, expectedAccelerationQuery, approximationQuery);
    }

    private String getRandomFuzzyPattern(HashSet<String> values, int edits, int prefixLength) {
        assert edits >=0 && edits <=2;
        // Pick one of the indexed document values to focus our queries on.
        String randomValue = values.toArray(new String[0])[randomIntBetween(0, values.size()-1)];

        if (edits == 0) {
            return randomValue;
        }

        if (randomValue.length() > prefixLength) {
            randomValue = randomValue.substring(0,prefixLength) + "C" + randomValue.substring(prefixLength);
            edits--;
        }

        if(edits > 0) {
            randomValue = randomValue + "a";
        }
        return randomValue;
    }

    private TermRangeQuery getRandomRange(HashSet<String> values) {
        // Pick one of the indexed document values to focus our queries on.
        String randomValue = values.toArray(new String[0])[randomIntBetween(0, values.size()-1)];
        StringBuilder upper = new StringBuilder();
        //Pick a part of the string to change
        int substitutionPoint = randomIntBetween(0, randomValue.length()-1);
        int substitutionLength = randomIntBetween(1, Math.min(10, randomValue.length() - substitutionPoint));

        //Add any head to the result, unchanged
        if(substitutionPoint >0) {
            upper.append(randomValue.substring(0,substitutionPoint));
        }

        // Modify the middle...
        String replacementPart = randomValue.substring(substitutionPoint, substitutionPoint+substitutionLength);
        // .-replace all a chars with z
        upper.append(replacementPart.replaceAll("a", "z"));

        //add any remaining tail, unchanged
        if(substitutionPoint + substitutionLength <= randomValue.length()-1) {
            upper.append(randomValue.substring(substitutionPoint + substitutionLength));
        }
        return new TermRangeQuery(WILDCARD_FIELD_NAME, new BytesRef(randomValue), new BytesRef(upper.toString()),
            randomBoolean(), randomBoolean());
    }

    private String getRandomRegexPattern(HashSet<String> values) {
        // Pick one of the indexed document values to focus our queries on.
        String randomValue = values.toArray(new String[0])[randomIntBetween(0, values.size()-1)];
        return convertToRandomRegex(randomValue);
    }

    // Produces a random regex string guaranteed to match the provided value
    protected String convertToRandomRegex(String randomValue) {
        StringBuilder result = new StringBuilder();
        //Pick a part of the string to change
        int substitutionPoint = randomIntBetween(0, randomValue.length()-1);
        int substitutionLength = randomIntBetween(1, Math.min(10, randomValue.length() - substitutionPoint));

        //Add any head to the result, unchanged
        if(substitutionPoint >0) {
            result.append(randomValue.substring(0,substitutionPoint));
        }

        // Modify the middle...
        String replacementPart = randomValue.substring(substitutionPoint, substitutionPoint+substitutionLength);
        int mutation = randomIntBetween(0, 11);
        switch (mutation) {
        case 0:
            // OR with random alpha of same length
            result.append("("+replacementPart+"|c"+ randomABString(replacementPart.length())+")");
            break;
        case 1:
            // OR with non-existant value
            result.append("("+replacementPart+"|doesnotexist)");
            break;
        case 2:
            // OR with another randomised regex (used to create nested levels of expression).
            result.append("(" + convertToRandomRegex(replacementPart) +"|doesnotexist)");
            break;
        case 3:
            // Star-replace all ab sequences.
            result.append(replacementPart.replaceAll("ab", ".*"));
            break;
        case 4:
            // .-replace all b chars
            result.append(replacementPart.replaceAll("b", "."));
            break;
        case 5:
            // length-limited stars {1,2}
            result.append(".{1,"+replacementPart.length()+"}");
            break;
        case 6:
            // replace all chars with .
            result.append(replacementPart.replaceAll(".", "."));
            break;
        case 7:
            // OR with uppercase chars eg [aA] (many of these sorts of expression in the wild..
            char [] chars = replacementPart.toCharArray();
            for (char c : chars) {
                result.append("[" + c + Character.toUpperCase(c) +"]");
            }
            break;
        case 8:
            // NOT a character - replace all b's with "not a"
            result.append(replacementPart.replaceAll("b", "[^a]"));
            break;
        case 9:
            // Make whole part repeatable 1 or more times
            result.append("(" + replacementPart +")+");
            break;
        case 10:
            // Make whole part repeatable 0 or more times
            result.append("(" + replacementPart +")?");
            break;
        case 11:
            // all but ... syntax
            result.append("@&~(doesnotexist.+)");
            break;
        default:
            break;
        }
        //add any remaining tail, unchanged
        if(substitutionPoint + substitutionLength <= randomValue.length()-1) {
            result.append(randomValue.substring(substitutionPoint + substitutionLength));
        }

        //Assert our randomly generated regex actually matches the provided raw input.
        RegExp regex = new RegExp(result.toString());
        Automaton automaton = regex.toAutomaton();
        ByteRunAutomaton bytesMatcher = new ByteRunAutomaton(automaton);
        BytesRef br = new BytesRef(randomValue);
        assertTrue("[" + result.toString() + "]should match [" + randomValue + "]" + substitutionPoint + "-" + substitutionLength + "/"
                + randomValue.length(), bytesMatcher.run(br.bytes, br.offset, br.length));
        return result.toString();
    }

    protected MappedFieldType provideMappedFieldType(String name) {
        if (name.equals(WILDCARD_FIELD_NAME)) {
            return wildcardFieldType.fieldType();
        } else {
            return keywordFieldType.fieldType();
        }
    }

    protected final SearchExecutionContext createMockContext() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 10), "_na_");
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(index,
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(idxSettings, Mockito.mock(BitsetFilterCache.Listener.class));
        TriFunction<MappedFieldType, String, Supplier<SearchLookup>, IndexFieldData<?>> indexFieldDataLookup =
            (fieldType, fieldIndexName, searchLookup) -> {
            IndexFieldData.Builder builder = fieldType.fielddataBuilder(fieldIndexName, searchLookup);
            return builder.build(new IndexFieldDataCache.None(), null);
        };
        return new SearchExecutionContext(0, 0, idxSettings, bitsetFilterCache, indexFieldDataLookup,
                null, null, null, null, xContentRegistry(), null, null, null,
                () -> randomNonNegativeLong(), null, null, () -> true, null, emptyMap()) {
            @Override
            public MappedFieldType getFieldType(String name) {
                return provideMappedFieldType(name);
            }
        };
    }

    private void addFields(ParseContext.Document parseDoc, Document doc, String docContent) throws IOException {
        ArrayList<IndexableField> fields = new ArrayList<>();
        wildcardFieldType.createFields(docContent, parseDoc, fields);

        for (IndexableField indexableField : fields) {
            doc.add(indexableField);
        }
        // Add keyword fields too
        doc.add(new SortedSetDocValuesField(KEYWORD_FIELD_NAME, new BytesRef(docContent)));
        doc.add(new StringField(KEYWORD_FIELD_NAME, docContent, Field.Store.YES));
    }

    private void indexDoc(ParseContext.Document parseDoc, Document doc, RandomIndexWriter iw) throws IOException {
        IndexableField field = parseDoc.getByKey(wildcardFieldType.name());
        if (field != null) {
            doc.add(field);
        }
        iw.addDocument(doc);
    }

    protected IndexSettings createIndexSettings(Version version) {
        return new IndexSettings(
                IndexMetadata.builder("_index").settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version))
                        .numberOfShards(1).numberOfReplicas(0).creationDate(System.currentTimeMillis()).build(),
                Settings.EMPTY);
    }


    static String randomABString(int minLength) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < minLength) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    sb.append("a");
                } else {
                    sb.append("A");
                }
            } else {
                sb.append("b");
            }
        }
        return sb.toString();
    }

    private void randomSyntaxChar(StringBuilder sb) {
        switch (randomInt(3)) {
        case 0:
            sb.append(WildcardQuery.WILDCARD_CHAR);
            break;
        case 1:
            sb.append(WildcardQuery.WILDCARD_STRING);
            break;
        case 2:
            sb.append(WildcardQuery.WILDCARD_ESCAPE);
            sb.append(WildcardQuery.WILDCARD_STRING);
            break;
        case 3:
            sb.append(WildcardQuery.WILDCARD_ESCAPE);
            sb.append(WildcardQuery.WILDCARD_CHAR);
            break;
        }
    }

    private String getRandomWildcardPattern() {
        StringBuilder sb = new StringBuilder();
        int numFragments = 1 + randomInt(4);
        if (randomInt(10) == 1) {
            randomSyntaxChar(sb);
        }
        for (int i = 0; i < numFragments; i++) {
            if (i > 0) {
                randomSyntaxChar(sb);
            }
            sb.append(randomABString(1 + randomInt(6)));
        }
        if (randomInt(10) == 1) {
            randomSyntaxChar(sb);
        }
        return sb.toString();
    }

    @Override
    protected String generateRandomInputValue(MappedFieldType ft) {
        return randomAlphaOfLengthBetween(1, 100);
    }

    @Override
    protected boolean dedupAfterFetch() {
        return true;
    }
}

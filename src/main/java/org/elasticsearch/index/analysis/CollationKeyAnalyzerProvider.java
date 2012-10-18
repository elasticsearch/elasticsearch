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

package org.elasticsearch.index.analysis;

import org.apache.lucene.collation.CollationKeyAnalyzer;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.text.Collator;
import java.text.ParseException;
import java.text.RuleBasedCollator;
import java.util.Locale;

/**
 * <p>
 *   An analyzer provider for filtering with 
 *   {@link org.apache.lucene.collation.CollationKeyFilter}.
 * </p>
 * <p>
 *   <strong>WARNING:</strong> Make sure you use exactly the same Collator at
 *   index and query time -- CollationKeys are only comparable when produced by
 *   the same Collator.  Since {@link java.text.RuleBasedCollator}s are not
 *   independently versioned, it is unsafe to search against stored
 *   CollationKeys unless the following are exactly the same (best practice is
 *   to store this information with the index and check that they remain the
 *   same at query time):
 * </p>
 * <ol>
 *   <li>JVM vendor</li>
 *   <li>JVM version, including patch version</li>
 *   <li>
 *     The language (and country and variant, if specified) of the Locale
 *     used when constructing the collator via
 *     {@link Collator#getInstance(java.util.Locale)}.
 *   </li>
 *   <li>
 *     The collation strength used - see {@link Collator#setStrength(int)}
 *   </li>
 * </ol> 
 * <p>
 *   The <code>ICUCollationKeyAnalyzer</code> in the 
 *   elasticsearch icu plugin uses ICU4J's Collator, which makes its
 *   its version available, thus allowing collation to be versioned
 *   independently from the JVM.  ICUCollationKeyAnalyzer is also significantly
 *   faster and generates significantly shorter keys than CollationKeyAnalyzer.
 *   See <a href="http://site.icu-project.org/charts/collation-icu4j-sun"
 *   >http://site.icu-project.org/charts/collation-icu4j-sun</a> for key
 *   generation timing and key length comparisons between ICU4J and
 *   java.text.Collator over several languages.
 * </p>
 * <p>
 *   CollationKeys generated by java.text.Collators are not compatible
 *   with those those generated by ICU Collators.  Specifically, if you use 
 *   CollationKeyAnalyzer to generate index terms, do not use
 *   ICUCollationKeyAnalyzer on the query side, or vice versa.
 * </p>
 */
public class CollationKeyAnalyzerProvider extends AbstractIndexAnalyzerProvider<CollationKeyAnalyzer> {

    private final CollationKeyAnalyzer analyzer;

    @Inject
    public CollationKeyAnalyzerProvider(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        Locale locale = settings.get("locale") != null ? new Locale(settings.get("locale")) : Locale.getDefault();
        Collator collator = Collator.getInstance(locale);
        int decomp = settings.getAsInt("decomp", collator.getDecomposition());
        collator.setDecomposition(decomp);
        int strength = settings.getAsInt("strength", collator.getStrength());
        collator.setStrength(decomp);
        String rules = settings.get("rules");
        if (rules != null) {
            try {
                collator = new RuleBasedCollator(rules);
            } catch (ParseException e) {
                // ignore
            }
        }
        this.analyzer = new CollationKeyAnalyzer(collator);
    }

    @Override
    public CollationKeyAnalyzer get() {
        return this.analyzer;
    }
}

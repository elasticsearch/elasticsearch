/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.index.similarity;

import org.apache.lucene.search.similarities.AfterEffect;
import org.apache.lucene.search.similarities.AfterEffectB;
import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.BasicModel;
import org.apache.lucene.search.similarities.BasicModelBE;
import org.apache.lucene.search.similarities.BasicModelD;
import org.apache.lucene.search.similarities.BasicModelG;
import org.apache.lucene.search.similarities.BasicModelIF;
import org.apache.lucene.search.similarities.BasicModelIn;
import org.apache.lucene.search.similarities.BasicModelIne;
import org.apache.lucene.search.similarities.BasicModelP;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.Normalization;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * {@link SimilarityProvider} for {@link DFRSimilarity}.
 * <p>
 * Configuration options available:
 * <ul>
 *     <li>basic_model</li>
 *     <li>after_effect</li>
 *     <li>normalization</li>
 * </ul>
 * @see DFRSimilarity For more information about configuration
 */
public class DFRSimilarityProvider extends BaseSimilarityProvider {
    private static final Map<String, BasicModel> BASIC_MODELS;
    private static final Map<String, AfterEffect> AFTER_EFFECTS;

    static {
        Map<String, BasicModel> models = new HashMap<>();
        models.put("be", new BasicModelBE());
        models.put("d", new BasicModelD());
        models.put("g", new BasicModelG());
        models.put("if", new BasicModelIF());
        models.put("in", new BasicModelIn());
        models.put("ine", new BasicModelIne());
        models.put("p", new BasicModelP());
        BASIC_MODELS = unmodifiableMap(models);

        Map<String, AfterEffect> effects = new HashMap<>();
        effects.put("no", new AfterEffect.NoAfterEffect());
        effects.put("b", new AfterEffectB());
        effects.put("l", new AfterEffectL());
        AFTER_EFFECTS = unmodifiableMap(effects);
    }

    private static final Setting<String> AFTER_EFFECT_SETTING =
        Setting.simpleString("after_effect", Setting.Property.Dynamic);
    private static final Setting<String> BASIC_MODEL_SETTING =
        Setting.simpleString("basic_model", Setting.Property.Dynamic);
    private DFRSimilarity similarity;

    public DFRSimilarityProvider(String name, Settings settings) {
        super(name, settings);
        this.similarity = create(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> lst = new ArrayList<>(super.getSettings());
        lst.addAll(Arrays.asList(BASIC_MODEL_SETTING, AFTER_EFFECT_SETTING,
            NORMALIZATION_SETTING, H1_C_SETTING, H2_C_SETTING, H3_C_SETTING, Z_Z_SETTING));
        return lst;
    }

    @Override
    protected void doValidateUpdateSettings(Settings settings) {
        create(settings);
    }

    @Override
    protected void doUpdateSettings(Settings settings) {
        similarity = create(settings);
    }

    @Override
    protected Similarity doGet() {
        return similarity;
    }

    /**
     * Parses the given Settings and creates the appropriate {@link BasicModel}
     *
     * @param settings Settings to parse
     * @return {@link BasicModel} referred to in the Settings
     */
    protected BasicModel parseBasicModel(Settings settings) {
        String basicModel = BASIC_MODEL_SETTING.get(settings);
        BasicModel model = BASIC_MODELS.get(basicModel);
        if (model == null) {
            throw new IllegalArgumentException("Unsupported BasicModel [" + basicModel + "]");
        }
        return model;
    }

    /**
     * Parses the given Settings and creates the appropriate {@link AfterEffect}
     *
     * @param settings Settings to parse
     * @return {@link AfterEffect} referred to in the Settings
     */
    protected AfterEffect parseAfterEffect(Settings settings) {
        String afterEffect = AFTER_EFFECT_SETTING.get(settings);
        AfterEffect effect = AFTER_EFFECTS.get(afterEffect);
        if (effect == null) {
            throw new IllegalArgumentException("Unsupported AfterEffect [" + afterEffect + "]");
        }
        return effect;
    }

    private DFRSimilarity create(Settings settings) {
        BasicModel basicModel = parseBasicModel(settings);
        AfterEffect afterEffect = parseAfterEffect(settings);
        Normalization normalization = parseNormalization(settings);
        DFRSimilarity sim = new DFRSimilarity(basicModel, afterEffect, normalization);
        sim.setDiscountOverlaps(discountOverlaps);
        return sim;
    }
}

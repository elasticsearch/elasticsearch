/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.ml.inference.TrainedModelConfig;
import org.elasticsearch.common.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class GetTrainedModelsRequest implements Validatable {

    public static final String ALLOW_NO_MATCH = "allow_no_match";
    public static final String INCLUDE_MODEL_DEFINITION = "include_model_definition";
    public static final String FOR_EXPORT = "for_export";
    public static final String DECOMPRESS_DEFINITION = "decompress_definition";
    public static final String TAGS = "tags";

    private final List<String> ids;
    private Boolean allowNoMatch;
    private Boolean includeDefinition;
    private Boolean decompressDefinition;
    private Boolean forExport;
    private PageParams pageParams;
    private List<String> tags;

    /**
     * Helper method to create a request that will get ALL TrainedModelConfigs
     * @return new {@link GetTrainedModelsRequest} object for the id "_all"
     */
    public static GetTrainedModelsRequest getAllTrainedModelConfigsRequest() {
        return new GetTrainedModelsRequest("_all");
    }

    public GetTrainedModelsRequest(String... ids) {
        this.ids = Arrays.asList(ids);
    }

    public List<String> getIds() {
        return ids;
    }

    public Boolean getAllowNoMatch() {
        return allowNoMatch;
    }

    /**
     * Whether to ignore if a wildcard expression matches no trained models.
     *
     * @param allowNoMatch If this is {@code false}, then an error is returned when a wildcard (or {@code _all})
     *                    does not match any trained models
     */
    public GetTrainedModelsRequest setAllowNoMatch(boolean allowNoMatch) {
        this.allowNoMatch = allowNoMatch;
        return this;
    }

    public PageParams getPageParams() {
        return pageParams;
    }

    public GetTrainedModelsRequest setPageParams(@Nullable PageParams pageParams) {
        this.pageParams = pageParams;
        return this;
    }

    public Boolean getIncludeDefinition() {
        return includeDefinition;
    }

    /**
     * Whether to include the full model definition.
     *
     * The full model definition can be very large.
     *
     * @param includeDefinition If {@code true}, the definition is included.
     */
    public GetTrainedModelsRequest setIncludeDefinition(Boolean includeDefinition) {
        this.includeDefinition = includeDefinition;
        return this;
    }

    public Boolean getDecompressDefinition() {
        return decompressDefinition;
    }

    /**
     * Whether or not to decompress the trained model, or keep it in its compressed string form
     *
     * @param decompressDefinition If {@code true}, the definition is decompressed.
     */
    public GetTrainedModelsRequest setDecompressDefinition(Boolean decompressDefinition) {
        this.decompressDefinition = decompressDefinition;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    /**
     * The tags that the trained model must match. These correspond to {@link TrainedModelConfig#getTags()}.
     *
     * The models returned will match ALL tags supplied.
     * If none are provided, only the provided ids are used to find models
     * @param tags The tags to match when finding models
     */
    public GetTrainedModelsRequest setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    /**
     * See {@link GetTrainedModelsRequest#setTags(List)}
     */
    public GetTrainedModelsRequest setTags(String... tags) {
        return setTags(Arrays.asList(tags));
    }

    public Boolean getForExport() {
        return forExport;
    }

    /**
     * Setting this flag to `true` removes certain fields from the model definition on retrieval.
     *
     * This is useful when getting the model and wanting to put it in another cluster.
     *
     * Default value is false.
     * @param forExport Boolean value indicating if certain fields should be removed from the mode on GET
     */
    public GetTrainedModelsRequest setForExport(Boolean forExport) {
        this.forExport = forExport;
        return this;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (ids == null || ids.isEmpty()) {
            return Optional.of(ValidationException.withError("trained model id must not be null"));
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GetTrainedModelsRequest other = (GetTrainedModelsRequest) o;
        return Objects.equals(ids, other.ids)
            && Objects.equals(allowNoMatch, other.allowNoMatch)
            && Objects.equals(decompressDefinition, other.decompressDefinition)
            && Objects.equals(includeDefinition, other.includeDefinition)
            && Objects.equals(forExport, other.forExport)
            && Objects.equals(pageParams, other.pageParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ids, allowNoMatch, pageParams, decompressDefinition, includeDefinition, forExport);
    }
}

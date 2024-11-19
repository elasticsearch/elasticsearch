/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialStExtentCartesianPointDocValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialStExtentCartesianPointSourceValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialStExtentGeoPointDocValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialStExtentGeoPointSourceValuesAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatialPoint;

/**
 * Calculate spatial centroid of all geo_point or cartesian point values of a field in matching documents.
 */
public final class SpatialStExtent extends SpatialAggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "SpatialStExtent",
        SpatialStExtent::new
    );

    @FunctionInfo(
        returnType = { "geo_point", "cartesian_point" },
        description = "Calculate the extent centroid over a field with spatial point geometry type.",
        isAggregation = true
//        ,
//        examples = @Example(file = "spatial", tag = "st_centroid_agg-airports")
    )
    public SpatialStExtent(Source source, @Param(name = "field", type = { "geo_point", "cartesian_point" }) Expression field) {
        this(source, field, Literal.TRUE, false);
    }

    private SpatialStExtent(Source source, Expression field, Expression filter, boolean useDocValues) {
        super(source, field, filter, useDocValues);
    }

    private SpatialStExtent(StreamInput in) throws IOException {
        super(in, false);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public SpatialStExtent withFilter(Expression filter) {
        return new SpatialStExtent(source(), field(), filter, useDocValues);
    }

    @Override
    public SpatialStExtent withDocValues() {
        return new SpatialStExtent(source(), field(), filter(), true);
    }

    @Override
    protected TypeResolution resolveType() {
        // TODO: Support geo_shape and cartesian_shape
        return isSpatialPoint(field(), sourceText(), DEFAULT);
    }

    @Override
    public DataType dataType() {
        // We aggregate incoming GEO_POINTs into a single GEO_POINT, or incoming CARTESIAN_POINTs into a single CARTESIAN_POINT.
        return field().dataType();
    }

    @Override
    protected NodeInfo<SpatialStExtent> info() {
        return NodeInfo.create(this, SpatialStExtent::new, field());
    }

    @Override
    public SpatialStExtent replaceChildren(List<Expression> newChildren) {
        return new SpatialStExtent(source(), newChildren.get(0));
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (useDocValues) {
            // When the points are read as doc-values (eg. from the index), feed them into the doc-values aggregator
            if (type == DataType.GEO_POINT) {
                return new SpatialStExtentGeoPointDocValuesAggregatorFunctionSupplier(inputChannels);
            }
            if (type == DataType.CARTESIAN_POINT) {
                return new SpatialStExtentCartesianPointDocValuesAggregatorFunctionSupplier(inputChannels);
            }
        } else {
            // When the points are read as WKB from source or as point literals, feed them into the source-values aggregator
            if (type == DataType.GEO_POINT) {
                return new SpatialStExtentGeoPointSourceValuesAggregatorFunctionSupplier(inputChannels);
            }
            if (type == DataType.CARTESIAN_POINT) {
                return new SpatialStExtentCartesianPointSourceValuesAggregatorFunctionSupplier(inputChannels);
            }
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }
}

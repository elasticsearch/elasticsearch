/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.SpatialEnvelopeVisitor;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

@FunctionName("st_ymax")
public class StYMaxTests extends AbstractScalarFunctionTestCase {
    public StYMaxTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String expectedEvaluator = "StYMaxFromWKBEvaluator[field=Attribute[channel=0]]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryGeoPoint(suppliers, expectedEvaluator, DOUBLE, StYMaxTests::valueOf, List.of());
        TestCaseSupplier.forUnaryCartesianPoint(suppliers, expectedEvaluator, DOUBLE, StYMaxTests::valueOf, List.of());
        TestCaseSupplier.forUnaryGeoShape(suppliers, expectedEvaluator, DOUBLE, StYMaxTests::valueOf, List.of());
        TestCaseSupplier.forUnaryCartesianShape(suppliers, expectedEvaluator, DOUBLE, StYMaxTests::valueOf, List.of());
        return parameterSuppliersFromTypedDataWithDefaultChecks(
            true,
            suppliers,
            (v, p) -> "geo_point, cartesian_point, geo_shape or cartesian_shape"
        );
    }

    private static double valueOf(BytesRef wkb) {
        var geometry = UNSPECIFIED.wkbToGeometry(wkb);
        if (geometry instanceof Point point) {
            return point.getY();
        }
        var envelope = new SpatialEnvelopeVisitor();
        if (geometry.visit(envelope)) {
            return envelope.getResult().getMaxY();
        }
        throw new IllegalArgumentException("Geometry is empty");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StYMax(source, args.get(0));
    }
}

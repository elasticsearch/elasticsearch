/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexSerializationTests;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.ByteSizeEqualsMatcher.byteSizeEquals;
import static org.hamcrest.Matchers.equalTo;

public class ExchangeSinkExecSerializationTests extends AbstractPhysicalPlanSerializationTests<ExchangeSinkExec> {
    static ExchangeSinkExec randomExchangeSinkExec(int depth) {
        Source source = randomSource();
        List<Attribute> output = randomFieldAttributes(1, 5, false);
        boolean intermediateAgg = randomBoolean();
        PhysicalPlan child = randomChild(depth);
        return new ExchangeSinkExec(source, output, intermediateAgg, child);
    }

    @Override
    protected ExchangeSinkExec createTestInstance() {
        return randomExchangeSinkExec(0);
    }

    @Override
    protected ExchangeSinkExec mutateInstance(ExchangeSinkExec instance) throws IOException {
        List<Attribute> output = instance.output();
        boolean intermediateAgg = instance.isIntermediateAgg();
        PhysicalPlan child = instance.child();
        switch (between(0, 2)) {
            case 0 -> output = randomValueOtherThan(output, () -> randomFieldAttributes(1, 5, false));
            case 1 -> intermediateAgg = false == intermediateAgg;
            case 2 -> child = randomValueOtherThan(child, () -> randomChild(0));
        }
        return new ExchangeSinkExec(instance.source(), output, intermediateAgg, child);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    /**
     * Test the size of serializing a plan with many conflicts.
     * See {@link #testManyTypeConflicts(boolean, ByteSizeValue)} for more.
     */
    public void testManyTypeConflicts() throws IOException {
        testManyTypeConflicts(false, ByteSizeValue.ofBytes(1424048));
        /*
         * History:
         *  2.3mb - shorten error messages for UnsupportedAttributes #111973
         *  1.8mb - cache EsFields #112008
         *  1.4mb - string serialization #112929
         */
    }

    /**
     * Test the size of serializing a plan with many conflicts.
     * See {@link #testManyTypeConflicts(boolean, ByteSizeValue)} for more.
     */
    public void testManyTypeConflictsWithParent() throws IOException {
        testManyTypeConflicts(true, ByteSizeValue.ofBytes(2774192));
        /*
         * History:
         *  2 gb+ - start
         * 43.3mb - Cache attribute subclasses #111447
         *  5.6mb - shorten error messages for UnsupportedAttributes #111973
         *  3.1mb - cache EsFields #112008
         *  2774214b - string serialization #112929
         *  2774192b - remove field attribute #112881
         */
    }

    private void testManyTypeConflicts(boolean withParent, ByteSizeValue expected) throws IOException {
        EsIndex index = EsIndexSerializationTests.indexWithManyConflicts(withParent);
        testSerializePlanWithIndex(index, expected);
    }

    /**
     * Test the size of serializing a plan like
     * FROM index | LIMIT 10
     * with a single root field that has many children, grandchildren etc.
     */
    public void testDeeplyNestedFields() throws IOException {
        ByteSizeValue expected = ByteSizeValue.ofBytes(47252411);
        /*
         * History:
         *  48223371b - string serialization #112929
         *  47252411b - remove field attribute #112881
         */

        int depth = 6;
        int childrenPerLevel = 8;

        EsIndex index = EsIndexSerializationTests.deeplyNestedIndex(depth, childrenPerLevel);
        testSerializePlanWithIndex(index, expected);
    }

    /**
     * Test the size of serializing a plan like
     * FROM index | LIMIT 10 | KEEP one_single_field
     * with a single root field that has many children, grandchildren etc.
     */
    public void testDeeplyNestedFieldsKeepOnlyOne() throws IOException {
        ByteSizeValue expected = ByteSizeValue.ofBytes(9425806);
        /*
         * History:
         *  9426058b - string serialization #112929
         *  9425806b - remove field attribute #112881
         */

        int depth = 6;
        int childrenPerLevel = 9;

        EsIndex index = EsIndexSerializationTests.deeplyNestedIndex(depth, childrenPerLevel);
        testSerializePlanWithIndex(index, expected, false);
    }

    /**
     * Test the size of serializing the physical plan that will be sent to a data node.
     * The plan corresponds to `FROM index | LIMIT 10`.
     * Callers of this method intentionally use a very precise size for the serialized
     * data so a programmer making changes has to think when this size changes.
     * <p>
     *     In general, shrinking the over the wire size is great and the precise
     *     size should just ratchet downwards. Small upwards movement is fine so
     *     long as you understand why the change is happening and you think it's
     *     worth it for the data node request for a big index to grow.
     * </p>
     * <p>
     *     Large upwards movement in the size is not fine! Folks frequently make
     *     requests across large clusters with many fields and these requests can
     *     really clog up the network interface. Super large results here can make
     *     ESQL impossible to use at all for big mappings with many conflicts.
     * </p>
     */
    private void testSerializePlanWithIndex(EsIndex index, ByteSizeValue expected) throws IOException {
        testSerializePlanWithIndex(index, expected, true);
    }

    private void testSerializePlanWithIndex(EsIndex index, ByteSizeValue expected, boolean keepAllFields) throws IOException {
        List<Attribute> allAttributes = Analyzer.mappingAsAttributes(randomSource(), index.mapping());
        List<Attribute> keepAttributes = keepAllFields ? allAttributes : List.of(allAttributes.get(0));
        EsRelation relation = new EsRelation(randomSource(), index, keepAttributes, IndexMode.STANDARD);
        Limit limit = new Limit(randomSource(), new Literal(randomSource(), 10, DataType.INTEGER), relation);
        Project project = new Project(randomSource(), limit, limit.output());
        FragmentExec fragmentExec = new FragmentExec(project);
        ExchangeSinkExec exchangeSinkExec = new ExchangeSinkExec(randomSource(), fragmentExec.output(), false, fragmentExec);
        try (BytesStreamOutput out = new BytesStreamOutput(); PlanStreamOutput pso = new PlanStreamOutput(out, configuration())) {
            pso.writeNamedWriteable(exchangeSinkExec);
            assertThat(ByteSizeValue.ofBytes(out.bytes().length()), byteSizeEquals(expected));
            try (PlanStreamInput psi = new PlanStreamInput(out.bytes().streamInput(), getNamedWriteableRegistry(), configuration())) {
                assertThat(psi.readNamedWriteable(PhysicalPlan.class), equalTo(exchangeSinkExec));
            }
        }
    }
}

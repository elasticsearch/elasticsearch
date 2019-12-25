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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Set;

/**
 * Represents a return statement.
 */
public final class SReturn extends AStatement {

    private AExpression expression;

    public SReturn(Location location, AExpression expression) {
        super(location);

        this.expression = expression;
    }

    @Override
    void extractVariables(Set<String> variables) {
        if (expression != null) {
            expression.extractVariables(variables);
        }
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        if (expression == null) {
            if (locals.getReturnType() != void.class) {
                throw location.createError(new ClassCastException("Cannot cast from " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(locals.getReturnType()) + "] to " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(void.class) + "]."));
            }
        } else {
            expression.expected = locals.getReturnType();
            expression.internal = true;
            expression.analyze(scriptRoot, locals);
            expression = expression.cast(scriptRoot, locals);
        }

        methodEscape = true;
        loopEscape = true;
        allEscape = true;

        statementCount = 1;
    }

    @Override
    ReturnNode write() {
        return new ReturnNode()
                .setExpressionNode(expression == null ? null : expression.write())
                .setLocation(location);
    }

    @Override
    public String toString() {
        return expression == null ? singleLineToString() : singleLineToString(expression);
    }
}

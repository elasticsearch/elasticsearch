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

package org.elasticsearch.painless.lookup;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;

public class PainlessBinding {

    public final Constructor<?> javaConstructor;
    public final List<Class<?>> constructorTypeParameters;

    public final Method javaMethod;
    public final Class<?> returnType;
    public final List<Class<?>> methodTypeParameters;

    PainlessBinding(Constructor<?> javaConstructor, List<Class<?>> constructorTypeParameters,
            Method javaMethod, Class<?> returnType, List<Class<?>> methodTypeParameters) {
        this.javaConstructor = javaConstructor;
        this.constructorTypeParameters = constructorTypeParameters;

        this.javaMethod = javaMethod;
        this.returnType = returnType;
        this.methodTypeParameters = methodTypeParameters;
    }
}

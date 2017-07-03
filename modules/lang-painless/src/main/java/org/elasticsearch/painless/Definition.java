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

package org.elasticsearch.painless;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SetOnce;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Spliterator;

/**
 * The entire API for Painless.  Also used as a whitelist for checking for legal
 * methods and fields during at both compile-time and runtime.
 */
public final class Definition {

    public static class StructWhitelist {
        public final String alias;
        public final Class<?> clazz;
        public final List<String> supers;
        public final List<WhitelistedObject> whitelist;

        public StructWhitelist(String alias, Class<?> clazz, List<String> supers, List<WhitelistedObject> whitelist) {
            this.alias = Objects.requireNonNull(alias);
            this.clazz = Objects.requireNonNull(clazz);
            this.supers = Collections.unmodifiableList(Objects.requireNonNull(supers));
            this.whitelist = Collections.unmodifiableList(Objects.requireNonNull(whitelist));
        }
    }

    public static class WhitelistedObject {
        public final String alias;
        public final boolean augmented;
        public final AccessibleObject object;

        public WhitelistedObject(String alias, boolean augmented, AccessibleObject object) {
            this.alias = alias;
            this.augmented = augmented;
            this.object = object;
        }
    }

    private static final List<String> DEFINITION_FILES = Collections.unmodifiableList(
        Arrays.asList("org.elasticsearch.txt",
                      "java.lang.txt",
                      "java.math.txt",
                      "java.text.txt",
                      "java.time.txt",
                      "java.time.chrono.txt",
                      "java.time.format.txt",
                      "java.time.temporal.txt",
                      "java.time.zone.txt",
                      "java.util.txt",
                      "java.util.function.txt",
                      "java.util.regex.txt",
                      "java.util.stream.txt",
                      "joda.time.txt"));

    /**
     * Whitelist that is "built in" to Painless and required by all scripts.
     */
    public static final Definition BUILTINS = buildDefinition();

    /** Some native types as constants: */
    public static final Type VOID_TYPE = BUILTINS.getType("void");
    public static final Type BOOLEAN_TYPE = BUILTINS.getType("boolean");
    public static final Type BOOLEAN_OBJ_TYPE = BUILTINS.getType("Boolean");
    public static final Type BYTE_TYPE = BUILTINS.getType("byte");
    public static final Type BYTE_OBJ_TYPE = BUILTINS.getType("Byte");
    public static final Type SHORT_TYPE = BUILTINS.getType("short");
    public static final Type SHORT_OBJ_TYPE = BUILTINS.getType("Short");
    public static final Type INT_TYPE = BUILTINS.getType("int");
    public static final Type INT_OBJ_TYPE = BUILTINS.getType("Integer");
    public static final Type LONG_TYPE = BUILTINS.getType("long");
    public static final Type LONG_OBJ_TYPE = BUILTINS.getType("Long");
    public static final Type FLOAT_TYPE = BUILTINS.getType("float");
    public static final Type FLOAT_OBJ_TYPE = BUILTINS.getType("Float");
    public static final Type DOUBLE_TYPE = BUILTINS.getType("double");
    public static final Type DOUBLE_OBJ_TYPE = BUILTINS.getType("Double");
    public static final Type CHAR_TYPE = BUILTINS.getType("char");
    public static final Type CHAR_OBJ_TYPE = BUILTINS.getType("Character");
    public static final Type OBJECT_TYPE = BUILTINS.getType("Object");
    public static final Type DEF_TYPE = BUILTINS.getType("def");
    public static final Type NUMBER_TYPE = BUILTINS.getType("Number");
    public static final Type STRING_TYPE = BUILTINS.getType("String");
    public static final Type EXCEPTION_TYPE = BUILTINS.getType("Exception");
    public static final Type PATTERN_TYPE = BUILTINS.getType("Pattern");
    public static final Type MATCHER_TYPE = BUILTINS.getType("Matcher");
    public static final Type ITERATOR_TYPE = BUILTINS.getType("Iterator");
    public static final Type ARRAY_LIST_TYPE = BUILTINS.getType("ArrayList");
    public static final Type HASH_MAP_TYPE = BUILTINS.getType("HashMap");

    public enum Sort {
        VOID(       void.class      , Void.class      , null          , 0 , true  , false , false , false ),
        BOOL(       boolean.class   , Boolean.class   , null          , 1 , true  , true  , false , true  ),
        BYTE(       byte.class      , Byte.class      , null          , 1 , true  , false , true  , true  ),
        SHORT(      short.class     , Short.class     , null          , 1 , true  , false , true  , true  ),
        CHAR(       char.class      , Character.class , null          , 1 , true  , false , true  , true  ),
        INT(        int.class       , Integer.class   , null          , 1 , true  , false , true  , true  ),
        LONG(       long.class      , Long.class      , null          , 2 , true  , false , true  , true  ),
        FLOAT(      float.class     , Float.class     , null          , 1 , true  , false , true  , true  ),
        DOUBLE(     double.class    , Double.class    , null          , 2 , true  , false , true  , true  ),

        VOID_OBJ(   Void.class      , null            , void.class    , 1 , true  , false , false , false ),
        BOOL_OBJ(   Boolean.class   , null            , boolean.class , 1 , false , true  , false , false ),
        BYTE_OBJ(   Byte.class      , null            , byte.class    , 1 , false , false , true  , false ),
        SHORT_OBJ(  Short.class     , null            , short.class   , 1 , false , false , true  , false ),
        CHAR_OBJ(   Character.class , null            , char.class    , 1 , false , false , true  , false ),
        INT_OBJ(    Integer.class   , null            , int.class     , 1 , false , false , true  , false ),
        LONG_OBJ(   Long.class      , null            , long.class    , 1 , false , false , true  , false ),
        FLOAT_OBJ(  Float.class     , null            , float.class   , 1 , false , false , true  , false ),
        DOUBLE_OBJ( Double.class    , null            , double.class  , 1 , false , false , true  , false ),

        NUMBER(     Number.class    , null            , null          , 1 , false , false , false , false ),
        STRING(     String.class    , null            , null          , 1 , false , false , false , true  ),

        OBJECT(     null            , null            , null          , 1 , false , false , false , false ),
        DEF(        null            , null            , null          , 1 , false , false , false , false ),
        ARRAY(      null            , null            , null          , 1 , false , false , false , false );

        public final Class<?> clazz;
        public final Class<?> boxed;
        public final Class<?> unboxed;
        public final int size;
        public final boolean primitive;
        public final boolean bool;
        public final boolean numeric;
        public final boolean constant;

        Sort(final Class<?> clazz, final Class<?> boxed, final Class<?> unboxed, final int size,
             final boolean primitive, final boolean bool, final boolean numeric, final boolean constant) {
            this.clazz = clazz;
            this.boxed = boxed;
            this.unboxed = unboxed;
            this.size = size;
            this.bool = bool;
            this.primitive = primitive;
            this.numeric = numeric;
            this.constant = constant;
        }
    }

    public static final class Type {
        public final String name;
        public final int dimensions;
        public final Struct struct;
        public final Class<?> clazz;
        public final org.objectweb.asm.Type type;
        public final Sort sort;

        private Type(final String name, final int dimensions, final Struct struct,
                     final Class<?> clazz, final org.objectweb.asm.Type type, final Sort sort) {
            this.name = name;
            this.dimensions = dimensions;
            this.struct = struct;
            this.clazz = clazz;
            this.type = type;
            this.sort = sort;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }

            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            final Type type = (Type)object;

            return this.type.equals(type.type) && struct.equals(type.struct);
        }

        @Override
        public int hashCode() {
            int result = struct.hashCode();
            result = 31 * result + type.hashCode();

            return result;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public static class Method {
        public final String name;
        public final Struct owner;
        public final Class<?> augmentation;
        public final Type rtn;
        public final List<Type> arguments;
        public final org.objectweb.asm.commons.Method method;
        public final int modifiers;
        public final MethodHandle handle;

        public Method(String name, Struct owner, Class<?> augmentation, Type rtn, List<Type> arguments,
                      org.objectweb.asm.commons.Method method, int modifiers, MethodHandle handle) {
            this.name = name;
            this.augmentation = augmentation;
            this.owner = owner;
            this.rtn = rtn;
            this.arguments = Collections.unmodifiableList(arguments);
            this.method = method;
            this.modifiers = modifiers;
            this.handle = handle;
        }

        /**
         * Returns MethodType for this method.
         * <p>
         * This works even for user-defined Methods (where the MethodHandle is null).
         */
        public MethodType getMethodType() {
            // we have a methodhandle already (e.g. whitelisted class)
            // just return its type
            if (handle != null) {
                return handle.type();
            }
            // otherwise compute it
            final Class<?> params[];
            final Class<?> returnValue;
            if (augmentation != null) {
                // static method disguised as virtual/interface method
                params = new Class<?>[1 + arguments.size()];
                params[0] = augmentation;
                for (int i = 0; i < arguments.size(); i++) {
                    params[i + 1] = arguments.get(i).clazz;
                }
                returnValue = rtn.clazz;
            } else if (Modifier.isStatic(modifiers)) {
                // static method: straightforward copy
                params = new Class<?>[arguments.size()];
                for (int i = 0; i < arguments.size(); i++) {
                    params[i] = arguments.get(i).clazz;
                }
                returnValue = rtn.clazz;
            } else if ("<init>".equals(name)) {
                // constructor: returns the owner class
                params = new Class<?>[arguments.size()];
                for (int i = 0; i < arguments.size(); i++) {
                    params[i] = arguments.get(i).clazz;
                }
                returnValue = owner.clazz;
            } else {
                // virtual/interface method: add receiver class
                params = new Class<?>[1 + arguments.size()];
                params[0] = owner.clazz;
                for (int i = 0; i < arguments.size(); i++) {
                    params[i + 1] = arguments.get(i).clazz;
                }
                returnValue = rtn.clazz;
            }
            return MethodType.methodType(returnValue, params);
        }

        public void write(MethodWriter writer) {
            final org.objectweb.asm.Type type;
            if (augmentation != null) {
                assert java.lang.reflect.Modifier.isStatic(modifiers);
                type = org.objectweb.asm.Type.getType(augmentation);
            } else {
                type = owner.type;
            }

            if (java.lang.reflect.Modifier.isStatic(modifiers)) {
                writer.invokeStatic(type, method);
            } else if (java.lang.reflect.Modifier.isInterface(owner.clazz.getModifiers())) {
                writer.invokeInterface(type, method);
            } else {
                writer.invokeVirtual(type, method);
            }
        }
    }

    public static final class Field {
        public final String name;
        public final Struct owner;
        public final Type type;
        public final String javaName;
        public final int modifiers;
        private final MethodHandle getter;
        private final MethodHandle setter;

        private Field(String name, String javaName, Struct owner, Type type, int modifiers, MethodHandle getter, MethodHandle setter) {
            this.name = name;
            this.javaName = javaName;
            this.owner = owner;
            this.type = type;
            this.modifiers = modifiers;
            this.getter = getter;
            this.setter = setter;
        }
    }

    // TODO: instead of hashing on this, we could have a 'next' pointer in Method itself, but it would make code more complex
    // please do *NOT* under any circumstances change this to be the crappy Tuple from elasticsearch!
    /**
     * Key for looking up a method.
     * <p>
     * Methods are keyed on both name and arity, and can be overloaded once per arity.
     * This allows signatures such as {@code String.indexOf(String) vs String.indexOf(String, int)}.
     * <p>
     * It is less flexible than full signature overloading where types can differ too, but
     * better than just the name, and overloading types adds complexity to users, too.
     */
    public static final class MethodKey {
        public final String name;
        public final int arity;

        /**
         * Create a new lookup key
         * @param name name of the method
         * @param arity number of parameters
         */
        public MethodKey(String name, int arity) {
            this.name = Objects.requireNonNull(name);
            this.arity = arity;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + arity;
            result = prime * result + name.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            MethodKey other = (MethodKey) obj;
            if (arity != other.arity) return false;
            if (!name.equals(other.name)) return false;
            return true;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(name);
            sb.append('/');
            sb.append(arity);
            return sb.toString();
        }
    }

    public static final class Struct {
        public final String name;
        public final Class<?> clazz;
        public final org.objectweb.asm.Type type;

        public final Map<MethodKey, Method> constructors;
        public final Map<MethodKey, Method> staticMethods;
        public final Map<MethodKey, Method> methods;

        public final Map<String, Field> staticMembers;
        public final Map<String, Field> members;

        private final SetOnce<Method> functionalMethod;

        private Struct(final String name, final Class<?> clazz, final org.objectweb.asm.Type type) {
            this.name = name;
            this.clazz = clazz;
            this.type = type;

            constructors = new HashMap<>();
            staticMethods = new HashMap<>();
            methods = new HashMap<>();

            staticMembers = new HashMap<>();
            members = new HashMap<>();

            functionalMethod = new SetOnce<>();
        }

        private Struct(final Struct struct) {
            name = struct.name;
            clazz = struct.clazz;
            type = struct.type;

            constructors = Collections.unmodifiableMap(struct.constructors);
            staticMethods = Collections.unmodifiableMap(struct.staticMethods);
            methods = Collections.unmodifiableMap(struct.methods);

            staticMembers = Collections.unmodifiableMap(struct.staticMembers);
            members = Collections.unmodifiableMap(struct.members);

            functionalMethod = struct.functionalMethod;
        }

        private Struct freeze() {
            return new Struct(this);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }

            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            Struct struct = (Struct)object;

            return name.equals(struct.name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        /**
         * If this class is a functional interface according to JLS, returns its method.
         * Otherwise returns null.
         */
        public Method getFunctionalMethod() {
            return functionalMethod.get();
        }
    }

    public static class Cast {
        public final Type from;
        public final Type to;
        public final boolean explicit;
        public final Type unboxFrom;
        public final Type unboxTo;
        public final Type boxFrom;
        public final Type boxTo;

        public Cast(final Type from, final Type to, final boolean explicit) {
            this.from = from;
            this.to = to;
            this.explicit = explicit;
            this.unboxFrom = null;
            this.unboxTo = null;
            this.boxFrom = null;
            this.boxTo = null;
        }

        public Cast(final Type from, final Type to, final boolean explicit,
                    final Type unboxFrom, final Type unboxTo, final Type boxFrom, final Type boxTo) {
            this.from = from;
            this.to = to;
            this.explicit = explicit;
            this.unboxFrom = unboxFrom;
            this.unboxTo = unboxTo;
            this.boxFrom = boxFrom;
            this.boxTo = boxTo;
        }

    }

    public static final class RuntimeClass {
        private final Struct struct;
        public final Map<MethodKey, Method> methods;
        public final Map<String, MethodHandle> getters;
        public final Map<String, MethodHandle> setters;

        private RuntimeClass(final Struct struct, final Map<MethodKey, Method> methods,
                             final Map<String, MethodHandle> getters, final Map<String, MethodHandle> setters) {
            this.struct = struct;
            this.methods = Collections.unmodifiableMap(methods);
            this.getters = Collections.unmodifiableMap(getters);
            this.setters = Collections.unmodifiableMap(setters);
        }

        public Struct getStruct() {
            return struct;
        }
    }

    private static Definition buildDefinition() {
        Map<String, Class<?>> aliasesToClasses = new HashMap<>();
        Map<String, List<String>> aliasesToSupers = new HashMap<>();

        for (String file : DEFINITION_FILES) {
            int currentLine = -1;

            try {
                try (InputStream stream = Definition.class.getResourceAsStream(file);
                     LineNumberReader reader = new LineNumberReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                    String line;

                    while ((line = reader.readLine()) != null) {
                        currentLine = reader.getLineNumber();
                        line = line.trim();
                        if (line.length() == 0 || line.charAt(0) == '#') {
                            continue;
                        }

                        if (line.startsWith("class ")) {
                            String elements[] = line.split("\u0020");
                            assert elements[2].equals("->") : "Invalid struct definition [" + String.join(" ", elements) +"]";
                            String alias = elements[1];

                            if (elements.length == 7) {
                                aliasesToSupers.put(alias, Arrays.asList(elements[5].split(",")));
                            } else {
                                assert elements.length == 5 : "Invalid struct definition [" + String.join(" ", elements) + "]";
                                aliasesToSupers.put(alias, Collections.emptyList());
                            }

                            String peer = elements[3];
                            Class<?> clazz;

                            switch (peer) {
                                case "void":
                                    clazz = void.class;
                                    break;
                                case "boolean":
                                    clazz = boolean.class;
                                    break;
                                case "byte":
                                    clazz = byte.class;
                                    break;
                                case "short":
                                    clazz = short.class;
                                    break;
                                case "char":
                                    clazz = char.class;
                                    break;
                                case "int":
                                    clazz = int.class;
                                    break;
                                case "long":
                                    clazz = long.class;
                                    break;
                                case "float":
                                    clazz = float.class;
                                    break;
                                case "double":
                                    clazz = double.class;
                                    break;
                                default:
                                    clazz = Class.forName(peer);
                                    break;
                            }

                            aliasesToClasses.put(alias, clazz);
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("error in " + file + ", line: " + currentLine, e);
            }
        }

        List<StructWhitelist> structWhitelists = new ArrayList<>();

        for (String file : DEFINITION_FILES) {
            int currentLine = -1;

            try {
                try (InputStream stream = Definition.class.getResourceAsStream(file);
                     LineNumberReader reader = new LineNumberReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                    String line;
                    String className = null;
                    Class<?> currentClass = null;
                    List<WhitelistedObject> objects = null;

                    while ((line = reader.readLine()) != null) {
                        currentLine = reader.getLineNumber();
                        line = line.trim();

                        if (line.length() == 0 || line.charAt(0) == '#') {
                            continue;
                        } else if (line.startsWith("class ")) {
                            assert className == null;
                            assert currentClass == null;
                            assert objects == null;
                            className = line.split("\u0020")[1];
                            currentClass = aliasesToClasses.get(className);
                            objects = new ArrayList<>();
                        } else if (line.equals("}")) {
                            assert currentClass != null;

                            structWhitelists.add(new StructWhitelist(className, currentClass, aliasesToSupers.get(className), objects));

                            className = null;
                            currentClass = null;
                            objects = null;
                        } else {
                            assert className != null;
                            assert currentClass != null;
                            assert objects != null;

                            String elements[] = line.split("\u0020");
                            if (elements.length != 2) {
                                throw new IllegalArgumentException("Malformed signature: " + line);
                            }

                            Class<?> rtn = getClassInternal(aliasesToClasses, elements[0]);
                            int parenIndex = elements[1].indexOf('(');
                            if (parenIndex != -1) {
                                int parenEnd = elements[1].indexOf(')');
                                Class<?> args[];
                                if (parenEnd > parenIndex + 1) {
                                    String arguments[] = elements[1].substring(parenIndex + 1, parenEnd).split(",");
                                    args = new Class<?>[arguments.length];
                                    for (int i = 0; i < arguments.length; i++) {
                                        args[i] = getClassInternal(aliasesToClasses, arguments[i]);
                                    }
                                } else {
                                    args = new Class<?>[0];
                                }
                                String methodName = elements[1].substring(0, parenIndex);
                                if (methodName.equals("<init>")) {
                                    if (currentClass.equals(rtn) == false) {
                                        throw new IllegalArgumentException("Constructors must return their own type");
                                    }

                                    Constructor<?> constructor = currentClass.getConstructor(args);
                                    objects.add(new WhitelistedObject("<init>", false, constructor));
                                } else {
                                    int index = methodName.lastIndexOf(".");

                                    if (index >= 0) {
                                        String augmentation = methodName.substring(0, index);
                                        methodName = methodName.substring(index + 1);
                                        Class<?> augmentClass = Class.forName(augmentation);
                                        Class<?>[] augmentArgs = new Class<?>[args.length + 1];
                                        augmentArgs[0] = currentClass;
                                        System.arraycopy(args, 0, augmentArgs, 1, args.length);
                                        java.lang.reflect.Method method = augmentClass.getMethod(methodName, augmentArgs);
                                        objects.add(new WhitelistedObject(methodName, true, method));
                                    } else {
                                        java.lang.reflect.Method method = currentClass.getMethod(methodName, args);
                                        objects.add(new WhitelistedObject(methodName, false, method));
                                    }
                                }
                            } else {
                                // field
                                java.lang.reflect.Field field = currentClass.getField(elements[1]);
                                objects.add(new WhitelistedObject(elements[1], false, field));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("syntax error in " + file + ", line: " + currentLine, e);
            }
        }

        return new Definition(structWhitelists);
    }

    /** Returns whether or not a non-array type exists. */
    public boolean isSimpleType(final String name) {
        return BUILTINS.structsMap.containsKey(name);
    }

    /** Gets the type given by its name */
    public Type getType(final String name) {
        return BUILTINS.getTypeInternal(name);
    }

    /** Creates an array type from the given Struct. */
    public Type getType(final Struct struct, final int dimensions) {
        return Definition.getTypeInternal(struct, dimensions);
    }

    public RuntimeClass getRuntimeClass(Class<?> clazz) {
        return BUILTINS.runtimeMap.get(clazz);
    }

    /** Collection of all simple types. Used by {@code PainlessDocGenerator} to generate an API reference. */
    static Collection<Type> allSimpleTypes() {
        return BUILTINS.simpleTypesMap.values();
    }

    // INTERNAL IMPLEMENTATION:

    private final Map<Class<?>, RuntimeClass> runtimeMap;
    private final Map<String, Struct> structsMap;
    private final Map<String, Type> simpleTypesMap;

    public Definition(List<StructWhitelist> structWhitelists) {
        structsMap = new HashMap<>();
        simpleTypesMap = new HashMap<>();
        runtimeMap = new HashMap<>();

        Map<String, Struct> classNamesToStructs = new HashMap<>();

        for (StructWhitelist structWhitelist : structWhitelists) {
            Struct struct = addStruct(structWhitelist.alias, structWhitelist.clazz);

            if ("def".equals(structWhitelist.alias) == false && classNamesToStructs.containsKey(structWhitelist.clazz.getName())) {
                throw new IllegalArgumentException("Cannot specify the same class [" + structWhitelist.clazz + " ] with multiple aliases.");
            }

            if ("def".equals(structWhitelist.alias) == false) {
                classNamesToStructs.put(structWhitelist.clazz.getName(), struct);
            }
        }

        for (StructWhitelist structWhitelist : structWhitelists) {
            Struct struct = structsMap.get(structWhitelist.alias);

            for (WhitelistedObject object : structWhitelist.whitelist) {
                if (object.object instanceof java.lang.reflect.Constructor) {
                    assert object.augmented == false;
                    addConstructorInternal(struct, classNamesToStructs, object.alias, (Constructor<?>)object.object);
                } else if (object.object instanceof java.lang.reflect.Method) {
                    addMethodInternal(struct, classNamesToStructs, object.alias, object.augmented, (java.lang.reflect.Method)object.object);
                } else if (object.object instanceof java.lang.reflect.Field) {
                    assert object.augmented == false;
                    addFieldInternal(struct, classNamesToStructs, object.alias, (java.lang.reflect.Field)object.object);
                }
            }
        }

        String base = classNamesToStructs.get(Object.class.getName()).name;

        for (StructWhitelist structWhitelist : structWhitelists) {
            if (base != null && structWhitelist.clazz.isInterface() && structWhitelist.supers.contains(base) == false) {
                List<String> supersWithBase = new ArrayList<>(structWhitelist.supers);
                supersWithBase.add(base);
                copyStruct(structWhitelist.alias, supersWithBase);
            } else {
                copyStruct(structWhitelist.alias, structWhitelist.supers);
            }
        }

        for (Struct clazz : structsMap.values()) {
            clazz.functionalMethod.set(computeFunctionalInterfaceMethod(clazz));
        }

        for (Struct struct : structsMap.values()) {
            addRuntimeClass(struct);
        }

        for (Map.Entry<String, Struct> entry : structsMap.entrySet()) {
            entry.setValue(entry.getValue().freeze());
        }
    }

    private Struct addStruct(String name, Class<?> clazz) {
        if (!name.matches("^[_a-zA-Z][\\.,_a-zA-Z0-9]*$")) {
            throw new IllegalArgumentException("Invalid struct name [" + name + "].");
        }

        if (structsMap.containsKey(name)) {
            throw new IllegalArgumentException("Duplicate struct name [" + name + "].");
        }

        final Struct struct = new Struct(name, clazz, org.objectweb.asm.Type.getType(clazz));

        structsMap.put(name, struct);
        simpleTypesMap.put(name, getTypeInternal(name));

        return struct;
    }

    private void addConstructorInternal(Struct owner, Map<String, Struct> classNamesToStructs, String alias, Constructor<?> constructor) {
        if (!owner.clazz.equals(constructor.getDeclaringClass())) {
            throw new IllegalArgumentException("Constructor's class [" + constructor.getDeclaringClass() + "] " +
                "does not match owner struct [" + owner.name + "] class [" + owner.clazz + "].");
        }

        if (!"<init>".equals(alias)) {
            throw new IllegalArgumentException("Invalid constructor name [" + alias + "] with the struct [" + owner.name + "].");
        }

        MethodKey methodKey = new MethodKey(alias, constructor.getParameterCount());

        if (owner.constructors.containsKey(methodKey)) {
            throw new IllegalArgumentException("Duplicate constructor [" + methodKey + "] found within the struct [" + owner.name + "].");
        }

        org.objectweb.asm.commons.Method asm = org.objectweb.asm.commons.Method.getMethod(constructor);
        Type returnType = getTypeInternal("void");
        MethodHandle handle;

        try {
            handle = MethodHandles.publicLookup().in(owner.clazz).unreflectConstructor(constructor);
        } catch (final IllegalAccessException exception) {
            throw new IllegalArgumentException("Constructor " +
                " not found for class [" + owner.clazz.getName() + "]" +
                " with arguments " + Arrays.toString(constructor.getParameterTypes()) + ".");
        }

        Type[] types = new Type[constructor.getParameterCount()];

        for (int type = 0; type < constructor.getParameterCount(); ++type) {
            types[type] = getTypeInternal(classNamesToStructs, constructor.getParameterTypes()[type]);
        }

        owner.constructors.put(methodKey,
            new Method(alias, owner, null, returnType, Arrays.asList(types), asm, constructor.getModifiers(), handle));
    }

    private void addMethodInternal(Struct owner, Map<String, Struct> classNamesToStructs, String alias, boolean augmented,
                                   java.lang.reflect.Method method) {
        if (!alias.matches("^[_a-zA-Z][_a-zA-Z0-9]*$")) {
            throw new IllegalArgumentException("Invalid method name [" + alias + "] with the struct [" + owner.name + "].");
        }

        MethodKey methodKey = new MethodKey(alias, method.getParameterCount());

        if (owner.staticMethods.containsKey(methodKey) || owner.methods.containsKey(methodKey)) {
            throw new IllegalArgumentException(
                "Duplicate method signature [" + methodKey + "] found within the struct [" + owner.name + "].");
        }

        Type rtn = getTypeInternal(classNamesToStructs, method.getReturnType());

        Type types[] = new Type[method.getParameterCount() - (augmented ? 1 : 0)];

        for (int count = augmented ? 1 : 0; count < method.getParameterCount(); ++count) {
            types[count - (augmented ? 1 : 0)] = getTypeInternal(classNamesToStructs, method.getParameterTypes()[count]);
        }

        org.objectweb.asm.commons.Method asm = org.objectweb.asm.commons.Method.getMethod(method);
        MethodHandle handle;

        try {
            handle = MethodHandles.publicLookup().in(method.getDeclaringClass()).unreflect(method);
        } catch (final IllegalAccessException exception) {
            throw new IllegalArgumentException("Method [" + alias + "]" +
                " not found for class [" + method.getDeclaringClass().getName() + "]" +
                " with arguments " + Arrays.toString(method.getParameterTypes()) + ".");
        }

        int modifiers = method.getModifiers();
        Method m =
            new Method(alias, owner, augmented ? method.getDeclaringClass() : null, rtn, Arrays.asList(types), asm, modifiers, handle);

        if (augmented == false && java.lang.reflect.Modifier.isStatic(modifiers)) {
            owner.staticMethods.put(methodKey, m);
        } else {
            owner.methods.put(methodKey, m);
        }
    }

    private void addFieldInternal(Struct owner, Map<String, Struct> classNamesToStructs, String alias, java.lang.reflect.Field field) {
        if (!alias.matches("^[_a-zA-Z][_a-zA-Z0-9]*$")) {
            throw new IllegalArgumentException("Invalid field name [" + alias + "] with the struct [" + owner.name + "].");
        }

        if (owner.staticMembers.containsKey(alias) || owner.members.containsKey(alias)) {
             throw new IllegalArgumentException("Duplicate field name [" + alias + "]" +
                     " found within the struct [" + owner.name + "].");
        }

        Type type = getTypeInternal(classNamesToStructs, field.getType());

        int modifiers = field.getModifiers();
        boolean isStatic = java.lang.reflect.Modifier.isStatic(modifiers);

        MethodHandle getter = null;
        MethodHandle setter = null;

        try {
            if (!isStatic) {
                getter = MethodHandles.publicLookup().unreflectGetter(field);
                setter = MethodHandles.publicLookup().unreflectSetter(field);
            }
        } catch (IllegalAccessException exception) {
            throw new IllegalArgumentException("Getter/Setter [" + alias + "] not found for class [" + owner.clazz.getName() + "].");
        }

        Field f = new Field(alias, field.getName(), owner, type, modifiers, getter, setter);

        if (isStatic) {
            // require that all static fields are static final
            if (!java.lang.reflect.Modifier.isFinal(modifiers)) {
                throw new IllegalArgumentException("Static field [" + alias + "] within the struct [" + owner.name + "] is not final.");
            }

            owner.staticMembers.put(alias, f);
        } else {
            owner.members.put(alias, f);
        }
    }

    private void copyStruct(String struct, List<String> children) {
        final Struct owner = structsMap.get(struct);

        if (owner.clazz == Collection.class) {
            boolean bp = true;
        }

        if (owner == null) {
            throw new IllegalArgumentException("Owner struct [" + struct + "] not defined for copy.");
        }

        for (int count = 0; count < children.size(); ++count) {
            final Struct child = structsMap.get(children.get(count));

            if (child == null) {
                throw new IllegalArgumentException("Child struct [" + children.get(count) + "]" +
                    " not defined for copy to owner struct [" + owner.name + "].");
            }

            if (!child.clazz.isAssignableFrom(owner.clazz)) {
                throw new ClassCastException("Child struct [" + child.name + "]" +
                    " is not a super type of owner struct [" + owner.name + "] in copy.");
            }

            for (Map.Entry<MethodKey,Method> kvPair : child.methods.entrySet()) {
                MethodKey methodKey = kvPair.getKey();
                Method method = kvPair.getValue();
                if (owner.methods.get(methodKey) == null) {
                    // sanity check, look for missing covariant/generic override
                    if (owner.clazz.isInterface() && structsMap.get("def").methods.containsKey(methodKey)) {
                        // ok
                    } else if (child.clazz == Spliterator.OfPrimitive.class || child.clazz == PrimitiveIterator.class) {
                        // ok, we rely on generics erasure for these (its guaranteed in the javadocs though!!!!)
                    } else if (Constants.JRE_IS_MINIMUM_JAVA9 && owner.clazz == LocalDate.class) {
                        // ok, java 9 added covariant override for LocalDate.getEra() to return IsoEra:
                        // https://bugs.openjdk.java.net/browse/JDK-8072746
                    } else {
                        try {
                            final Class<?> impl = method.augmentation == null ? owner.clazz : method.augmentation;
                            final Class<?> arguments[];
                            arguments = new Class<?>[method.arguments.size()];
                            for (int i = 0; i < method.arguments.size(); i++) {
                                arguments[i] = method.arguments.get(i).clazz;
                            }
                            java.lang.reflect.Method m = impl.getMethod(method.method.getName(), arguments);
                            if (m.getReturnType() != method.rtn.clazz) {
                                throw new IllegalStateException("missing covariant override for: " + m + " in " + owner.name);
                            }
                            if (m.isBridge() && !Modifier.isVolatile(method.modifiers)) {
                                // its a bridge in the destination, but not in the source, but it might still be ok, check generics:
                                java.lang.reflect.Method source = child.clazz.getMethod(method.method.getName(), arguments);
                                if (!Arrays.equals(source.getGenericParameterTypes(), source.getParameterTypes())) {
                                    throw new IllegalStateException("missing generic override for: " + m + " in " + owner.name);
                                }
                            }
                        } catch (ReflectiveOperationException e) {
                            throw new AssertionError(e);
                        }
                    }
                    owner.methods.put(methodKey, method);
                }
            }

            for (Field field : child.members.values()) {
                if (owner.members.get(field.name) == null) {
                    owner.members.put(field.name,
                        new Field(field.name, field.javaName, owner, field.type, field.modifiers, field.getter, field.setter));
                }
            }
        }
    }

    /**
     * Precomputes a more efficient structure for dynamic method/field access.
     */
    private void addRuntimeClass(final Struct struct) {
        final Map<MethodKey, Method> methods = struct.methods;
        final Map<String, MethodHandle> getters = new HashMap<>();
        final Map<String, MethodHandle> setters = new HashMap<>();

        // add all members
        for (final Map.Entry<String, Field> member : struct.members.entrySet()) {
            getters.put(member.getKey(), member.getValue().getter);
            setters.put(member.getKey(), member.getValue().setter);
        }

        // add all getters/setters
        for (final Map.Entry<MethodKey, Method> method : methods.entrySet()) {
            final String name = method.getKey().name;
            final Method m = method.getValue();

            if (m.arguments.size() == 0 &&
                name.startsWith("get") &&
                name.length() > 3 &&
                Character.isUpperCase(name.charAt(3))) {
                final StringBuilder newName = new StringBuilder();
                newName.append(Character.toLowerCase(name.charAt(3)));
                newName.append(name.substring(4));
                getters.putIfAbsent(newName.toString(), m.handle);
            } else if (m.arguments.size() == 0 &&
                name.startsWith("is") &&
                name.length() > 2 &&
                Character.isUpperCase(name.charAt(2))) {
                final StringBuilder newName = new StringBuilder();
                newName.append(Character.toLowerCase(name.charAt(2)));
                newName.append(name.substring(3));
                getters.putIfAbsent(newName.toString(), m.handle);
            }

            if (m.arguments.size() == 1 &&
                name.startsWith("set") &&
                name.length() > 3 &&
                Character.isUpperCase(name.charAt(3))) {
                final StringBuilder newName = new StringBuilder();
                newName.append(Character.toLowerCase(name.charAt(3)));
                newName.append(name.substring(4));
                setters.putIfAbsent(newName.toString(), m.handle);
            }
        }

        runtimeMap.put(struct.clazz, new RuntimeClass(struct, methods, getters, setters));
    }

    /** computes the functional interface method for a class, or returns null */
    private Method computeFunctionalInterfaceMethod(Struct clazz) {
        if (!clazz.clazz.isInterface()) {
            return null;
        }
        // if its marked with this annotation, we fail if the conditions don't hold (means whitelist bug)
        // otherwise, this annotation is pretty useless.
        boolean hasAnnotation = clazz.clazz.isAnnotationPresent(FunctionalInterface.class);
        List<java.lang.reflect.Method> methods = new ArrayList<>();
        for (java.lang.reflect.Method m : clazz.clazz.getMethods()) {
            // default interface methods don't count
            if (m.isDefault()) {
                continue;
            }
            // static methods don't count
            if (Modifier.isStatic(m.getModifiers())) {
                continue;
            }
            // if its from Object, it doesn't count
            try {
                Object.class.getMethod(m.getName(), m.getParameterTypes());
                continue;
            } catch (ReflectiveOperationException e) {
                // it counts
            }
            methods.add(m);
        }
        if (methods.size() != 1) {
            if (hasAnnotation) {
                throw new IllegalArgumentException("Class: " + clazz.name +
                                                   " is marked with FunctionalInterface but doesn't fit the bill: " + methods);
            }
            return null;
        }
        // inspect the one method found from the reflection API, it should match the whitelist!
        java.lang.reflect.Method oneMethod = methods.get(0);
        Method painless = clazz.methods.get(new Definition.MethodKey(oneMethod.getName(), oneMethod.getParameterCount()));
        if (painless == null || painless.method.equals(org.objectweb.asm.commons.Method.getMethod(oneMethod)) == false) {
            throw new IllegalArgumentException("Class: " + clazz.name + " is functional but the functional " +
                                               "method is not whitelisted!");
        }
        return painless;
    }

    private static Class<?> getClassInternal(Map<String, Class<?>> aliasesToClasses, String className) throws ClassNotFoundException {
        int arrayDimensions = getDimensions(className);
        className = className.substring(0, className.length() - 2*arrayDimensions);
        Class<?> clazz = aliasesToClasses.get(className);
        className = clazz.getName();

        if (arrayDimensions > 0) {
            if (void.class == clazz) {
                className = "V";
            } else if (byte.class == clazz) {
                className = "B";
            } else if (short.class == clazz) {
                className = "S";
            } else if (char.class == clazz) {
                className = "C";
            } else if (int.class == clazz) {
                className = "I";
            } else if (long.class == clazz) {
                className = "J";
            } else if (float.class == clazz) {
                className = "F";
            } else if (double.class == clazz) {
                className = "D";
            } else {
                className = "L" + className +";";
            }

            StringBuilder builder = new StringBuilder();

            for (int arrayDimension = 0; arrayDimension < arrayDimensions; ++arrayDimension) {
                builder.append("[");
            }

            builder.append(className);
            clazz = Class.forName(builder.toString());
        }

        return clazz;
    }

    private Type getTypeInternal(Map<String, Struct> classNamesToStructs, Class<?> clazz) {
        String className = clazz.getName();

        int arrayDimensions = getDimensions(className);

        if (className.charAt(0) == '[') {
            className = className.substring(arrayDimensions);

            if ("V".equals(className)) {
                className = "void";
            } else if ("B".equals(className)) {
                className = "byte";
            } else if ("S".equals(className)) {
                className = "short";
            } else if ("C".equals(className)) {
                className = "char";
            } else if ("I".equals(className)) {
                className = "int";
            } else if ("J".equals(className)) {
                className = "long";
            } else if ("F".equals(className)) {
                className = "float";
            } else if ("D".equals(className)) {
                className = "double";
            } else {
                className = className.substring(1, className.length() - 1);
            }
        } else {
            className = className.substring(0, className.length() - 2 * arrayDimensions);
        }

        Struct paramStruct = classNamesToStructs.get(className);

        if (paramStruct.clazz.equals(Object.class)) {
            paramStruct = structsMap.get("def");
        }

        return getTypeInternal(paramStruct, arrayDimensions);
    }

    private Type getTypeInternal(String name) {
        // simple types (e.g. 0 array dimensions) are a simple hash lookup for speed
        Type simple = simpleTypesMap.get(name);

        if (simple != null) {
            return simple;
        }

        int dimensions = getDimensions(name);
        String structstr = dimensions == 0 ? name : name.substring(0, name.indexOf('['));
        Struct struct = structsMap.get(structstr);

        if (struct == null) {
            throw new IllegalArgumentException("The struct with name [" + name + "] has not been defined.");
        }

        return getTypeInternal(struct, dimensions);
    }

    private static Type getTypeInternal(Struct struct, int dimensions) {
        String name = struct.name;
        org.objectweb.asm.Type type = struct.type;
        Class<?> clazz = struct.clazz;
        Sort sort;

        if (dimensions > 0) {
            StringBuilder builder = new StringBuilder(name);
            char[] brackets = new char[dimensions];

            for (int count = 0; count < dimensions; ++count) {
                builder.append("[]");
                brackets[count] = '[';
            }

            String descriptor = new String(brackets) + struct.type.getDescriptor();

            name = builder.toString();
            type = org.objectweb.asm.Type.getType(descriptor);

            try {
                clazz = Class.forName(type.getInternalName().replace('/', '.'));
            } catch (ClassNotFoundException exception) {
                throw new IllegalArgumentException("The class [" + type.getInternalName() + "]" +
                    " could not be found to create type [" + name + "].");
            }

            sort = Sort.ARRAY;
        } else if ("def".equals(struct.name)) {
            sort = Sort.DEF;
        } else {
            sort = Sort.OBJECT;

            for (Sort value : Sort.values()) {
                if (value.clazz == null) {
                    continue;
                }

                if (value.clazz.equals(struct.clazz)) {
                    sort = value;

                    break;
                }
            }
        }

        return new Type(name, dimensions, struct, clazz, type, sort);
    }

    private static int getDimensions(String name) {
        int dimensions = 0;
        int index = name.indexOf('[');

        if (index != -1) {
            int length = name.length();

            if (index == 0) {
                while (index < length && name.charAt(index++) == '[') {
                    ++dimensions;
                }
            } else {
                while (index < length) {
                    if (name.charAt(index) == '[' && ++index < length && name.charAt(index++) == ']') {
                        ++dimensions;
                    } else {
                        throw new IllegalArgumentException("Invalid array braces in canonical name [" + name + "].");
                    }
                }
            }
        }

        return dimensions;
    }
}

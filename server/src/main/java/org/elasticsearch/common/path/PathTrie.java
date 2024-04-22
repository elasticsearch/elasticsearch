/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.path;

import org.elasticsearch.common.collect.Iterators;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class PathTrie<T> {

    enum TrieMatchingMode {
        /*
         * Retrieve only explicitly mapped nodes, no wildcards are
         * matched.
         */
        EXPLICIT_NODES_ONLY,
        /*
         * Retrieve only explicitly mapped nodes, with wildcards
         * allowed as root nodes.
         */
        WILDCARD_ROOT_NODES_ALLOWED,
        /*
         * Retrieve only explicitly mapped nodes, with wildcards
         * allowed as leaf nodes.
         */
        WILDCARD_LEAF_NODES_ALLOWED,
        /*
         * Retrieve both explicitly mapped and wildcard nodes.
         */
        WILDCARD_NODES_ALLOWED
    }

    private static final EnumSet<TrieMatchingMode> EXPLICIT_OR_ROOT_WILDCARD = EnumSet.of(
        TrieMatchingMode.EXPLICIT_NODES_ONLY,
        TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED
    );

    private final UnaryOperator<String> decoder;
    private final TrieNode root;
    private T rootValue;

    private static final String SEPARATOR = "/";
    private static final char REGEX_SEPARATOR = '|';
    private static final String WILDCARD = "*";

    public PathTrie(UnaryOperator<String> decoder) {
        this.decoder = decoder;
        root = new TrieNode(SEPARATOR, null);
    }

    private class TrieNode {
        private T value;
        private String namedWildcard;
        private Pattern wildcardRegex;

        private Map<String, TrieNode> children;

        private TrieNode(String key, T value) {
            this.value = value;
            this.children = emptyMap();
            if (isNamedWildcard(key)) {
                updateNamedWildcard(key);
            }
        }

        private void updateNamedWildcard(String key) {
            assert key.startsWith("{") && key.endsWith("}") : key + " is not a wildcard";

            Pattern newWildcardRegex;
            String newNamedWildcard;
            int regexIndex = key.lastIndexOf(REGEX_SEPARATOR);
            if (regexIndex >= 0) {
                // first part is regex, second part is param id
                // remember to strip off the { and }
                newWildcardRegex = Pattern.compile(key.substring(1, regexIndex));
                newNamedWildcard = key.substring(regexIndex + 1, key.length() - 1);
            } else {
                newWildcardRegex = null;
                newNamedWildcard = key.substring(1, key.length() - 1);
            }

            if (newNamedWildcard.equals(namedWildcard) == false) {
                if (namedWildcard != null) {
                    throw new IllegalArgumentException(
                        "Trying to use conflicting wildcard names for same path: [" + namedWildcard + "] and [" + newNamedWildcard + "]"
                    );
                }
                namedWildcard = newNamedWildcard;
            }

            if (newWildcardRegex != null
                && newWildcardRegex.pattern().equals(wildcardRegex != null ? wildcardRegex.pattern() : null) == false) {
                if (wildcardRegex != null) {
                    throw new IllegalArgumentException(
                        "Trying to use conflicting wildcard regex for same wildcard ["
                            + namedWildcard
                            + "]: ["
                            + wildcardRegex
                            + "] and ["
                            + newWildcardRegex
                            + "]"
                    );
                }
                wildcardRegex = newWildcardRegex;
            }
        }

        private void addInnerChild(String key, TrieNode child) {
            Map<String, TrieNode> newChildren = new HashMap<>(children);
            newChildren.put(key, child);
            children = unmodifiableMap(newChildren);
        }

        private synchronized void insert(String[] path, int index, T value) {
            if (index >= path.length) return;

            String token = path[index];
            String key = token;
            if (isNamedWildcard(token)) {
                key = WILDCARD;
            }

            TrieNode node = children.get(key);
            if (node == null) {
                T nodeValue = index == path.length - 1 ? value : null;
                node = new TrieNode(token, nodeValue);
                addInnerChild(key, node);
            } else {
                if (isNamedWildcard(token)) {
                    node.updateNamedWildcard(token);
                }
                /*
                 * If the target node already exists, but is without a value,
                 *  then the value should be updated.
                 */
                if (index == (path.length - 1)) {
                    if (node.value != null) {
                        throw new IllegalArgumentException(
                            "Path [" + String.join("/", path) + "] already has a value [" + node.value + "]"
                        );
                    } else {
                        node.value = value;
                    }
                }
            }

            node.insert(path, index + 1, value);
        }

        private synchronized void insertOrUpdate(String[] path, int index, T value, BinaryOperator<T> updater) {
            if (index >= path.length) return;

            String token = path[index];
            String key = token;
            if (isNamedWildcard(token)) {
                key = WILDCARD;
            }

            TrieNode node = children.get(key);
            if (node == null) {
                T nodeValue = index == path.length - 1 ? value : null;
                node = new TrieNode(token, nodeValue);
                addInnerChild(key, node);
            } else {
                if (isNamedWildcard(token)) {
                    node.updateNamedWildcard(token);
                }
                /*
                 * If the target node already exists, but is without a value,
                 *  then the value should be updated.
                 */
                if (index == (path.length - 1)) {
                    if (node.value != null) {
                        node.value = updater.apply(node.value, value);
                    } else {
                        node.value = value;
                    }
                }
            }

            node.insertOrUpdate(path, index + 1, value, updater);
        }

        private static boolean isNamedWildcard(String key) {
            return key.charAt(0) == '{' && key.charAt(key.length() - 1) == '}';
        }

        private TrieNode getWildcardNodeForToken(String token) {
            TrieNode wildcard = children.get(WILDCARD);
            // check the wildcard match predicate too, if the token itself is not a wildcard
            if (token.equals(WILDCARD) == false
                && wildcard != null
                && wildcard.wildcardRegex != null
                && wildcard.wildcardRegex.matcher(token).matches() == false) {
                return null;
            }
            return wildcard;
        }

        private T retrieve(String[] path, int index, Map<String, String> params, TrieMatchingMode trieMatchingMode) {
            if (index >= path.length) return null;

            String token = path[index];
            TrieNode node = children.get(token);
            boolean usedWildcard;

            if (node == null) {
                if (trieMatchingMode == TrieMatchingMode.WILDCARD_NODES_ALLOWED) {
                    node = getWildcardNodeForToken(token);
                    if (node == null) {
                        return null;
                    }
                    usedWildcard = true;
                } else if (trieMatchingMode == TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED && index == 1) {
                    /*
                     * Allow root node wildcard matches.
                     */
                    node = getWildcardNodeForToken(token);
                    if (node == null) {
                        return null;
                    }
                    usedWildcard = true;
                } else if (trieMatchingMode == TrieMatchingMode.WILDCARD_LEAF_NODES_ALLOWED && index + 1 == path.length) {
                    /*
                     * Allow leaf node wildcard matches.
                     */
                    node = getWildcardNodeForToken(token);
                    if (node == null) {
                        return null;
                    }
                    usedWildcard = true;
                } else {
                    return null;
                }
            } else {
                TrieNode wildcardNode;
                if (index + 1 == path.length
                    && node.value == null
                    && EXPLICIT_OR_ROOT_WILDCARD.contains(trieMatchingMode) == false
                    && (wildcardNode = getWildcardNodeForToken(token)) != null) {
                    /*
                     * If we are at the end of the path, the current node does not have a value but
                     * there is a child wildcard node, use the child wildcard node.
                     */
                    node = wildcardNode;
                    usedWildcard = true;
                } else if (index == 1
                    && node.value == null
                    && trieMatchingMode == TrieMatchingMode.WILDCARD_ROOT_NODES_ALLOWED
                    && (wildcardNode = getWildcardNodeForToken(token)) != null) {
                        /*
                         * If we are at the root, and root wildcards are allowed, use the child wildcard
                         * node.
                         */
                        node = wildcardNode;
                        usedWildcard = true;
                    } else {
                        usedWildcard = token.equals(WILDCARD);
                    }
            }

            recordWildcardParam(params, node, token);

            if (index == (path.length - 1)) {
                return node.value;
            }

            T nodeValue = node.retrieve(path, index + 1, params, trieMatchingMode);
            if (nodeValue == null && usedWildcard == false && trieMatchingMode != TrieMatchingMode.EXPLICIT_NODES_ONLY) {
                node = getWildcardNodeForToken(token);
                if (node != null) {
                    recordWildcardParam(params, node, token);
                    nodeValue = node.retrieve(path, index + 1, params, trieMatchingMode);
                }
            }

            return nodeValue;
        }

        private void recordWildcardParam(Map<String, String> params, TrieNode node, String value) {
            if (params != null && node.namedWildcard != null) {
                params.put(node.namedWildcard, decoder.apply(value));
            }
        }

        private Iterator<T> allNodeValues() {
            final Iterator<T> childrenIterator = Iterators.flatMap(children.values().iterator(), TrieNode::allNodeValues);
            if (value == null) {
                return childrenIterator;
            } else {
                return Iterators.concat(Iterators.single(value), childrenIterator);
            }
        }
    }

    public void insert(String path, T value) {
        String[] strings = path.split(SEPARATOR);
        if (strings.length == 0) {
            if (rootValue != null) {
                throw new IllegalArgumentException("Path [/] already has a value [" + rootValue + "]");
            }
            rootValue = value;
            return;
        }
        int index = 0;
        // Supports initial delimiter.
        if (strings[0].isEmpty()) {
            index = 1;
        }
        root.insert(strings, index, value);
    }

    /**
     * Insert a value for the given path. If the path already exists, replace the value with:
     * <pre>
     * value = updater.apply(oldValue, newValue);
     * </pre>
     * allowing the value to be updated if desired.
     */
    public void insertOrUpdate(String path, T value, BinaryOperator<T> updater) {
        String[] strings = path.split(SEPARATOR);
        if (strings.length == 0) {
            if (rootValue != null) {
                rootValue = updater.apply(rootValue, value);
            } else {
                rootValue = value;
            }
            return;
        }
        int index = 0;
        // Supports initial delimiter.
        if (strings[0].isEmpty()) {
            index = 1;
        }
        root.insertOrUpdate(strings, index, value, updater);
    }

    public T retrieve(String path) {
        return retrieve(path, null, TrieMatchingMode.WILDCARD_NODES_ALLOWED);
    }

    public T retrieve(String path, Map<String, String> params) {
        return retrieve(path, params, TrieMatchingMode.WILDCARD_NODES_ALLOWED);
    }

    T retrieve(String path, Map<String, String> params, TrieMatchingMode trieMatchingMode) {
        if (path.isEmpty()) {
            return rootValue;
        }
        String[] strings = path.split(SEPARATOR);
        if (strings.length == 0) {
            return rootValue;
        }
        int index = 0;

        // Supports initial delimiter.
        if (strings[0].isEmpty()) {
            index = 1;
        }

        return root.retrieve(strings, index, params, trieMatchingMode);
    }

    /**
     * Returns a stream of the objects stored in the {@code PathTrie}, using
     * all possible {@code TrieMatchingMode} modes. The {@code paramSupplier}
     * is called for each mode to supply a new map of parameters.
     */
    public Stream<T> retrieveAll(String path, Supplier<Map<String, String>> paramSupplier) {
        return Arrays.stream(TrieMatchingMode.values()).map(m -> retrieve(path, paramSupplier.get(), m));
    }

    public Iterator<T> allNodeValues() {
        return Iterators.concat(Iterators.single(rootValue), root.allNodeValues());
    }
}

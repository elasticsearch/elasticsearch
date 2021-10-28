/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.support.filtering;

import org.elasticsearch.core.Glob;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FilterPath {
    private static final String WILDCARD = "*";
    private static final String DOUBLE_WILDCARD = "**";

    private final Map<String, FilterPath> termsChildren;
    private final List<FilterPath> wildcardChildren;
    private final String pattern;
    private final boolean doubleWildcard;
    private final boolean isFinalNode;
    private boolean hasDoubleWildcard;

    private FilterPath(
        String pattern,
        boolean doubleWildcard,
        boolean isFinalNode,
        boolean hasDoubleWildcard,
        Map<String, FilterPath> termsChildren,
        List<FilterPath> wildcardChildren
    ) {
        this.pattern = pattern;
        this.doubleWildcard = doubleWildcard;
        this.isFinalNode = isFinalNode;
        this.hasDoubleWildcard = hasDoubleWildcard;
        this.termsChildren = termsChildren;
        this.wildcardChildren = wildcardChildren;
    }

    public boolean hasDoubleWildcard() {
        return hasDoubleWildcard;
    }

    private String getPattern() {
        return pattern;
    }

    private boolean isFinalNode() {
        return isFinalNode;
    }

    /**
     * check if the name matches filter nodes
     * if the name equals the filter node name, the node will add to nextFilters.
     * if the filter node is a final node, it means the name matches the pattern, and return true
     * if the name don't equal a final node, then return false, continue to check the inner filter node
     * if current node is a double wildcard node, the node will also add to nextFilters.
     * @param name the xcontent property name
     * @param nextFilters nextFilters is a List, used to check the inner property of name
     * @return true if the name equal a final node, otherwise return false
     */
    boolean matches(String name, List<FilterPath> nextFilters) {
        if (nextFilters == null) {
            return false;
        }

        FilterPath termNode = termsChildren.get(name);
        if (termNode != null) {
            if (termNode.isFinalNode()) {
                return true;
            } else {
                nextFilters.add(termNode);
            }
        }

        for (FilterPath wildcardNode : wildcardChildren) {
            String wildcardPattern = wildcardNode.getPattern();
            if (Glob.globMatch(wildcardPattern, name)) {
                if (wildcardNode.isFinalNode()) {
                    return true;
                } else {
                    nextFilters.add(wildcardNode);
                }
            }
        }

        if (doubleWildcard) {
            nextFilters.add(this);
        }

        return false;
    }

    private static class FilterPathBuilder {
        private class BuildNode {
            private final Map<String, BuildNode> children;
            private final boolean isFinalNode;

            BuildNode(boolean isFinalNode) {
                children = new HashMap<>();
                this.isFinalNode = isFinalNode;
            }
        }

        private BuildNode root = new BuildNode(false);

        void insert(String filter) {
            insertNode(filter, root);
        }

        FilterPath build() {
            return buildPath("", root);
        }

        void insertNode(String filter, BuildNode node) {
            int end = filter.length();
            for (int i = 0; i < end;) {
                char c = filter.charAt(i);
                if (c == '.') {
                    String field = filter.substring(0, i).replaceAll("\\\\.", ".");
                    BuildNode child = node.children.get(field);
                    if (child == null) {
                        child = new BuildNode(false);
                        node.children.put(field, child);
                    }
                    if (false == child.isFinalNode) {
                        insertNode(filter.substring(i + 1), child);
                    }
                    return;
                }
                ++i;
                if ((c == '\\') && (i < end) && (filter.charAt(i) == '.')) {
                    ++i;
                }
            }

            String field = filter.replaceAll("\\\\.", ".");
            node.children.put(field, new BuildNode(true));
        }

        FilterPath buildPath(String segment, BuildNode node) {
            Map<String, FilterPath> termsChildren = new HashMap<>();
            List<FilterPath> wildcardChildren = new ArrayList<>();
            for (Map.Entry<String, BuildNode> entry : node.children.entrySet()) {
                String childName = entry.getKey();
                BuildNode childNode = entry.getValue();
                FilterPath childFilterPath = buildPath(childName, childNode);
                if (childName.contains(WILDCARD)) {
                    wildcardChildren.add(childFilterPath);
                } else {
                    termsChildren.put(childName, childFilterPath);
                }
            }

            boolean doubleWildcard = segment.equals(DOUBLE_WILDCARD);
            boolean isFinalNode = node.isFinalNode;
            boolean hasDoubleWildcard = hasDoubleWildcard(segment, termsChildren, wildcardChildren);

            return new FilterPath(segment, doubleWildcard, isFinalNode, hasDoubleWildcard, termsChildren, wildcardChildren);
        }

        static boolean hasDoubleWildcard(String name, Map<String, FilterPath> termsChildren, List<FilterPath> wildcardChildren) {
            if (name.contains(DOUBLE_WILDCARD)) {
                return true;
            }
            for (FilterPath filterPath : wildcardChildren) {
                if (filterPath.hasDoubleWildcard()) {
                    return true;
                }
            }
            for (FilterPath filterPath : termsChildren.values()) {
                if (filterPath.hasDoubleWildcard()) {
                    return true;
                }
            }
            return false;
        }
    }

    public static FilterPath[] compile(Set<String> filters) {
        if (filters == null || filters.isEmpty()) {
            return null;
        }

        FilterPathBuilder builder = new FilterPathBuilder();
        for (String filter : filters) {
            if (filter != null) {
                filter = filter.trim();
                if (filter.length() > 0) {
                    builder.insert(filter);
                }
            }
        }
        FilterPath filterPath = builder.build();
        return Collections.singletonList(filterPath).toArray(new FilterPath[0]);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A basic directed acyclic graph
 */
class Graph<E> {
    // map of node to its successors
    private final Map<E, Set<E>> successors = new LinkedHashMap<>();
    private final Map<E, Set<E>> predecessors = new LinkedHashMap<>();
    // set of nodes with no predecessors to start iteration from
    private final Set<E> startingNodes = new LinkedHashSet<>();

    public boolean addNode(E node) {
        boolean addedSucc = successors.putIfAbsent(node, new LinkedHashSet<>()) == null;
        boolean addedPred = predecessors.putIfAbsent(node, new LinkedHashSet<>()) == null;
        assert addedSucc == addedPred;
        if (addedSucc) {
            startingNodes.add(node);
        }
        return addedSucc;
    }

    private Set<E> add(E node) {
        Set<E> newSet = new LinkedHashSet<>();
        Set<E> existing = successors.putIfAbsent(node, newSet);
        if (existing == null) {
            predecessors.put(node, new LinkedHashSet<>());
            startingNodes.add(node);
            return newSet;
        } else {
            return existing;
        }
    }

    public boolean addEdge(E source, E target) {
        boolean modified = false;
        modified |= addNode(target);
        modified |= add(source).add(target);
        if (modified) {
            predecessors.get(target).add(source);
            // target is no longer a starting node
            startingNodes.remove(target);
        }
        return modified;
    }

    public Set<E> nodes() {
        return Collections.unmodifiableSet(successors.keySet());
    }

    public Set<E> predecessors(E node) {
        return Collections.unmodifiableSet(predecessors.getOrDefault(node, Set.of()));
    }

    private class GraphIterator implements Iterator<E> {
        private final Deque<E> nextNodes;
        private final Set<E> seenNodes = new HashSet<>();
        private final BiConsumer<Deque<E>, E> addNewNodes;

        private GraphIterator(Collection<E> startingNodes, BiConsumer<Deque<E>, E> addNewNodes) {
            this.nextNodes = new ArrayDeque<>(startingNodes);
            this.addNewNodes = addNewNodes;
        }

        @Override
        public boolean hasNext() {
            if (nextNodes.isEmpty()) {
                checkLoop();
                return false;
            } else {
                return true;
            }
        }

        private void checkLoop() {
            if (seenNodes.size() < successors.size()) {
                Set<E> loopedNodes = new HashSet<>(successors.keySet());
                loopedNodes.removeAll(seenNodes);
                // TODO: order these by iteration order
                throw new IllegalStateException(
                    "Cycle found in nodes " + loopedNodes.stream().map(Object::toString).collect(Collectors.joining(",", "[", "]"))
                );
            }
        }

        @Override
        public E next() {
            E next = nextNodes.removeFirst();
            if (seenNodes.add(next) == false) {
                throw new IllegalStateException("Cycle found");
            }
            addNewNodes.accept(nextNodes, next);
            return next;
        }
    }

    public void checkLoops() {
        // just do an iteration, it will throw at the end if there's looping nodes
        Iterator<E> it = new GraphIterator(startingNodes, (d, e) -> d.addAll(successors.get(e)));
        while (it.hasNext()) {
            it.next();
        }
    }

    public Iterator<E> breadthFirst() {
        return new GraphIterator(startingNodes, (d, e) -> successors.get(e).forEach(d::addLast));
    }

    public Iterator<E> depthFirst() {
        return new GraphIterator(startingNodes, (d, e) -> successors.get(e).forEach(d::addFirst));
    }

    private Stream<E> graphStream(Iterator<E> it) {
        return StreamSupport.stream(
            Spliterators.spliterator(it, successors.size(), Spliterator.SIZED | Spliterator.DISTINCT | Spliterator.ORDERED),
            false
        );
    }

    public Stream<E> streamBreadthFirst() {
        return graphStream(breadthFirst());
    }

    public Stream<E> streamDepthFirst() {
        return graphStream(depthFirst());
    }
}

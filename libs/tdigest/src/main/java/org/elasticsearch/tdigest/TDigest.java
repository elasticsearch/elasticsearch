/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * Adaptive histogram based on something like streaming k-means crossed with Q-digest.
 *
 * The special characteristics of this algorithm are:
 *
 * - smaller summaries than Q-digest
 *
 * - works on doubles as well as integers.
 *
 * - provides part per million accuracy for extreme quantiles and typically &lt;1000 ppm accuracy for middle quantiles
 *
 * - fast
 *
 * - simple
 *
 * - test coverage roughly at 90%
 *
 * - easy to adapt for use with map-reduce
 */
public abstract class TDigest {
    protected ScaleFunction scale = ScaleFunction.K_2;
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;

    /**
     * Creates an {@link MergingDigest}.  This is generally the best known implementation right now.
     *
     * @param compression The compression parameter.  100 is a common value for normal uses.  1000 is extremely large.
     *                    The number of centroids retained will be a smallish (usually less than 10) multiple of this number.
     * @return the MergingDigest
     */
    @SuppressWarnings("WeakerAccess")
    public static TDigest createMergingDigest(double compression) {
        return new MergingDigest(compression);
    }

    /**
     * Creates an AVLTreeDigest.  AVLTreeDigest is nearly the best known implementation right now.
     *
     * @param compression The compression parameter.  100 is a common value for normal uses.  1000 is extremely large.
     *                    The number of centroids retained will be a smallish (usually less than 10) multiple of this number.
     * @return the AvlTreeDigest
     */
    @SuppressWarnings("WeakerAccess")
    public static TDigest createAvlTreeDigest(double compression) {
        return new AVLTreeDigest(compression);
    }

    /**
     * Creates a TDigest of whichever type is the currently recommended type.  MergingDigest is generally the best
     * known implementation right now.
     *
     * @param compression The compression parameter.  100 is a common value for normal uses.  1000 is extremely large.
     *                    The number of centroids retained will be a smallish (usually less than 10) multiple of this number.
     * @return the TDigest
     */
    @SuppressWarnings({ "WeakerAccess", "SameParameterValue" })
    public static TDigest createDigest(double compression) {
        return createMergingDigest(compression);
    }

    /**
     * Adds a sample to a histogram.
     *
     * @param x The value to add.
     * @param w The weight of this point.
     */
    public abstract void add(double x, int w);

    final void checkValue(double x) {
        if (Double.isNaN(x)) {
            throw new IllegalArgumentException("Cannot add NaN");
        }
    }

    public abstract void add(List<? extends TDigest> others);

    /**
     * Re-examines a t-digest to determine whether some centroids are redundant.  If your data are
     * perversely ordered, this may be a good idea.  Even if not, this may save 20% or so in space.
     *
     * The cost is roughly the same as adding as many data points as there are centroids.  This
     * is typically &lt; 10 * compression, but could be as high as 100 * compression.
     *
     * This is a destructive operation that is not thread-safe.
     */
    public abstract void compress();

    /**
     * Returns the number of points that have been added to this TDigest.
     *
     * @return The sum of the weights on all centroids.
     */
    public abstract long size();

    /**
     * Returns the fraction of all points added which are &le; x. Points
     * that are exactly equal get half credit (i.e. we use the mid-point
     * rule)
     *
     * @param x The cutoff for the cdf.
     * @return The fraction of all data which is less or equal to x.
     */
    public abstract double cdf(double x);

    /**
     * Returns an estimate of a cutoff such that a specified fraction of the data
     * added to this TDigest would be less than or equal to the cutoff.
     *
     * @param q The desired fraction
     * @return The smallest value x such that cdf(x) &ge; q
     */
    public abstract double quantile(double q);

    /**
     * A {@link Collection} that lets you go through the centroids in ascending order by mean.  Centroids
     * returned will not be re-used, but may or may not share storage with this TDigest.
     *
     * @return The centroids in the form of a Collection.
     */
    public abstract Collection<Centroid> centroids();

    /**
     * Returns the current compression factor.
     *
     * @return The compression factor originally used to set up the TDigest.
     */
    public abstract double compression();

    /**
     * Returns the number of bytes required to encode this TDigest using #asBytes().
     *
     * @return The number of bytes required.
     */
    public abstract int byteSize();

    /**
     * Returns the number of bytes required to encode this TDigest using #asSmallBytes().
     *
     * Note that this is just as expensive as actually compressing the digest. If you don't
     * care about time, but want to never over-allocate, this is fine. If you care about compression
     * and speed, you pretty much just have to overallocate by using allocating #byteSize() bytes.
     *
     * @return The number of bytes required.
     */
    public abstract int smallByteSize();

    public void setScaleFunction(ScaleFunction scaleFunction) {
        if (scaleFunction.toString().endsWith("NO_NORM")) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Can't use %s as scale with %s", scaleFunction, this.getClass()));
        }
        this.scale = scaleFunction;
    }

    /**
     * Serialize this TDigest into a byte buffer.  Note that the serialization used is
     * very straightforward and is considerably larger than strictly necessary.
     *
     * @param buf The byte buffer into which the TDigest should be serialized.
     */
    public abstract void asBytes(ByteBuffer buf);

    /**
     * Serialize this TDigest into a byte buffer.  Some simple compression is used
     * such as using variable byte representation to store the centroid weights and
     * using delta-encoding on the centroid means so that floats can be reasonably
     * used to store the centroid means.
     *
     * @param buf The byte buffer into which the TDigest should be serialized.
     */
    public abstract void asSmallBytes(ByteBuffer buf);

    /**
     * Tell this TDigest to record the original data as much as possible for test
     * purposes.
     *
     * @return This TDigest so that configurations can be done in fluent style.
     */
    public abstract TDigest recordAllData();

    public abstract boolean isRecording();

    /**
     * Add a sample to this TDigest.
     *
     * @param x The data value to add
     */
    public abstract void add(double x);

    /**
     * Add all of the centroids of another TDigest to this one.
     *
     * @param other The other TDigest
     */
    public abstract void add(TDigest other);

    public abstract int centroidCount();

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    /**
     * Over-ride the min and max values for testing purposes
     */
    @SuppressWarnings("SameParameterValue")
    void setMinMax(double min, double max) {
        this.min = min;
        this.max = max;
    }
}

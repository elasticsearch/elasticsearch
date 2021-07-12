/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.feature;

import org.apache.lucene.util.BitUtil;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;

import java.util.Comparator;
import java.util.List;

/**
 * Similar to {@link FeatureFactory} but only supports points and rectangles. It is just
 * more efficient for those shapes and it does not use external dependencies.
 */
public class SimpleFeatureFactory {

    private final int extent;
    private final double pointXScale, pointYScale, pointXTranslate, pointYTranslate;

    private static final int MOVETO = 1;
    private static final int LINETO = 2;
    private static final int CLOSEPATH = 7;

    private static final byte[] EMPTY = new byte[0];

    public SimpleFeatureFactory(int z, int x, int y, int extent) {
        this.extent = extent;
        final Rectangle rectangle = SphericalMercatorUtils.recToSphericalMercator(GeoTileUtils.toBoundingBox(x, y, z));
        pointXScale = (double) extent / (rectangle.getMaxLon() - rectangle.getMinLon());
        pointYScale = (double) -extent / (rectangle.getMaxLat() - rectangle.getMinLat());
        pointXTranslate = -pointXScale * rectangle.getMinX();
        pointYTranslate = -pointYScale * rectangle.getMinY();
    }

    /**
     * Returns a {@code byte[]} containing the mvt representation of the provided point
     */
    public byte[] point(double lon, double lat) {
        final int posLon = lon(lon);
        if (posLon > extent || posLon < 0) {
            return EMPTY;
        }
        final int posLat = lat(lat);
        if (posLat > extent || posLat < 0) {
            return EMPTY;
        }
        final int[] commands = new int[3];
        commands[0] = encodeCommand(MOVETO, 1);
        commands[1] = BitUtil.zigZagEncode(posLon);
        commands[2] = BitUtil.zigZagEncode(posLat);
        return writeCommands(commands, 1, 3);
    }

    /**
     * Returns a {@code byte[]} containing the mvt representation of the provided points
     */
    public byte[] points(List<Point> multiPoint) {
        multiPoint.sort(Comparator.comparingDouble(Point::getLon).thenComparingDouble(Point::getLat));
        final int[] commands = new int[2 * multiPoint.size() + 1];
        int pos = 1, prevLon = 0, prevLat = 0, numPoints = 0;
        for (int i = 0; i < multiPoint.size(); i++) {
            final Point point = multiPoint.get(i);
            final int posLon = lon(point.getLon());
            if (posLon > extent || posLon < 0) {
                continue;
            }
            final int posLat = lat(point.getLat());
            if (posLat > extent || posLat < 0) {
                continue;
            }
            if (i == 0 || posLon != prevLon || posLat != prevLat) {
                commands[pos++] = BitUtil.zigZagEncode(posLon - prevLon);
                commands[pos++] = BitUtil.zigZagEncode(posLat - prevLat);
                prevLon = posLon;
                prevLat = posLat;
                numPoints++;
            }
        }
        if (numPoints == 0) {
            return EMPTY;
        }
        commands[0] = encodeCommand(MOVETO, numPoints);
        return writeCommands(commands, 1, pos);
    }

    /**
     * Returns a {@code byte[]} containing the mvt representation of the provided rectangle
     */
    public byte[] box(double minLon, double maxLon, double minLat, double maxLat) {
        int[] commands = new int[11];
        final int minX = Math.max(0, lon(minLon));
        if (minX > extent) {
            return EMPTY;
        }
        final int minY = Math.min(extent, lat(minLat));
        if (minY > extent) {
            return EMPTY;
        }
        final int maxX = Math.min(extent, lon(maxLon));
        if (maxX < 0 || minX == maxX) {
            return EMPTY;
        }
        final int maxY = Math.max(0, lat(maxLat));
        if (maxY < 0 || minY == maxY) {
            return EMPTY;
        }
        commands[0] = encodeCommand(MOVETO, 1);
        commands[1] = BitUtil.zigZagEncode(minX);
        commands[2] = BitUtil.zigZagEncode(minY);
        commands[3] = encodeCommand(LINETO, 3);
        // 1
        commands[4] = BitUtil.zigZagEncode(maxX - minX);
        commands[5] = BitUtil.zigZagEncode(0);
        // 2
        commands[6] = BitUtil.zigZagEncode(0);
        commands[7] = BitUtil.zigZagEncode(maxY - minY);
        // 3
        commands[8] = BitUtil.zigZagEncode(minX - maxX);
        commands[9] = BitUtil.zigZagEncode(0);
        // close
        commands[10] = encodeCommand(CLOSEPATH, 1);
        return writeCommands(commands, 3, 11);
    }

    private int lat(double lat) {
        return (int) Math.round(pointYScale * SphericalMercatorUtils.latToSphericalMercator(lat) + pointYTranslate) + extent;
    }

    private int lon(double lon) {
        return (int) Math.round(pointXScale * SphericalMercatorUtils.lonToSphericalMercator(lon) + pointXTranslate);
    }

    private static int encodeCommand(int id, int length) {
        return (id & 0x7) | (length << 3);
    }

    private static byte[] writeCommands(final int[] commands, final int type, final int length) {
        int size = 3;
        int dataSize = 0;
        for (int i = 0; i < length; i++) {
            dataSize += computeUInt32SizeNoTag(commands[i]);
        }
        size += dataSize;
        size += computeUInt32SizeNoTag(dataSize);
        int position = 0;
        byte[] result = new byte[size];
        // for points
        position = writeInt(result, position, 24); // tag
        position = writeInt(result, position, type);
        position = writeInt(result, position, 34); // tag
        position = writeInt(result, position, dataSize);
        for (int i = 0; i < length; i++) {
            position = writeInt(result, position, commands[i]);
        }
        return result;
    }

    private static int writeInt(byte[] buffer, int position, int value) {
        assert value >= 0;
        while (true) {
            if ((value & ~0x7F) == 0) {
                buffer[position++] = (byte) value;
                return position;
            } else {
                buffer[position++] = (byte) ((value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    // Compute the number of bytes that would be needed to encode a uint32 field.
    private static int computeUInt32SizeNoTag(final int value) {
        if ((value & (~0 << 7)) == 0) {
            return 1;
        }
        if ((value & (~0 << 14)) == 0) {
            return 2;
        }
        if ((value & (~0 << 21)) == 0) {
            return 3;
        }
        if ((value & (~0 << 28)) == 0) {
            return 4;
        }
        return 5;
    }
}

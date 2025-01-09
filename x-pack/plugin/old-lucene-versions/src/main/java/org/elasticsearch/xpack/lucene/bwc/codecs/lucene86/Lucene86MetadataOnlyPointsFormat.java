/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene86;

import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * Allows reading metadata only from Lucene 8.6 point format
 **/
public class Lucene86MetadataOnlyPointsFormat extends PointsFormat {

    static final String DATA_CODEC_NAME = "Lucene86PointsFormatData";
    static final String META_CODEC_NAME = "Lucene86PointsFormatMeta";

    /** Filename extension for the leaf blocks */
    public static final String DATA_EXTENSION = "dim";

    /** Filename extension for the index per field */
    public static final String INDEX_EXTENSION = "dii";

    static final int DATA_VERSION_START = 0;
    static final int DATA_VERSION_CURRENT = DATA_VERSION_START;

    static final int INDEX_VERSION_START = 0;
    static final int INDEX_VERSION_CURRENT = INDEX_VERSION_START;

    /** Sole constructor */
    public Lucene86MetadataOnlyPointsFormat() {}

    @Override
    public PointsWriter fieldsWriter(SegmentWriteState state) {
        throw new UnsupportedOperationException("Old codecs may only be used for reading");
    }

    @Override
    public PointsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene86MetadataOnlyPointsReader(state);
    }
}

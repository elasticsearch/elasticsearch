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

/**
 * <p>This package exposes the blobstore repository used by Elasticsearch Snapshots.</p>
 *
 * <h1>Preliminaries</h1>
 *
 * <p>The {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository} forms the basis of implementations of
 * {@link org.elasticsearch.repositories.Repository} on top of a blob store. A blobstore can be used as the basis for an implementation
 * as long as it provides for GET, PUT and (except for in the case of read-only repositories) LIST operations.
 * These operations are formally defined as specified by the {@link org.elasticsearch.common.blobstore.BlobContainer} interface that
 * any {@code BlobStoreRepository} implementation must provide via its implementation of
 * {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository#getBlobContainer()}.</p>
 *
 * <p>The blob store is written to and read from by both the master node as well as the data nodes. All metadata related to the snapshots'
 * scope and health (i.e. the indices a snapshot contains, a snapshot of the cluster state, index metadata and the status of the snapshot)
 * are written by the master node.</p>
 * <p>For each shard, the data-node holding the shard's primary writes the actual data in form of the shard's segment files to the
 * repository as well as metadata about all the segment files that the repository stores for a given shard.</p>
 *
 * <p>For the specifics of how the operations on the repository are invoked during the snapshot process please refer to the documentation
 * of the {@link org.elasticsearch.snapshots} package.</p>
 *
 * <p>BlobStoreRepository maintains the following structure of blobs containing data and metadata in the blob store. The exact operations
 * executed on these blobs are explained below.</p>
 * <pre>
 * {@code
 *   STORE_ROOT
 *   |- index-N           - JSON serialized {@link org.elasticsearch.repositories.RepositoryData} containing a list of all snapshot ids
 *   |                      and the indices belonging to each snapshot, N is the generation of the file
 *   |- index.latest      - contains the numeric value of the latest generation of the index file (i.e. N from above)
 *   |- incompatible-snapshots - list of all snapshot ids that are no longer compatible with the current version of the cluster
 *   |- snap-20131010.dat - SMILE serialized {@link org.elasticsearch.snapshots.SnapshotInfo} for snapshot "20131010"
 *   |- meta-20131010.dat - SMILE serialized {@link org.elasticsearch.cluster.metadata.MetaData} for snapshot "20131010"
 *   |                      (includes only global metadata)
 *   |- snap-20131011.dat - SMILE serialized {@link org.elasticsearch.snapshots.SnapshotInfo} for snapshot "20131011"
 *   |- meta-20131011.dat - SMILE serialized {@link org.elasticsearch.cluster.metadata.MetaData} for snapshot "20131011"
 *   .....
 *   |- indices/ - data for all indices
 *      |- Ac1342-B_x/ - data for index "foo" which was assigned the unique id of Ac1342-B_x in the repository
 *      |  |- meta-20131010.dat - JSON Serialized {@link org.elasticsearch.cluster.metadata.IndexMetaData} for index "foo"
 *      |  |- 0/ - data for shard "0" of index "foo"
 *      |  |  |- __1                      \  (files with numeric names were created by older ES versions)
 *      |  |  |- __2                      |
 *      |  |  |- __VPO5oDMVT5y4Akv8T_AO_A |- files from different segments see snap-* for their mappings to real segment files
 *      |  |  |- __1gbJy18wS_2kv1qI7FgKuQ |
 *      |  |  |- __R8JvZAHlSMyMXyZc2SS8Zg /
 *      |  |  .....
 *      |  |  |- snap-20131010.dat - SMILE serialized {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot} for
 *      |  |  |                      snapshot "20131010"
 *      |  |  |- snap-20131011.dat - SMILE serialized {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot} for
 *      |  |  |                      snapshot "20131011"
 *      |  |  |- index-123         - SMILE serialized {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots} for
 *      |  |  |                      the shard
 *      |  |
 *      |  |- 1/ - data for shard "1" of index "foo"
 *      |  |  |- __1
 *      |  |  .....
 *      |  |
 *      |  |-2/
 *      |  ......
 *      |
 *      |- 1xB0D8_B3y/ - data for index "bar" which was assigned the unique id of 1xB0D8_B3y in the repository
 *      ......
 * }
 * </pre>
 *
 * <h1>Getting the Repository's RepositoryData</h1>
 *
 * <p>Loading the {@link org.elasticsearch.repositories.RepositoryData} holding a list of all snapshots as well as the mapping of indices'
 * names to their repository {@link org.elasticsearch.repositories.IndexId} by invoking
 * {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository#getRepositoryData} is implemented as follows:</p>
 * <ol>
 * <li>
 * <ol>
 * <li>The blobstore repository stores the {@code RepositoryData} in blobs named {@code /index-N} directly under the
 * repositories' root.</li>
 * <li>The blobstore also stores the most recent {@code N} as a signed 64bit long in the blob {@code /index.latest} directly
 * under the repositories' root.</li>
 * </ol>
 * </li>
 * <li>
 * <ol>
 * <li>Find the most recent {@code RepositoryData} by getting a list of all index-N blobs through listing all blobs with prefix "index-"
 * under the repository root and selecting the one with the highest value for N.</li>
 * <li>If this operation fails because the repositories' {@code BlobContainer} does not support list operations in the case of read-only
 * repositories, read the highest value of N from the the index.latest blob.</li>
 * </ol>
 * </li>
 * <li>
 * <ol>
 * <li>Use the just determined value of {@code N} and get the "/index-N" blob and deserialize the {@code RepositoryData} from it.</li>
 * <li>If no value of {@code N} could be found since neither an {@code index.latest} nor any {@code index-N} blobs exist in the repository,
 * it is assumed to be empty and {@link org.elasticsearch.repositories.RepositoryData#EMPTY} is returned.</li>
 * </ol>
 * </li>
 * </ol>
 * <h1>Creating a Snapshot</h1>
 *
 * <h2>Initializing a Snapshot in the Repository</h2>
 *
 * <p>Creating a snapshot in the repository begins by a call to {@link org.elasticsearch.repositories.Repository#initializeSnapshot} which
 * the blob store repository implements as such:</p>
 * <ol>
 * <li>Verify that no snapshot by the requested name exists.</li>
 * <li>Write a blob containing the cluster metadata to the root of the blob store repository at /meta-${snapshot-uuid}.dat</li>
 * <li>Write the metadata for each index to a blob in that index's directory at /indices/${index-snapshot-uuid}/meta-${snapshot-uuid}.dat
 * </li>
 * </ol>
 * TODO: This behavior is problematic, adjust these docs once https://github.com/elastic/elasticsearch/issues/41581 is fixed
 *
 * <h2>Writing Shard Data (Segments)</h2>
 *
 * <p>Once all the metadata was written by the snapshot initialization the snapshot process moves on to writing the actual shard data to the
 * repository by invoking {@link org.elasticsearch.repositories.Repository#snapshotShard} which is implemented as follows:</p>
 *
 * <p>Note:</p>
 * <ul>
 * <li>For each shard {@code i} in a given index its path in the blob store is located at {@code /indices/${index-snapshot-uuid}/{i}}</li>
 * <li>All the following steps on the shard's primary's data node.</li>
 * </ul>
 * <ol>
 * <li>Create the {@link org.apache.lucene.index.IndexCommit} for each shard to snapshot on the shard's primary.</li>
 * <li>List all blobs in the shard's path.</li>
 * <li>Find the {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots} blob with name "index-${N}" for the
 * highest possible value of N in the list to get the information of what segment files are already available in the blobstore.</li>
 * <li>By comparing the files in the {@code IndexCommit} and the available file list from the previous step, determine the segment files
 * that need to be written to the blob store. For each segment to be written to the blob store, the logic generates a unique name by
 * combining the segment data blob prefix "__" and a UUID and writes the segment to the blobstore.</li>
 * <li>After completing all segment writes, a blob containing a
 * {@link org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot} with name "snap-${snapshot-uuid}.dat" is written to the
 * shard's path, containing a list of all the files referenced by the snapshot as well as some metadata about the snapshot.</li>
 * <li>Once all the segments and the {@code BlobStoreIndexShardSnapshot} blob have been written, an updated
 * {@code BlobStoreIndexShardSnapshots} blob is written to the shard's path with name "index-${(N+1)}".</li>
 * </ol>
 *
 * <h2>Finalizing the Snapshot</h2>
 *
 * <p>After all primaries have finished writing the necessary segment files to the blob store in the previous step the master node moves on
 * to finalize the snapshot by invoking {@link org.elasticsearch.repositories.Repository#finalizeSnapshot}.</p>
 *
 * This method executes the following actions in order:
 * <ol>
 * <li>Write the {@link org.elasticsearch.snapshots.SnapshotInfo} blob for the given snapshot to the key {@code /snap-${snapshot-uuid}.dat}
 * directly under the repository root.</li>
 * <li>Write an updated {@code RepositoryData} blob to the key {@code /index-${N+1}} using the {@code N} derived when initializing the
 * snapshot in the first step.</li>
 * <li>Write the updated {@code /index.latest} blob containing the new repository generation {@code N + 1}.</li>
 * </ol>
 *
 * <h1>Deleting a Snapshot</h1>
 *
 * <p>Deleting a snapshot is an operation that is exclusively executed on the master node that runs through the following sequence of
 * action when {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository#deleteSnapshot} is invoked:</p>
 *
 * <ol>
 * <li>Get the current {@code RepositoryData} from the latest {@code index-N} blob at the repository root.</li>
 * <li>Write an updated {@code RepositoryData} blob with the deleted snapshot removed to key {@code index-${N+1}} under the repository
 * root.</li>
 * <li>Write an updated {@code index.latest} blob containing {@code N + 1}.</li>
 * <li>Delete the global {@code MetaData} blob {@code meta-${snapshot-uuid}} stored directly under the repository root associated with the
 * snapshot as well as the {@code SnapshotInfo} blob at {@code /snap-${snapshot-uuid}.dat}.</li>
 * <li>For each index referenced by the snapshot:
 * <ol>
 * <li>Delete the snapshot's {@code IndexMetaData} at {@code /indices/${index-snapshot-uuid}/meta-${snapshot-uuid}}.</li>
 * <li>Go through all shard directories {@code /indices/${index-snapshot-uuid}/${i}} and:
 * <ol>
 * <li>Remove the {@code BlobStoreIndexShardSnapshot} blob at {@code /indices/${index-snapshot-uuid}/${i}/snap-${snapshot-uuid}.dat}.</li>
 * <li>List all blobs in the shard path {@code /indices/${index-snapshot-uuid}} and build a new {@code BlobStoreIndexShardSnapshots} from
 * the remaining {@code BlobStoreIndexShardSnapshot} blobs in the shard, then write it to the next shard generation blob at
 * {@code /indices/${index-snapshot-uuid}/${i}/index-${N+1}} (The shard's generation is determined from the list of {@code index-N} blobs
 * in the shard directory).</li>
 * <li>Delete all segment blobs (identified by having the data blob prefix {@code __}) in the shard directory that are not referenced by the
 * just written {@code BlobStoreIndexShardSnapshots}.</li>
 * </ol>
 * </li>
 * </ol>
 * </li>
 * </ol>
 * TODO: The above sequence of actions can lead to leaking files when an index completely goes out of scope. Adjust this documentation once
 *       https://github.com/elastic/elasticsearch/issues/13159 is fixed.
 */
package org.elasticsearch.repositories.blobstore;

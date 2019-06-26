/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.s3guard;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.hops.metadata.s3.entity.S3PathMeta;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.checkPathMetadata;
import static org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore.makeDirStatus;


public class NDBMetadataStore implements MetadataStore {
    public static final Logger LOG = LoggerFactory.getLogger(NDBMetadataStore.class);

    // config constants
    private String username;
    private String region;
    private String default_bucket;
    private Configuration conf;

    // NN API handle
    private ClientProtocol namenode;

    @VisibleForTesting
    static final String DESCRIPTION = "S3Guard metadata store in HopsFS NetworkDB";


    @Override
    public void initialize(FileSystem fs) throws IOException {
        Preconditions.checkArgument(fs instanceof S3AFileSystem,"NDBMetadataStore only supports S3A filesystem.");
        final S3AFileSystem s3afs = (S3AFileSystem) fs;

        // get config
        conf = s3afs.getConf();
        username = s3afs.getUsername();
        conf = s3afs.getConf();
        region = s3afs.getBucketLocation();
        default_bucket = s3afs.getBucket();

        initialize(conf);
    }

    /**
     * Performs one-time initialization of the metadata store via configuration.
     *
     * This initialization depends on the configuration object to get AWS
     * credentials, table names, etc. After initialization, this metadata store does
     * not explicitly relate to any S3 bucket, which be nonexistent.
     *
     * This is used to operate the metadata store directly beyond the scope of the
     * S3AFileSystem integration, e.g. command line tools.
     * Generally, callers should use {@link #initialize(FileSystem)}
     * with an initialized {@code S3AFileSystem} instance.
     *
     * Without a filesystem to act as a reference point, the configuration itself
     * must declare the table name and region in the
     * {@link Constants#S3GUARD_DDB_TABLE_NAME_KEY} and
     * {@link Constants#S3GUARD_DDB_REGION_KEY} respectively.
     *
     * @see #initialize(FileSystem)
     * @throws IOException if there is an error
     * @throws IllegalArgumentException if the configuration is incomplete
     */
    @Override
    public void initialize(Configuration conf) throws IOException {
        // Get Namenode address
        List<InetSocketAddress> addrs = DFSUtil.getNameNodesServiceRpcAddresses(conf);
        URI nameNodeUri = NameNode.getUri(addrs.get(0));

        // connect to namenode
        AtomicBoolean nnFallbackToSimpleAuth = new AtomicBoolean(false);
        NameNodeProxies.ProxyAndInfo<ClientProtocol> proxyInfo = NameNodeProxies.createHopsRandomStickyProxy(conf, nameNodeUri, ClientProtocol.class, nnFallbackToSimpleAuth);
        namenode = proxyInfo.getProxy();

        // table for s3_metadata in NDB should already exist
    }


    @Override
    public void delete(Path path) throws IOException {
        innerDelete(path, true);
    }

    /**
     * Inner delete option, action based on the {@code tombstone} flag.
     * No tombstone: delete the entry. Tombstone: create a tombstone entry.
     * There is no check as to whether the entry exists in the table first.
     * @param path path to delete
     * @param tombstone flag to create a tombstone marker
     * @throws IOException I/O error.
     */
    private void innerDelete(Path path, boolean tombstone) throws IOException {
        path = checkPath(path);
        URI parent_uri = path.getParent().toUri();
        String bucket = parent_uri.getHost();
        LOG.debug("Deleting from bucket {} in region {}: {}", bucket, region, path);

        // deleting nonexistent item consumes 1 write capacity; skip it
        if (path.isRoot()) {
            LOG.debug("Skip deleting root directory as it does not exist in table");
            return;
        }

        if (tombstone) {
            S3PathMeta s3path = convertPathMetadataToS3Meta(PathMetadata.tombstone(path));
            namenode.s3MetadataPutPath(s3path);
        } else {
            namenode.s3MetadataDeletePath(pathToParentKey(path.getParent()), path.getName(), bucket);
        }
    }

    @Override
    public void forgetMetadata(Path path) throws IOException {
        innerDelete(path, false);
    }

    @Override
    public void deleteSubtree(Path path) throws IOException {
        path = checkPath(path);
        String bucket = path.toUri().getHost();
        LOG.debug("Deleting subtree from bucket {}: {}", bucket, path);

        final PathMetadata meta = get(path);
        if (meta == null || meta.isDeleted()) {
            LOG.debug("Subtree path {} does not exist; this will be a no-op", path);
            return;
        }

        for (DescendantsIterator desc = new DescendantsIterator(this, meta);
             desc.hasNext();) {
            innerDelete(desc.next().getPath(), true);
        }
    }

    static String pathToParentKey(Path path) {
        Preconditions.checkArgument(path.isUriPathAbsolute());
        URI parentUri = path.toUri();
        String s =  parentUri.getPath();
        // strip trailing slash if it's not root dir
        if (s.endsWith("/") && !path.isRoot()) {
            s = s.substring(0, s.length()-1);
        }
        return s;
    }

    // equivalent method: PathMetadataDynamoDBTranslation.pathMetadataToItem()
    static S3PathMeta convertPathMetadataToS3Meta(PathMetadata meta) {
        if (meta == null) {
            return null;
        }
        // get status of the path
        final FileStatus status = meta.getFileStatus();
        Path path = status.getPath();


        // Create new s3path object
        S3PathMeta s3path = new S3PathMeta();
        if (path.isRoot()) {
            s3path.parent = path.toUri().getPath();
        } else {
            s3path.parent = path.getParent().toUri().getPath();
        }
        s3path.child = status.getPath().getName();
        s3path.bucket = path.toUri().getHost();
        Preconditions.checkArgument(!StringUtils.isEmpty(s3path.getBucket()), "Path missing bucket");
        Preconditions.checkArgument(!StringUtils.isEmpty(s3path.getParent()), "Path missing parent");

        if (status.isDirectory()) {
            s3path.isDir = true;
        } else {
            s3path.fileLength = status.getLen();
            // why isnt mod time also for dir? (DynamoDB impl only modifies it here as well)
            s3path.modTime = status.getModificationTime();
            s3path.blockSize = status.getBlockSize();
        }
        s3path.isDeleted = meta.isDeleted();
        return s3path;
    }


    /**
     * Converts a collection {@link PathMetadata} to a collection S3 or NDB items.
     * @return List<S3PathMeta>
     */
    private static List<S3PathMeta> convertPathMetadataToS3Meta(Collection<PathMetadata> metas) {
        if (metas == null) {
            return null;
        }
        List<S3PathMeta> items = new ArrayList(metas.size());
        for (PathMetadata meta : metas) {
            items.add(convertPathMetadataToS3Meta(meta));
        }
        return items;
    }

    static PathMetadata convertS3ToPathMetadata(S3PathMeta s3path, String username) throws IOException {
        // nothing found
        if (s3path == null || s3path.getBucket() == "" && s3path.getParent() == "") {
            return null;
        }

        Preconditions.checkNotNull(s3path.parent, "No parent entry in item %s", s3path);
        Preconditions.checkNotNull(s3path.bucket, "No bucket entry in item %s", s3path);
        Preconditions.checkNotNull(s3path.child, "No child entry in item %s", s3path);

        String parentStr = "/" + s3path.getBucket() + "/" + s3path.getParent();

        // Skip table version markers, which are only non-absolute paths stored.
        Path rawPath = new Path(parentStr, s3path.getChild());
        if (!rawPath.isAbsoluteAndSchemeAuthorityNull()) {
            return null;
        }

        Path parent = new Path(Constants.FS_S3A + ":/" + parentStr + "/");
        Path path = new Path(parent, s3path.getChild());

        final FileStatus fileStatus;
        if (s3path.isDir) {
            fileStatus = makeDirStatus(path, username);
        } else {
            fileStatus = new FileStatus(s3path.fileLength, false, 1, s3path.blockSize, s3path.modTime, 0, null,
                    username, username, path);
        }
        return new PathMetadata(fileStatus, Tristate.UNKNOWN, s3path.isDeleted);
    }

    @Override
    public PathMetadata get(Path path) throws IOException {
        return get(path, false);
    }

    @Override
    public PathMetadata get(Path path, boolean wantEmptyDirectoryFlag) throws IOException {
        path = checkPath(path);
        String bucket = path.toUri().getHost();
        String parent_key;
        if (path.isRoot()) {
            parent_key = pathToParentKey(path);
        } else {
            parent_key = pathToParentKey(path.getParent());
        }
        LOG.debug("Get from bucket {} in region {}: {}", bucket, region, path);

        final PathMetadata meta;
        final S3PathMeta s3path;
        if (path.isRoot()) {
            // Root does not persist in the table
            meta = new PathMetadata(makeDirStatus(path, username));
            s3path = convertPathMetadataToS3Meta(meta);
        } else {
            String name = path.getName();
            s3path = namenode.s3MetadataGetPath(parent_key, name, bucket);

            meta = convertS3ToPathMetadata(s3path, username);
            LOG.debug("Get from bucket {} in region {} returning for {}: {}", bucket, region, path, meta);
        }

        if (wantEmptyDirectoryFlag && meta != null) {
            final FileStatus status = meta.getFileStatus();
            // for directory, we query its direct children (on server) to determine isEmpty bit
            if (status.isDirectory()) {
                boolean isEmpty = namenode.s3MetadataIsDirEmpty(s3path.getParent(), s3path.getChild(), s3path.getBucket());

                // When this class has support for authoritative
                // (fully-cached) directory listings, we may also be able to answer
                // TRUE here.  Until then, we don't know if we have full listing or
                // not, thus the UNKNOWN here:
                meta.setIsEmptyDirectory(isEmpty ? Tristate.UNKNOWN : Tristate.FALSE);
            }
        }
        return meta;
    }

    /**
     * Validates a path object; it must be absolute, have a scheme, and start with s3a:// */
    private Path checkPath(Path path) {
        Preconditions.checkNotNull(path);
        Preconditions.checkArgument(path.isAbsolute(), "Path %s is not absolute", path);
        URI uri = path.toUri();
        Preconditions.checkNotNull(uri.getScheme(), "Path %s missing scheme", path);
        Preconditions.checkArgument(uri.getScheme().equals(Constants.FS_S3A),
                "Path %s scheme must be %s", path, Constants.FS_S3A);
        return path;
    }

    @Override
    public DirListingMetadata listChildren(Path path) throws IOException {
        path = checkPath(path);
        String path_children_key;
        String bucket;

        if (path.isRoot()) {
            bucket = path.toUri().getHost();
            path_children_key = "/";
        } else {
            URI parentUri = path.getParent().toUri();
            String parent = parentUri.getPath();
            String dir_name = path.getName();
            bucket = parentUri.getHost();

            // if parent is / and dir_name is foo, we dont need extra /
            if (parent.endsWith("/")) {
                path_children_key = parent + dir_name;
            // deeply nested paths
            } else {
                path_children_key = parent + "/" + dir_name;
            }
        }

        LOG.debug("Listing table {} in region {}: {}", bucket, region, path);
        List<S3PathMeta> children = namenode.s3MetadataGetPathChildren(path_children_key, bucket);

        final List<PathMetadata> metas = new ArrayList<>();
        for (S3PathMeta s3path : children) {
            PathMetadata meta = convertS3ToPathMetadata(s3path, username);
            metas.add(meta);
        }
        LOG.trace("Listing table {} in region {} for {} returning {}", bucket, region, path, metas);

        return (metas.isEmpty() && get(path) == null)
                ? null
                : new DirListingMetadata(path, metas, false);
    }

    @Override
    public void move(Collection<Path> pathsToDelete, Collection<PathMetadata> pathsToCreate) throws IOException {
        if (pathsToDelete == null && pathsToCreate == null) {
            return;
        }

        LOG.debug("Moving paths in region {}: {} paths to delete and {}" + " paths to create", region,
                pathsToDelete == null ? 0 : pathsToDelete.size(),
                pathsToCreate == null ? 0 : pathsToCreate.size());
        LOG.trace("move: pathsToDelete = {}, pathsToCreate = {}", pathsToDelete, pathsToCreate);

        // In DynamoDBMetadataStore implementation, we assume that if a path
        // exists, all its ancestors will also exist in the table.
        // Following code is to maintain this invariant by putting all ancestor
        // directories of the paths to create.
        // ancestor paths that are not explicitly added to paths to create
        Collection<PathMetadata> newItems = new ArrayList<>();
        if (pathsToCreate != null) {
            newItems.addAll(completeAncestry(pathsToCreate));
        }
        if (pathsToDelete != null) {
            for (Path meta : pathsToDelete) {
                newItems.add(PathMetadata.tombstone(meta));
            }
        }
        namenode.s3MetadataPutPaths(convertPathMetadataToS3Meta(newItems));
    }

    // build the list of all parent entries.
    private Collection<PathMetadata> completeAncestry(Collection<PathMetadata> pathsToCreate) {
        // Key on path to allow fast lookup
        Map<Path, PathMetadata> ancestry = new HashMap<>();

        for (PathMetadata meta : pathsToCreate) {
            Preconditions.checkArgument(meta != null);
            Path path = meta.getFileStatus().getPath();
            if (path.isRoot()) {
                break;
            }
            ancestry.put(path, meta);
            Path parent = path.getParent();
            while (!parent.isRoot() && !ancestry.containsKey(parent)) {
                LOG.debug("auto-create ancestor path {} for child path {}", parent, path);
                final FileStatus status = makeDirStatus(parent, username);
                ancestry.put(parent, new PathMetadata(status, Tristate.FALSE, false));
                parent = parent.getParent();
            }
        }
        return ancestry.values();
    }

    @Override
    public void put(Collection<PathMetadata> metas) throws IOException {
        LOG.debug("Saving batch to table {} in region {}", "doesnt matter - meta has bucket name per item", region);

        List<S3PathMeta> s3paths = convertPathMetadataToS3Meta(completeAncestry(metas));
        namenode.s3MetadataPutPaths(s3paths);
    }


    @Override
    public void put(PathMetadata meta) throws IOException {
        // For a deeply nested path, this method will automatically create the full
        // ancestry and save respective item in DynamoDB table.
        // So after put operation, we maintain the invariant that if a path exists,
        // all its ancestors will also exist in the table.
        // For performance purpose, we generate the full paths to put and use batch
        // write item request to save the items.
        S3PathMeta s3path = convertPathMetadataToS3Meta(meta);
        String bucket = s3path.getBucket();
        LOG.debug("Saving to bucket {} in region {}: {}", bucket, region, meta);

        namenode.s3MetadataPutPath(s3path);
    }

    @Override
    public void put(DirListingMetadata meta) throws IOException {
        String bucket = meta.getPath().toUri().getHost();
        LOG.debug("Saving to table {} in region {}: {}", bucket, region, meta);

        // directory path
        PathMetadata p = new PathMetadata(makeDirStatus(meta.getPath(), username), meta.isEmpty(), false);

        // First add any missing ancestors...
        final Collection<PathMetadata> metasToPut = fullPathsToPut(p);

        // next add all children of the directory
        metasToPut.addAll(meta.getListing());

        // Put all metas into database
        namenode.s3MetadataPutPaths(convertPathMetadataToS3Meta(metasToPut));
    }

    /**
     * Helper method to get full path of ancestors that are nonexistent in table.
     */
    private Collection<PathMetadata> fullPathsToPut(PathMetadata meta) throws IOException {
        checkPathMetadata(meta);

        final Collection<PathMetadata> metasToPut = new ArrayList<>();
        // root path is not persisted
        if (!meta.getFileStatus().getPath().isRoot()) {
            metasToPut.add(meta);
        }

        // put all its ancestors if not present; as an optimization we return at its
        // first existent ancestor
        Path path = meta.getFileStatus().getPath().getParent();

        while (path != null && !path.isRoot()) {
            PathMetadata new_meta = get(path);
            S3PathMeta s3path = convertPathMetadataToS3Meta(new_meta);

            if (!itemExists(s3path)) {
                final FileStatus status = makeDirStatus(path, username);
                metasToPut.add(new PathMetadata(status, Tristate.FALSE, false));
                path = path.getParent();
            } else {
                break;
            }
        }
        return metasToPut;
    }

    // helper method to say if an item exists in DB or not
    private boolean itemExists(S3PathMeta s3path) {
        if (s3path == null)   {  return false;  }
        if (s3path.isDeleted) {  return false;  }
        return true;
    }

    @Override
    public void destroy() throws IOException {
        namenode.s3MetadataDeleteBucket(default_bucket);
    }

    @Override
    public void prune(long modTime) throws IOException {
        int itemCount = 0;
        try {
            List<S3PathMeta> deletionBatch = new ArrayList<>(S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT);
            int delay = conf.getInt(S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_KEY, S3GUARD_DDB_BACKGROUND_SLEEP_MSEC_DEFAULT);

            // Use bucket=null to prune entire table and not just 1 bucket
            List<S3PathMeta> expired_files = namenode.s3MetadataGetExpiredFiles(modTime, default_bucket);

            for (S3PathMeta s3path : expired_files) {
                deletionBatch.add(s3path);
                itemCount++;
                if (deletionBatch.size() == S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT) {
                    Thread.sleep(delay);
                    namenode.s3MetadataDeletePaths(deletionBatch);
                    deletionBatch.clear();
                }
            }
            if (deletionBatch.size() > 0) {
                Thread.sleep(delay);
                namenode.s3MetadataDeletePaths(deletionBatch);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Pruning was interrupted");
        }
        LOG.info("Finished pruning {} items in batches of {}", itemCount, S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT);
    }

    @Override
    public Map<String, String> getDiagnostics() throws IOException {
        Map<String, String> map = new TreeMap<>();
        map.put("description", DESCRIPTION);
        map.put("region", region);
        map.put("default_bucket", default_bucket);
        map.put("username", username);

        List<InetSocketAddress> addrs = DFSUtil.getNameNodesServiceRpcAddresses(conf);
        URI nameNodeUri = NameNode.getUri(addrs.get(0));
        map.put("namenode_address", nameNodeUri.toString());

        return map;
    }

    @Override
    public void updateParameters(Map<String, String> parameters) throws IOException {
        // no params to update for NDB
    }


    @Override
    public void close() throws IOException {
    }

    public ClientProtocol getNameNodeClient() {
        return namenode;
    }
}

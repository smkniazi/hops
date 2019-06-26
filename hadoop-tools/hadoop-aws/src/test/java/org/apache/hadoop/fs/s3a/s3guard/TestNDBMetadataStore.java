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

import com.google.common.collect.Lists;
import io.hops.metadata.s3.entity.S3PathMeta;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.fs.s3a.Constants.*;

public class TestNDBMetadataStore extends MetadataStoreTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(TestNDBMetadataStore.class);
    private static final String BUCKET = "TestNDBMetadataStore";
    private static final String S3URI = URI.create(FS_S3A + "://" + BUCKET + "/").toString();

    /** NameNode client instance that can issue requests directly to server. */
    private static ClientProtocol namenode;
    private static MiniDFSCluster cluster;

    private static Configuration test_conf = new Configuration();

    /**
     * Start the NDB and initializes s3 file system.
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        cluster = new MiniDFSCluster.Builder(test_conf).numDataNodes(0).build();
        cluster.waitActive();
        namenode = new TestNDBMetadataStore.NDBContract().getMetadataStore().getNameNodeClient();
    }
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (namenode != null) {

        }
    }

    /**
     * Each contract has its own S3AFileSystem and DynamoDBMetadataStore objects.
     */
    private static class NDBContract extends AbstractMSContract {
        private final S3AFileSystem s3afs;
        private final NDBMetadataStore ms = new NDBMetadataStore();

        NDBContract() throws IOException {
            this(test_conf);
        }

        // This gets called EVERY new test run
        NDBContract(Configuration conf) throws IOException {
            // using mocked S3 clients
            conf.setClass(S3_CLIENT_FACTORY_IMPL, MockS3ClientFactory.class, S3ClientFactory.class);
            // default protocol is s3a://
            conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, S3URI);

            // table should already exist
            conf.setBoolean(S3GUARD_DDB_TABLE_CREATE_KEY, false);

            // always create new file system object for a test contract
            s3afs = (S3AFileSystem) FileSystem.newInstance(conf);
            ms.initialize(s3afs);

            // clear the DB for every test
            ms.getNameNodeClient().s3MetadataDeleteBucket(BUCKET);
        }

        @Override
        public FileSystem getFileSystem() {
            return s3afs;
        }

        @Override
        public NDBMetadataStore getMetadataStore() {
            return ms;
        }
    }

    @Override
    public AbstractMSContract createContract() throws IOException {
        return new NDBContract();
    }

    @Override
    public AbstractMSContract createContract(Configuration conf) throws IOException {
        // not used and doesnt work because the conf needs to contain HDFS cluster info created in setUpBeforeClass()
        return null;
    }

    @Test
    public void initialize() throws IOException {
        final FileSystem fs = getFileSystem();
        final Configuration conf = fs.getConf();

        try (NDBMetadataStore ndbms = new NDBMetadataStore()) {
            ndbms.initialize(conf);
            assertNotNull(ndbms.getNameNodeClient());
        }
    }

    @Test
    public void put_get_server_directly() {
        try {
            S3PathMeta s3path = new S3PathMeta("/", "bar4", BUCKET, false, false, 123, 123);
            cluster.getNameNode().getNamesystem().s3MetadataPutPath(s3path);

            S3PathMeta s3path_new = cluster.getNameNode().getNamesystem().s3MetadataGetPath(s3path.getParent(), s3path.getChild(), BUCKET);

            assertEquals( s3path.getParent(), s3path_new.getParent());
            assertEquals( s3path.getChild(), s3path_new.getChild());
            assertEquals( s3path.getBucket(), s3path_new.getBucket());

        } catch (IOException err) {
            fail(err.getMessage());
        }
    }

    @Test
    public void getExpiredFiles() {
        try {
            String expired_bucket = "Test_expired_files";
            S3PathMeta dir = new S3PathMeta("/expired", "", expired_bucket, false, true);
            namenode.s3MetadataPutPath(dir);
            S3PathMeta s3path = new S3PathMeta("/expired", "bread", expired_bucket, false, false, 2212, 222);
            namenode.s3MetadataPutPath(s3path);
            long start_time = System.currentTimeMillis();

            S3PathMeta dir2 = new S3PathMeta("/new", "", expired_bucket, false, true);
            namenode.s3MetadataPutPath(dir);
            S3PathMeta s3path2 = new S3PathMeta("/new", "kessel", expired_bucket, false, false, 2212, 2);
            namenode.s3MetadataPutPath(s3path2);
            S3PathMeta s3path3 = new S3PathMeta("/new", "spark", expired_bucket, false, false, 2212, 2);
            namenode.s3MetadataPutPath(s3path3);

            List<S3PathMeta> expired_files = namenode.s3MetadataGetExpiredFiles(start_time, expired_bucket);
            assertEquals(expired_files.size(), 2);

        } catch (IOException err) {
            fail(err.getMessage());
        }
    }

    private S3AFileSystem getFileSystem() throws IOException {
        return (S3AFileSystem) getContract().getFileSystem();
    }


    private NDBMetadataStore getNDBMetadataStore() throws IOException {
        return (NDBMetadataStore) getContract().getMetadataStore();
    }

    @Test
    public void put_get() {
        try {
            S3PathMeta s3path_orig = new S3PathMeta("foo", "bar3", BUCKET, false, false, 123, 123);
            namenode.s3MetadataPutPath(s3path_orig);

            S3PathMeta s3path_new =  namenode.s3MetadataGetPath(s3path_orig.getParent(), s3path_orig.getChild(), BUCKET);
            System.out.println("Got new s3meta from server:" + s3path_new.getParent() + "/" + s3path_new.getChild());

            assertEquals(s3path_orig.getParent(), s3path_new.getParent());
            assertEquals(s3path_orig.getChild(), s3path_new.getChild());
            assertEquals(s3path_orig.getBucket(), s3path_new.getBucket());
            assertEquals(s3path_orig.getBlockSize(), s3path_new.getBlockSize());
            assertEquals(s3path_orig.getFileLength(), s3path_new.getFileLength());
            assertEquals(s3path_orig.getModTime(), s3path_new.getModTime());
            assertEquals(s3path_orig.isDeleted(), s3path_new.isDeleted());
            assertEquals(s3path_orig.isDir(), s3path_new.isDir());

        } catch (IOException err) {
            fail(err.getMessage());
        }
    }

    @Test
    public void listChildren() {
        try {
            S3PathMeta dir = new S3PathMeta("/", "bananas", BUCKET, false, true);
            cluster.getNameNode().getNamesystem().s3MetadataPutPath(dir);
            S3PathMeta s3path = new S3PathMeta("/bananas", "monkey", BUCKET, false, false, 2212, 222);
            cluster.getNameNode().getNamesystem().s3MetadataPutPath(s3path);
            S3PathMeta s3path2 = new S3PathMeta("/bananas", "monkey2", BUCKET, false, false, 2212, 2);
            cluster.getNameNode().getNamesystem().s3MetadataPutPath(s3path2);

            List<S3PathMeta> children = namenode.s3MetadataGetPathChildren("/bananas", BUCKET);
            System.out.println(children);
            assertEquals(2, children.size());
        } catch (IOException err) {
            fail(err.getMessage());
        }
    }

    @Override
    FileStatus basicFileStatus(Path path, int size, boolean isDir) throws IOException {
        String owner = UserGroupInformation.getCurrentUser().getShortUserName();
        return isDir
                ? new S3AFileStatus(true, path, owner)
                : new S3AFileStatus(size, getModTime(), path, BLOCK_SIZE, owner);
    }

    /**
     * Test that for a large batch write request, the limit is handled correctly.
     */
    @Test
    public void testBatchWrite() throws IOException {
        final int[] numMetasToDeleteOrPut = {
                -1, // null
                0, // empty collection
                1, // one path
                S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT, // exact limit of a batch request
                S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT + 1 // limit + 1
        };
        for (int numOldMetas : numMetasToDeleteOrPut) {
            for (int numNewMetas : numMetasToDeleteOrPut) {
                doTestBatchWrite(numOldMetas, numNewMetas);
            }
        }
    }

    private void doTestBatchWrite(int numDelete, int numPut) throws IOException {
        final String root = S3URI + "/testBatchWrite_" + numDelete + '_' + numPut;
        final Path oldDir = new Path(root, "oldDir");
        final Path newDir = new Path(root, "newDir");
        LOG.info("doTestBatchWrite: oldDir={}, newDir={}", oldDir, newDir);

        final NDBMetadataStore ms = getNDBMetadataStore();
        ms.put(new PathMetadata(basicFileStatus(oldDir, 0, true)));
        ms.put(new PathMetadata(basicFileStatus(newDir, 0, true)));

        final List<PathMetadata> oldMetas =
                numDelete < 0 ? null : new ArrayList<PathMetadata>(numDelete);
        for (int i = 0; i < numDelete; i++) {
            oldMetas.add(new PathMetadata(
                    basicFileStatus(new Path(oldDir, "child" + i), i, true)));
        }
        final List<PathMetadata> newMetas =
                numPut < 0 ? null : new ArrayList<PathMetadata>(numPut);
        for (int i = 0; i < numPut; i++) {
            newMetas.add(new PathMetadata(
                    basicFileStatus(new Path(newDir, "child" + i), i, false)));
        }

        Collection<Path> pathsToDelete = null;
        if (oldMetas != null) {
            // put all metadata of old paths and verify
            ms.put(new DirListingMetadata(oldDir, oldMetas, false));
            assertEquals(0, ms.listChildren(newDir).withoutTombstones().numEntries());
            assertTrue(CollectionUtils.isEqualCollection(oldMetas,
                    ms.listChildren(oldDir).getListing()));

            pathsToDelete = new ArrayList<>(oldMetas.size());
            for (PathMetadata meta : oldMetas) {
                pathsToDelete.add(meta.getFileStatus().getPath());
            }
        }

        // move the old paths to new paths and verify
        ms.move(pathsToDelete, newMetas);
        assertEquals(0, ms.listChildren(oldDir).withoutTombstones().numEntries());
        if (newMetas != null) {
            assertTrue(CollectionUtils.isEqualCollection(newMetas,
                    ms.listChildren(newDir).getListing()));
        }
    }

    /**
     * Test that when moving nested paths, all its ancestors up to destination
     * root will also be created.
     * Here is the directory tree before move:
     * <pre>
     * testMovePopulateAncestors
     * ├── a
     * │   └── b
     * │       └── src
     * │           ├── dir1
     * │           │   └── dir2
     * │           └── file1.txt
     * └── c
     *     └── d
     *         └── dest
     *</pre>
     * As part of rename(a/b/src, d/c/dest), S3A will enumerate the subtree at
     * a/b/src.  This test verifies that after the move, the new subtree at
     * 'dest' is reachable from the root (i.e. c/ and c/d exist in the table.
     * DynamoDBMetadataStore depends on this property to do recursive delete
     * without a full table scan.
     */
    @Test
    public void testMovePopulatesAncestors() throws IOException {
        final NDBMetadataStore ndbms = getNDBMetadataStore();
        final String testRoot = "/testMovePopulatesAncestors";
        final String srcRoot = testRoot + "/a/b/src";
        final String destRoot = testRoot + "/c/d/e/dest";

        final Path nestedPath1 = strToPath(srcRoot + "/file1.txt");
        ndbms.put(new PathMetadata(basicFileStatus(nestedPath1, 1024, false)));

        final Path nestedPath2 = strToPath(srcRoot + "/dir1/dir2");
        ndbms.put(new PathMetadata(basicFileStatus(nestedPath2, 0, true)));

        // We don't put the destRoot path here, since put() would create ancestor
        // entries, and we want to ensure that move() does it, instead.

        // Build enumeration of src / dest paths and do the move()
        final Collection<Path> fullSourcePaths = Lists.newArrayList(
                strToPath(srcRoot),
                strToPath(srcRoot + "/dir1"),
                strToPath(srcRoot + "/dir1/dir2"),
                strToPath(srcRoot + "/file1.txt")
        );
        final Collection<PathMetadata> pathsToCreate = Lists.newArrayList(
                new PathMetadata(basicFileStatus(strToPath(destRoot),
                        0, true)),
                new PathMetadata(basicFileStatus(strToPath(destRoot + "/dir1"),
                        0, true)),
                new PathMetadata(basicFileStatus(strToPath(destRoot + "/dir1/dir2"),
                        0, true)),
                new PathMetadata(basicFileStatus(strToPath(destRoot + "/file1.txt"),
                        1024, false))
        );

        ndbms.move(fullSourcePaths, pathsToCreate);

        // assert that all the ancestors should have been populated automatically
        assertCached(testRoot + "/c");
        assertCached(testRoot + "/c/d");
        assertCached(testRoot + "/c/d/e");
        assertCached(destRoot /* /c/d/e/dest */);

        // Also check moved files while we're at it
        assertCached(destRoot + "/dir1");
        assertCached(destRoot + "/dir1/dir2");
        assertCached(destRoot + "/file1.txt");
    }

    @Test
    public void testIsRootEmpty() {
        try {
            boolean rootEmpty = namenode.s3MetadataIsDirEmpty("/", "", BUCKET);

            assertTrue(rootEmpty);
        } catch (IOException err) {
            fail(err.getMessage());
        }
    }

    /**
     * Test cases about root directory as it is not in the NDB table.
     */
    @Test
    public void testRootDirectory() throws IOException {
        final NDBMetadataStore ndbms = getNDBMetadataStore();
        Path rootPath = new Path(S3URI);
        verifyRootDirectory(ndbms.get(rootPath), true);

        ndbms.put(new PathMetadata(new S3AFileStatus(true,
                new Path(rootPath, "sudo"),
                UserGroupInformation.getCurrentUser().getShortUserName())));
        verifyRootDirectory(ndbms.get(new Path(S3URI)), false);
    }

    private void verifyRootDirectory(PathMetadata rootMeta, boolean isEmpty) {
        assertNotNull(rootMeta);
        final FileStatus status = rootMeta.getFileStatus();
        assertNotNull(status);
        assertTrue(status.isDirectory());
        // UNKNOWN is always a valid option, but true / false should not contradict
        if (isEmpty) {
            assertNotSame("Should not be marked non-empty",
                    Tristate.FALSE,
                    rootMeta.isEmptyDirectory());
        } else {
            assertNotSame("Should not be marked empty",
                    Tristate.TRUE,
                    rootMeta.isEmptyDirectory());
        }
    }

    @Test
    public void close() {
        // Functionality not used by NDB
        assertTrue(true);
    }
}
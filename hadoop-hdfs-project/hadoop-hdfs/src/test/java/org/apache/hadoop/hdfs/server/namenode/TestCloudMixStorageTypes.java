/*
 * Copyright (C) 2019 LogicalClocks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CloudProvider;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;

import java.io.IOException;

import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.hdfs.HopsFilesTestHelper.*;
import static org.junit.Assert.fail;

public class TestCloudMixStorageTypes {

  static final Log LOG = LogFactory.getLog(TestCloudMixStorageTypes.class);

  @Rule
  public TestName testname = new TestName();

  @Before
  public void setup() {
    //Logger.getLogger(ProvidedBlocksChecker.class).setLevel(Level.DEBUG);
    //Logger.getLogger(CloudTestHelper.class).setLevel(Level.DEBUG);
  }

  @Test
  public void TestNestedCloudAndDBTypes() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 5;
      final int BLKSIZE = 128 * 1024;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      final int ONDISK_SMALL_BUCKET_SIZE = FSNamesystem.getDBOnDiskSmallBucketSize();
      final int ONDISK_MEDIUM_BUCKET_SIZE = FSNamesystem.getDBOnDiskMediumBucketSize();
      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/"), "CLOUD");
      dfs.setStoragePolicy(new Path("/dir"), "DB");

      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";
      final String FILE_NAME3 = "/dir/TEST-FLIE3";
      final String FILE_NAME4 = "/dir/TEST-FLIE4";
      final String FILE_NAME5 = "/dir/TEST-FLIE5";
      final String FILE_NAME6 = "/dir/TEST-FLIE6";

      //write small files
      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME2, ONDISK_SMALL_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME2, ONDISK_SMALL_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME3, ONDISK_MEDIUM_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME3, ONDISK_MEDIUM_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME4, MAX_SMALL_FILE_SIZE);
      verifyFile(dfs, FILE_NAME4, MAX_SMALL_FILE_SIZE);

      //now write large files. these should be stored in the cloud
      writeFile(dfs, FILE_NAME5, MAX_SMALL_FILE_SIZE + 1); // 1 block
      verifyFile(dfs, FILE_NAME5, MAX_SMALL_FILE_SIZE + 1);
      writeFile(dfs, FILE_NAME6, 10 * BLKSIZE);  // 10 blocks
      verifyFile(dfs, FILE_NAME6, 10 * BLKSIZE);
      assert CloudTestHelper.findAllBlocks().size() == 11;  // 11 blocks so far

      //validate
      assertTrue("Expecting 1 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 3 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 3);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);


    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void TestHotStorage() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 1;
      final int BLKSIZE = 128 * 1024;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/"), "CLOUD");
      dfs.setStoragePolicy(new Path("/dir"), "HOT");

      final String FILE_NAME1 = "/dir/TEST-FLIE1";

      writeFile(dfs, FILE_NAME1, BLKSIZE*10); //replication is 1
      verifyFile(dfs, FILE_NAME1, BLKSIZE*10);

      assert CloudTestHelper.findAllBlocks().size() == 10;
      //stored on disk. Expecting 10 replicas
      assert CloudTestHelper.findAllReplicas().size() == 10;

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  // store small files in DB and large files in cloud
  @Test
  public void TestSmallAndLargeFiles() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 5;
      final int BLKSIZE = 128 * 1024;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_AWS_S3_NUM_BUCKETS, 2);
      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      final int ONDISK_SMALL_BUCKET_SIZE = FSNamesystem.getDBOnDiskSmallBucketSize();
      final int ONDISK_MEDIUM_BUCKET_SIZE = FSNamesystem.getDBOnDiskMediumBucketSize();
      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");
      dfs.mkdirs(new Path("/dir2"));
      dfs.setStoragePolicy(new Path("/dir2"), "DB");

      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";
      final String FILE_NAME3 = "/dir/TEST-FLIE3";
      final String FILE_NAME4 = "/dir/TEST-FLIE4";
      final String FILE_NAME5 = "/dir/TEST-FLIE5";
      final String FILE_NAME6 = "/dir/TEST-FLIE6";

      //write small files
      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME2, ONDISK_SMALL_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME2, ONDISK_SMALL_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME3, ONDISK_MEDIUM_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME3, ONDISK_MEDIUM_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME4, MAX_SMALL_FILE_SIZE);
      verifyFile(dfs, FILE_NAME4, MAX_SMALL_FILE_SIZE);

      //validate
      assertTrue("Expecting 1 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 3 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 3);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);


      //now write large files. these should be stored in the cloud
      writeFile(dfs, FILE_NAME5, MAX_SMALL_FILE_SIZE + 1); // 1 block
      verifyFile(dfs, FILE_NAME5, MAX_SMALL_FILE_SIZE + 1);

      writeFile(dfs, FILE_NAME6, 10 * BLKSIZE);  // 10 blocks
      verifyFile(dfs, FILE_NAME6, 10 * BLKSIZE);
      assert CloudTestHelper.findAllBlocks().size() == 11;  // 11 blocks so far
      CloudTestHelper.matchMetadata(conf);

      final String FILE_NAME7 = "/dir2/TEST-FLIE7";
      final String FILE_NAME8 = "/dir2/TEST-FLIE8";
      final String FILE_NAME9 = "/dir2/TEST-FLIE9";

      writeFile(dfs, FILE_NAME7, MAX_SMALL_FILE_SIZE);  // goes to DB
      verifyFile(dfs, FILE_NAME7, MAX_SMALL_FILE_SIZE);

      writeFile(dfs, FILE_NAME8, MAX_SMALL_FILE_SIZE + 1);  // goes to DNs disks. 1 blk
      verifyFile(dfs, FILE_NAME8, MAX_SMALL_FILE_SIZE + 1);

      writeFile(dfs, FILE_NAME9, BLKSIZE * 10); // goes to DNs disks. 10 blks. total of 22 blks so far
      verifyFile(dfs, FILE_NAME9, BLKSIZE * 10);

      assert CloudTestHelper.findAllBlocks().size() == 22;
//      assert CloudTestHelper.getAllCloudBlocks(cloud).size() == 11;


      assertTrue("Expecting 1 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 4 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 4);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 2 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 2);

      dfs.delete(new Path("/dir"), true);
      dfs.delete(new Path("/dir2"), true);
      Thread.sleep(20000);
      CloudTestHelper.matchMetadata(conf);
      assert CloudTestHelper.findAllBlocks().size() == 0;


    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  // test disabling small files support for cloud storage policy
  @Test
  public void TestDisableSmallFiles() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int NUM_DN = 5;
      final int BLKSIZE = 128 * 1024;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_AWS_S3_NUM_BUCKETS, 2);
      conf.setBoolean(DFSConfigKeys.DFS_CLOUD_STORE_SMALL_FILES_IN_DB_KEY, false);
      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      final int ONDISK_SMALL_BUCKET_SIZE = FSNamesystem.getDBOnDiskSmallBucketSize();
      final int ONDISK_MEDIUM_BUCKET_SIZE = FSNamesystem.getDBOnDiskMediumBucketSize();
      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();
      final int INMEMORY_BUCKET_SIZE = FSNamesystem.getDBInMemBucketSize();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";
      final String FILE_NAME3 = "/dir/TEST-FLIE3";
      final String FILE_NAME4 = "/dir/TEST-FLIE4";

      //write small files
      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME2, ONDISK_SMALL_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME2, ONDISK_SMALL_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME3, ONDISK_MEDIUM_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME3, ONDISK_MEDIUM_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME4, MAX_SMALL_FILE_SIZE);
      verifyFile(dfs, FILE_NAME4, MAX_SMALL_FILE_SIZE);

      //validate
      assertTrue("Expecting 0 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 0);

      CloudTestHelper.matchMetadata(conf);
      assert CloudTestHelper.findAllBlocks().size() == 4;

      //However, the DB policy should work

      dfs.mkdirs(new Path("/dir2"));
      dfs.setStoragePolicy(new Path("/dir2"), "DB");

      final String FILE_NAME5 = "/dir2/TEST-FLIE1";
      final String FILE_NAME6 = "/dir2/TEST-FLIE2";
      final String FILE_NAME7 = "/dir2/TEST-FLIE3";
      final String FILE_NAME8 = "/dir2/TEST-FLIE4";

      //write small files
      writeFile(dfs, FILE_NAME5, INMEMORY_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME5, INMEMORY_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME6, ONDISK_SMALL_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME6, ONDISK_SMALL_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME7, ONDISK_MEDIUM_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME7, ONDISK_MEDIUM_BUCKET_SIZE);
      writeFile(dfs, FILE_NAME8, MAX_SMALL_FILE_SIZE);
      verifyFile(dfs, FILE_NAME8, MAX_SMALL_FILE_SIZE);

      //validate
      assertTrue("Expecting 1 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 3 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 3);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  @AfterClass
  public static void TestZDeleteAllBuckets() throws IOException {
    CloudTestHelper.purgeS3();
  }

}

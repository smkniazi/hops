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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.log4j.Level;
import org.junit.*;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.hdfs.HopsFilesTestHelper.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

public class TestCloudFileCreation {

  static final Log LOG = LogFactory.getLog(TestCloudFileCreation.class);
  @Rule
  public TestName testname = new TestName();

  @BeforeClass
  public static void setBucketPrefix(){
    CloudTestHelper.prependBucketPrefix("TCFC");
  }


  /**
   * Simple read and write test
   *
   * @throws IOException
   */
  @Test
  public void TestSimpleReadAndWrite() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {

      final int BLKSIZE = 128 * 1024;
      final int FILESIZE = 2 * BLKSIZE;

      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final int NUM_DN = 3;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      CloudTestHelper.setRandomBucketPrefix(conf,testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      writeFile(dfs, FILE_NAME1, FILESIZE);
      verifyFile(dfs, FILE_NAME1, FILESIZE);

      CloudTestHelper.matchMetadata(conf);

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
  public void TestListing() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      final int BLK_SIZE = 128 * 1024;
      final int BLK_PER_FILE = 3;
      final int FILESIZE = BLK_PER_FILE * BLK_SIZE;
      final int NUM_FILES = 1;
      final int NUM_DN = 5;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);
      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      for (int i = 0; i < NUM_FILES; i++) {
        writeFile(dfs, "/dir/file" + i, FILESIZE);
      }

      CloudTestHelper.matchMetadata(conf);

      FileStatus[] filesStatus = dfs.listStatus(new Path("/dir"));

      assert filesStatus.length == NUM_FILES;

      RemoteIterator<LocatedFileStatus> locatedFilesStatus = dfs.listLocatedStatus(new Path("/dir"));

      int count = 0;
      while (locatedFilesStatus.hasNext()) {
        LocatedFileStatus locFileStatus = locatedFilesStatus.next();

        assert locFileStatus.getBlockLocations().length == BLK_PER_FILE;

        count += locFileStatus.getBlockLocations().length;
      }

      assert count == NUM_FILES * BLK_PER_FILE;


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
  public void TestDeleteFile() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      final int BLK_SIZE = 128 * 1024;
      final int BLK_PER_FILE = 3;
      final int FILESIZE = BLK_PER_FILE * BLK_SIZE;
      final int NUM_DN = 5;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);
      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      writeFile(dfs, "/dir/file", FILESIZE);

      CloudTestHelper.matchMetadata(conf);

      dfs.delete(new Path("/dir/file"), true);

      Thread.sleep(10000);

      CloudTestHelper.matchMetadata(conf);
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
  public void TestDeleteDir() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLK_SIZE = 128 * 1024;
      final int BLK_PER_FILE = 3;
      final int FILESIZE = BLK_PER_FILE * BLK_SIZE;
      final int NUM_DN = 5;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);
      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.mkdirs(new Path("/dir/dir1"));
      dfs.mkdirs(new Path("/dir/dir2"));

      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      for (int i = 0; i < 5; i++) {
        String file = "/dir/dir1/dir-dir1-file-" + i;
        writeFile(dfs, file, FILESIZE);
        verifyFile(dfs, file, FILESIZE);
      }

      for (int i = 0; i < 5; i++) {
        String file = "/dir/dir2/dir-dir2-file-" + i;
        writeFile(dfs, file, FILESIZE);
        verifyFile(dfs, file, FILESIZE);
      }

      CloudTestHelper.matchMetadata(conf);
      assert CloudTestHelper.findAllBlocks().size() == 10 * 3;

      dfs.delete(new Path("/dir"), true);

      // sleep to make sure that the objects from the cloud storage
      // have been deleted
      Thread.sleep(20000);

      CloudTestHelper.matchMetadata(conf);

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
  public void TestRename() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLK_SIZE = 128 * 1024;
      final int BLK_PER_FILE = 3;
      final int FILESIZE = BLK_PER_FILE * BLK_SIZE;
      final int NUM_DN = 5;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);
      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));

      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      for (int i = 0; i < 5; i++) {
        writeFile(dfs, "/dir/file-" + i, FILESIZE);
      }

      CloudTestHelper.matchMetadata(conf);
      assert CloudTestHelper.findAllBlocks().size() == 5 * 3;

      for (int i = 0; i < 5; i++) {
        dfs.rename(new Path("/dir/file-" + i), new Path("/dir/file-new-" + i));
      }

      for (int i = 0; i < 5; i++) {
        verifyFile(dfs, "/dir/file-new-" + i, FILESIZE);
      }

      CloudTestHelper.matchMetadata(conf);
      assert CloudTestHelper.findAllBlocks().size() == 5 * 3;

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
  public void TestOverWrite() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLK_SIZE = 128 * 1024;
      final int BLK_PER_FILE = 3;
      final int FILESIZE = BLK_PER_FILE * BLK_SIZE;
      final int NUM_DN = 5;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);
      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));

      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      writeFile(dfs, "/dir/file1", FILESIZE);

      CloudTestHelper.matchMetadata(conf);
      assert CloudTestHelper.findAllBlocks().size() == BLK_PER_FILE;

      // now overrite this file
      writeFile(dfs, "/dir/file1", FILESIZE);
      assert CloudTestHelper.findAllBlocks().size() == BLK_PER_FILE;

      Thread.sleep(20000); //wait so that cloud bocks are deleted

      CloudTestHelper.matchMetadata(conf);

      writeFile(dfs, "/dir/file2", FILESIZE);

      assert CloudTestHelper.findAllBlocks().size() == 2 * BLK_PER_FILE;
      CloudTestHelper.matchMetadata(conf);

      dfs.rename(new Path("/dir/file1"), new Path("/dir/file2"), Options.Rename.OVERWRITE);
      assert CloudTestHelper.findAllBlocks().size() == 1 * BLK_PER_FILE;

      Thread.sleep(20000); //wait so that cloud bocks are deleted
      CloudTestHelper.matchMetadata(conf);

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
  public void TestAppend() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLK_SIZE = 128 * 1024;
      final int BLK_PER_FILE = 1;
      final int FILESIZE = BLK_PER_FILE * BLK_SIZE;
      final int NUM_DN = 5;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);
      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).
              storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));

      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      int initialSize = FSNamesystem.getMaxSmallFileSize() + 1;
      writeFile(dfs, "/dir/file1", initialSize);  // write to cloud

      CloudTestHelper.matchMetadata(conf);

      final int APPEND_SIZE = 512;
      final int TIMES = 10;
      //append 1 byte ten times
      for (int i = 0; i < TIMES; i++) {
        FSDataOutputStream out = dfs.append(new Path("/dir/file1"));
        HopsFilesTestHelper.writeData(out, initialSize + APPEND_SIZE * i, APPEND_SIZE);
        out.close();
      }

      verifyFile(dfs, "/dir/file1", initialSize + APPEND_SIZE * TIMES);

      CloudTestHelper.matchMetadata(conf);
      assert CloudTestHelper.findAllBlocks().size() == TIMES + 1;

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  appending to a file stored in DB.
   */
  @Test
  public void TestAppend2() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final int BLK_SIZE = 128 * 1024;
      final int BLK_PER_FILE = 1;
      final int NUM_DN = 5;

      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);
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

      String FILE_NAME1 = "/dir/file";
      writeFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);
      verifyFile(dfs, FILE_NAME1, INMEMORY_BUCKET_SIZE);

      //validate
      assertTrue("Expecting 1 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 1);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 0);
      CloudTestHelper.matchMetadata(conf);

      FSDataOutputStream out = dfs.append(new Path(FILE_NAME1));
      writeData(out, INMEMORY_BUCKET_SIZE, ONDISK_SMALL_BUCKET_SIZE - INMEMORY_BUCKET_SIZE);
      out.close();
      verifyFile(dfs, FILE_NAME1, ONDISK_SMALL_BUCKET_SIZE);

      //validate
      assertTrue("Expecting 0 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 0);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 1);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 1);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 0);
      CloudTestHelper.matchMetadata(conf);

      out = dfs.append(new Path(FILE_NAME1));
      writeData(out, ONDISK_SMALL_BUCKET_SIZE, ONDISK_MEDIUM_BUCKET_SIZE - ONDISK_SMALL_BUCKET_SIZE);
      out.close();
      verifyFile(dfs, FILE_NAME1, ONDISK_MEDIUM_BUCKET_SIZE);
      //validate
      assertTrue("Expecting 0 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 0);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 1);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 0);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 1);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 0);
      CloudTestHelper.matchMetadata(conf);

      out = dfs.append(new Path(FILE_NAME1));
      writeData(out, ONDISK_MEDIUM_BUCKET_SIZE, MAX_SMALL_FILE_SIZE - ONDISK_MEDIUM_BUCKET_SIZE);
      out.close();
      verifyFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE);
      //validate
      assertTrue("Expecting 0 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 0);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 1);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 0);
      assertTrue("Expecting 1 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 1);
      CloudTestHelper.matchMetadata(conf);


      out = dfs.append(new Path(FILE_NAME1));
      writeData(out, MAX_SMALL_FILE_SIZE, 1);
      out.close();
      verifyFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE+1);
      //validate
      assertTrue("Expecting 0 in-memory file. Got: " + countInMemoryDBFiles(), countInMemoryDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countAllOnDiskDBFiles(), countAllOnDiskDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskSmallDBFiles(), countOnDiskSmallDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskMediumDBFiles(), countOnDiskMediumDBFiles() == 0);
      assertTrue("Expecting 0 on-disk file(s). Got:" + countOnDiskLargeDBFiles(), countOnDiskLargeDBFiles() == 0);
      CloudTestHelper.matchMetadata(conf);
      assert CloudTestHelper.findAllBlocks().size()==1;

      out = dfs.append(new Path(FILE_NAME1));
      writeData(out, MAX_SMALL_FILE_SIZE+1, 1);
      out.close();
      verifyFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE+2);
      CloudTestHelper.matchMetadata(conf);
      assert CloudTestHelper.findAllBlocks().size()==2;


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
  public void TestSetReplication() throws IOException {
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

      final int MAX_SMALL_FILE_SIZE = FSNamesystem.getMaxSmallFileSize();

      final int S = FSNamesystem.getDBInMemBucketSize();
      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final String FILE_NAME2 = "/dir/TEST-FLIE2";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      writeFile(dfs, FILE_NAME1, MAX_SMALL_FILE_SIZE);
      writeFile(dfs, FILE_NAME2, BLKSIZE);

      assertTrue("Count of db file should be 1", countAllOnDiskDBFiles() == 1);

      dfs.setReplication(new Path(FILE_NAME1), (short) 10);
      dfs.setReplication(new Path(FILE_NAME2), (short) 10);

      if (dfs.getFileStatus(new Path(FILE_NAME1)).getReplication() != 10) {
        fail("Unable to set replication for a small file");
      }

      if (dfs.getFileStatus(new Path(FILE_NAME2)).getReplication() != 10) {
        fail("Unable to set replication for a large file");
      }

      assert CloudTestHelper.matchMetadata(conf);

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
  public void TestSmallFileHsync() throws IOException {
    CloudTestHelper.purgeS3();
    TestHsyncORFlush(true);
  }

  @Test
  public void TestSmallFileHflush() throws IOException {
    CloudTestHelper.purgeS3();
    TestHsyncORFlush(false);
  }

  public void TestHsyncORFlush(boolean hsync) throws IOException {
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

      final String FILE_NAME = "/dir/TEST-FLIE";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      final int TIMES = 10;
      final int SYNC_SIZE = 1024;
      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);
      HopsFilesTestHelper.writeData(out, 0, SYNC_SIZE);

      for (int i = 1; i <= TIMES; i++) {
        if (hsync) {
          out.hsync();   // syncs and closes a block
        } else {
          out.hflush();
        }
        HopsFilesTestHelper.writeData(out, i * SYNC_SIZE, 1024);  //written to new block
      }
      out.close();

      verifyFile(dfs, FILE_NAME, SYNC_SIZE * (TIMES + 1));

      assert CloudTestHelper.matchMetadata(conf);
      assert CloudTestHelper.findAllBlocks().size() == TIMES + 1;

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
  public void TestDataRace() throws IOException {
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

      final FSNamesystem fsNamesystem = cluster.getNameNode().getNamesystem();
      final FSNamesystem fsNamesystemSpy = Mockito.spy(fsNamesystem);
      NameNodeRpcServer rpcServer = (NameNodeRpcServer) cluster.getNameNode().getRpcServer();
      rpcServer.setFSNamesystem(fsNamesystemSpy);

      Answer delayer = new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          LOG.info("Delaying the FSYNC a bit to create a race condition");
          Thread.sleep(2000);
          return invocationOnMock.callRealMethod();
        }
      };

      Mockito.doAnswer(delayer).when(fsNamesystemSpy).fsync(anyString(), anyLong(), anyString(),
              anyLong());

      final String FILE_NAME = "/dir/TEST-FLIE";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");


      final int TIMES = 20;
      final int SYNC_SIZE = 1024;
      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);
      HopsFilesTestHelper.writeData(out, 0, SYNC_SIZE);

      for (int i = 1; i <= TIMES; i++) {
        ((DFSOutputStream) out.getWrappedStream()).hsync(EnumSet
                .of(HdfsDataOutputStream.SyncFlag.END_BLOCK));
        HopsFilesTestHelper.writeData(out, i * SYNC_SIZE, 1024);  //written to new block
      }
      out.close();

      verifyFile(dfs, FILE_NAME, SYNC_SIZE * (TIMES + 1));

      assert CloudTestHelper.findAllBlocks().size() == TIMES + 1;

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
  public void TestConcat() throws IOException {
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

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      Path paths[] = new Path[5];
      for (int i = 0; i < paths.length; i++) {
        paths[i] = new Path("/dir/TEST-FLIE" + i);
        writeFile(dfs, paths[i].toString(), BLKSIZE);
      }

      //combine these files
      int targetFileSize = 0;
      Path merged = new Path("/dir/merged");
      writeFile(dfs, merged.toString(), targetFileSize);

      dfs.concat(merged, paths);

      verifyFile(dfs, merged.toString(), BLKSIZE * paths.length + targetFileSize);

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
  public void TestTruncate() throws IOException {
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

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      Path path = new Path("/dir/file");
      writeFile(dfs, path.toString(), BLKSIZE * 2);

      boolean isReady = dfs.truncate(path, BLKSIZE + BLKSIZE / 2);

      if (!isReady) {
        TestFileTruncate.checkBlockRecovery(path, dfs);
      }

      verifyFile(dfs, path.toString(), BLKSIZE + BLKSIZE / 2);

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
  public void TestTruncateSlowIncrementalBR() throws IOException {
    Logger.getRootLogger().setLevel(Level.DEBUG);
    CloudTestHelper.purgeS3();
    final Logger logger = Logger.getRootLogger();
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

      final FSNamesystem fsNamesystem = cluster.getNameNode().getNamesystem();
      final FSNamesystem fsNamesystemSpy = Mockito.spy(fsNamesystem);
      NameNodeRpcServer rpcServer = (NameNodeRpcServer) cluster.getNameNode().getRpcServer();
      rpcServer.setFSNamesystem(fsNamesystemSpy);

      Answer delayer = new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          LOG.info("Delaying the incremental block report so that" +
                  " sync / close file does not succeed on the first try");
          Thread.sleep(1000);
          return invocationOnMock.callRealMethod();
        }
      };

      Mockito.doAnswer(delayer).when(fsNamesystemSpy).
              processIncrementalBlockReport(any(DatanodeRegistration.class),
                      any(StorageReceivedDeletedBlocks.class));
      final String FILE_NAME = "/dir/TEST-FLIE";

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      final LogVerificationAppender appender1 = new LogVerificationAppender();
      logger.addAppender(appender1);
      writeFile(dfs, FILE_NAME, BLKSIZE * 2);
      verifyFile(dfs, FILE_NAME, BLKSIZE * 2);
      assertTrue(getExceptionCount(appender1.getLog(), NotReplicatedYetException.class) != 0);

      LOG.info("HopsFS-Cloud. Truncating file");
      final LogVerificationAppender appender2 = new LogVerificationAppender();
      logger.addAppender(appender2);
      boolean isReady = dfs.truncate(new Path(FILE_NAME), BLKSIZE + (BLKSIZE / 2));
      if (!isReady) {
        TestFileTruncate.checkBlockRecovery(new Path(FILE_NAME), dfs);
      }
      assertTrue(getExceptionCount(appender2.getLog(), NotReplicatedYetException.class) != 0);

      LOG.info("HopsFS-Cloud. Truncate op has completed");

      verifyFile(dfs, FILE_NAME, BLKSIZE + BLKSIZE / 2);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /*
  Check that upon format the default storage policy for the root
  is set to CLOUD
   */
  @Test
  public void TestFormat() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {
      final int NUM_DN = 5;
      final int BLKSIZE = 128 * 1024;
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      BlockStoragePolicy policy = dfs.getStoragePolicy(new Path("/"));
      assertTrue("Expected: "+ HdfsConstants.CLOUD_STORAGE_POLICY_NAME+" Got: "+policy.getName(),
              policy.getName().compareTo("CLOUD") == 0);

      writeFile(dfs, "/file1", BLKSIZE);
      policy = dfs.getStoragePolicy(new Path("/file1"));
      assertTrue("Expected: "+ HdfsConstants.CLOUD_STORAGE_POLICY_NAME+" Got: "+policy.getName(),
              policy.getName().compareTo("CLOUD") == 0);


    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  int getExceptionCount(List<LoggingEvent> log, Class e) {
    int count = 0;
    for (int i = 0; i < log.size(); i++) {
      if (log.get(i).getMessage().toString().contains(e.getSimpleName())) {
        count++;
      }
    }
    return count;
  }

  @AfterClass
  public static void TestZDeleteAllBuckets() throws IOException {
    CloudTestHelper.purgeS3();
  }
}

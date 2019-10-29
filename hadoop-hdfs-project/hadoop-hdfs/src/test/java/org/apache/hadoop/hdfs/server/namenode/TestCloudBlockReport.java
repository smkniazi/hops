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

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CloudBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedBlocksChecker;
import org.apache.hadoop.hdfs.server.common.CloudHelper;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.CloudPersistenceProvider;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.CloudFsDatasetImpl;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.CloudPersistenceProviderFactory;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.HopsFilesTestHelper.*;

import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.hdfs.TestSmallFilesCreation.*;
import static org.junit.Assert.fail;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestCloudBlockReport {

  static final Log LOG = LogFactory.getLog(TestCloudBlockReport.class);
  @Rule
  public TestName testname = new TestName();

  @Before
  public void setup() {
    Logger.getLogger(ProvidedBlocksChecker.class).setLevel(Level.DEBUG);
  }

  /**
   * Simple block report testing
   * <p>
   * Write some files --> Trigger block report --> Make sure every thing is fine and dandy
   *
   * @throws IOException
   */
  @Test
  public void TestBlockReportSimple() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {

      final int BLKSIZE = 128 * 1024;
      final int NUM_DN = 3;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_THREAD_SLEEP_INTERVAL_KEY, 1000);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY, 10);
      conf.setInt(DFSConfigKeys.DFS_CLOUD_AWS_S3_NUM_BUCKETS, 2);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_KEY,
              DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_DEFAULT);
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_BLOCKID_BATCH_SIZE, 10);

      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      ProvidedBlocksChecker pbc =
              cluster.getNamesystem().getBlockManager().getProvidedBlocksChecker();

      long ret = CloudBlockReportTestHelper.waitForBRCompletion(pbc, 1);
      assertTrue("Exptected 1. Got: " + ret, 1 == ret);

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      for (int i = 0; i < 10; i++) {
        writeFile(dfs, "/dir/file" + i, BLKSIZE * 2);
      }
      CloudTestHelper.matchMetadata(conf);

      pbc.scheduleBlockReportNow();
      ret = CloudBlockReportTestHelper.waitForBRCompletion(pbc, 2);
      assertTrue("Exptected 2. Got: " + ret, 2 == ret);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Block report detects and deletes abandoned blocks.
   * Abandoned blocks happen if block delete request is
   * lost due to network or DN failure
   * <p>
   * Write some files --> remove all the metadata for some files to
   * simulate file delete operation where delete obj requset
   * to s3 is lost --> Block report deleted the abondoned blocks
   *
   * @throws IOException
   */
  @Test
  public void TestBlockReportAbandonedBlocks() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {

      final int BLKSIZE = 128 * 1024;
      final int NUM_DN = 3;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_THREAD_SLEEP_INTERVAL_KEY, 1000);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY, 10);
      conf.setInt(DFSConfigKeys.DFS_CLOUD_AWS_S3_NUM_BUCKETS, 2);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_KEY,
              DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_DEFAULT);
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_BLOCKID_BATCH_SIZE, 10);

      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      ProvidedBlocksChecker pbc =
              cluster.getNamesystem().getBlockManager().getProvidedBlocksChecker();

      long ret = CloudBlockReportTestHelper.waitForBRCompletion(pbc, 1);
      assertTrue("Exptected 1. Got: " + ret, 1 == ret);

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      for (int i = 0; i < 3; i++) {
        writeFile(dfs, "/dir/file" + i, BLKSIZE * 10);
      }
      CloudTestHelper.matchMetadata(conf);

      pbc.scheduleBlockReportNow();
      ret = CloudBlockReportTestHelper.waitForBRCompletion(pbc, 2);
      assertTrue("Exptected 2. Got: " + ret, ret == 2);

      // creating abandoned blocks
      deleteFileMetadata("file0");

      CloudPersistenceProvider cloudConnector =
              CloudPersistenceProviderFactory.getCloudClient(conf);
      Map<Long, CloudBlock> cloudBlocksMap = cloudConnector.getAll("");
      Map<Long, BlockInfoContiguous> dbBlocksMap = pbc.findAllBlocksRange(0, 1000);

      assert cloudBlocksMap.size() == 30;
      assert dbBlocksMap.size() == 20;

      List<BlockInfoContiguous> toMissing = new ArrayList<>();
      List<ProvidedBlocksChecker.BlockToMarkCorrupt> toCorrupt = new ArrayList<>();
      List<CloudBlock> toDelete = new ArrayList<>();
      pbc.reportDiff(dbBlocksMap, cloudBlocksMap, toMissing, toCorrupt, toDelete);

      assertTrue("Exptected 10. Got: " + toDelete.size(), toDelete.size() == 10);
      assertTrue("Exptected 0. Got: " + toMissing.size(), toMissing.size() == 0);
      assertTrue("Exptected 0. Got: " + toCorrupt.size(), toCorrupt.size() == 0);

      long brCount = pbc.getProvidedBlockReportsCount();
      pbc.scheduleBlockReportNow();
      CloudBlockReportTestHelper.waitForBRCompletion(pbc, brCount + 1);
      assert pbc.getProvidedBlockReportsCount() == brCount + 1;

      //Now the blocks are put in the invalidated list
      //wait for some time to make sure that the cloud has removed the
      //delete blocks

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

  /**
   * Block report detects corrupt blocks and adds the
   * block to the URB list with corrupt priority
   * <p>
   * Write some files --> change GS of some blocks in the cloud
   * --> Block report detects corrupt blocks
   *
   * @throws IOException
   */
  @Test
  public void TestBlockReportCorruptBlocks() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {

      final int BLKSIZE = 128 * 1024;
      final int NUM_DN = 3;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_THREAD_SLEEP_INTERVAL_KEY, 1000);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY, 10);
      conf.setInt(DFSConfigKeys.DFS_CLOUD_AWS_S3_NUM_BUCKETS, 2);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_KEY,
              DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_DEFAULT);
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_BLOCKID_BATCH_SIZE, 10);

      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      ProvidedBlocksChecker pbc =
              cluster.getNamesystem().getBlockManager().getProvidedBlocksChecker();

      CloudBlockReportTestHelper.waitForBRCompletion(pbc, 1);
      assert pbc.getProvidedBlockReportsCount() == 1;

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      for (int i = 0; i < 1; i++) {
        writeFile(dfs, "/dir/file" + i, BLKSIZE * 10);
      }

      CloudTestHelper.matchMetadata(conf);

      CloudBlockReportTestHelper.changeGSOfCloudObjs(conf, 5);

      CloudPersistenceProvider cloudConnector =
              CloudPersistenceProviderFactory.getCloudClient(conf);
      Map<Long, CloudBlock> cloudBlocksMap = cloudConnector.getAll("");
      Map<Long, BlockInfoContiguous> dbBlocksMap = pbc.findAllBlocksRange(0, 1000);

      assert cloudBlocksMap.size() == 10;
      assert dbBlocksMap.size() == 10;

      List<BlockInfoContiguous> toMissing = new ArrayList<>();
      List<ProvidedBlocksChecker.BlockToMarkCorrupt> toCorrupt = new ArrayList<>();
      List<CloudBlock> toDelete = new ArrayList<>();
      pbc.reportDiff(dbBlocksMap, cloudBlocksMap, toMissing, toCorrupt, toDelete);

      assertTrue("Exptected 0. Got: " + toDelete.size(), toDelete.size() == 0);
      assertTrue("Exptected 0. Got: " + toMissing.size(), toMissing.size() == 0);
      assertTrue("Exptected 5. Got: " + toCorrupt.size(), toCorrupt.size() == 5);

      long brCount = pbc.getProvidedBlockReportsCount();
      pbc.scheduleBlockReportNow();
      CloudBlockReportTestHelper.waitForBRCompletion(pbc, brCount + 1);
      assert pbc.getProvidedBlockReportsCount() == brCount + 1;

      //check
      assert cluster.getNamesystem().getMissingBlocksCount() == 5;

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Testing user deleting complete blocks from S3 bucket
   *
   * @throws IOException
   */

  @Test
  public void TestManuallyDeletedBlocks() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {

      final int BLKSIZE = 128 * 1024;
      final int NUM_DN = 3;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_THREAD_SLEEP_INTERVAL_KEY, 1000);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY, 10);
      conf.setInt(DFSConfigKeys.DFS_CLOUD_AWS_S3_NUM_BUCKETS, 2);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_KEY,
              DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_DEFAULT);
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_BLOCKID_BATCH_SIZE, 10);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_MARK_PARTIALLY_LISTED_BLOCKS_CORRUPT_AFTER_KEY,
              30 * 1000);

      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      ProvidedBlocksChecker pbc =
              cluster.getNamesystem().getBlockManager().getProvidedBlocksChecker();

      CloudBlockReportTestHelper.waitForBRCompletion(pbc, 1);
      assert pbc.getProvidedBlockReportsCount() == 1;

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      for (int i = 0; i < 1; i++) {
        writeFile(dfs, "/dir/file" + i, BLKSIZE * 10);
      }

      CloudTestHelper.matchMetadata(conf);

      CloudBlockReportTestHelper.deleteBlocksAndMetaObjs(conf, 5);

      long brCount = pbc.getProvidedBlockReportsCount();
      pbc.scheduleBlockReportNow();
      CloudBlockReportTestHelper.waitForBRCompletion(pbc, brCount + 1);
      assert pbc.getProvidedBlockReportsCount() == brCount + 1;

      assert cluster.getNamesystem().getMissingBlocksCount() == 0;

      Thread.sleep(30000);

      brCount = pbc.getProvidedBlockReportsCount();
      pbc.scheduleBlockReportNow();
      CloudBlockReportTestHelper.waitForBRCompletion(pbc, brCount + 1);
      assert pbc.getProvidedBlockReportsCount() == brCount + 1;

      //check
      assert cluster.getNamesystem().getMissingBlocksCount() == 5;

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Testing partial listing (when only the block or meta obj is found in S3)
   *
   * @throws IOException
   */

  @Test
  public void TestBlockReportPartialListing() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {

      final int BLKSIZE = 128 * 1024;
      final int NUM_DN = 3;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_THREAD_SLEEP_INTERVAL_KEY, 1000);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY, 10);
      conf.setInt(DFSConfigKeys.DFS_CLOUD_AWS_S3_NUM_BUCKETS, 2);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_KEY,
              DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_DEFAULT);
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_BLOCKID_BATCH_SIZE, 10);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_MARK_PARTIALLY_LISTED_BLOCKS_CORRUPT_AFTER_KEY,
              30 * 1000);

      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      ProvidedBlocksChecker pbc =
              cluster.getNamesystem().getBlockManager().getProvidedBlocksChecker();

      CloudBlockReportTestHelper.waitForBRCompletion(pbc, 1);
      assert pbc.getProvidedBlockReportsCount() == 1;

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      for (int i = 0; i < 1; i++) {
        writeFile(dfs, "/dir/file" + i, BLKSIZE * 10);
      }

      CloudTestHelper.matchMetadata(conf);

      CloudBlockReportTestHelper.deleteMetaObjects(conf, 5);

      long brCount = pbc.getProvidedBlockReportsCount();
      pbc.scheduleBlockReportNow();
      CloudBlockReportTestHelper.waitForBRCompletion(pbc, brCount + 1);
      assert pbc.getProvidedBlockReportsCount() == brCount + 1;

      assert cluster.getNamesystem().getMissingBlocksCount() == 0;

      Thread.sleep(30000);

      brCount = pbc.getProvidedBlockReportsCount();
      pbc.scheduleBlockReportNow();
      CloudBlockReportTestHelper.waitForBRCompletion(pbc, brCount + 1);
      assert pbc.getProvidedBlockReportsCount() == brCount + 1;

      //check
      assert cluster.getNamesystem().getMissingBlocksCount() == 5;

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
  public void TestBlockReportMultipleErrors() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {

      final int BLKSIZE = 128 * 1024;
      final int NUM_DN = 3;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_THREAD_SLEEP_INTERVAL_KEY, 1000);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY, 10);
      conf.setInt(DFSConfigKeys.DFS_CLOUD_AWS_S3_NUM_BUCKETS, 2);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_KEY,
              DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_DEFAULT);
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_BLOCKID_BATCH_SIZE, 10);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_MARK_PARTIALLY_LISTED_BLOCKS_CORRUPT_AFTER_KEY,
              20 * 1000);

      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      ProvidedBlocksChecker pbc =
              cluster.getNamesystem().getBlockManager().getProvidedBlocksChecker();

      CloudBlockReportTestHelper.waitForBRCompletion(pbc, 1);
      assert pbc.getProvidedBlockReportsCount() == 1;

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      for (int i = 0; i < 20; i++) {
        writeFile(dfs, "/dir/file" + i, BLKSIZE * 1);
      }

      CloudTestHelper.matchMetadata(conf);

      deleteFileMetadata("file9");
      deleteFileMetadata("file8");
      CloudBlockReportTestHelper.deleteMetaObjects(conf, 2);
      CloudBlockReportTestHelper.changeGSOfCloudObjs(conf, 2);

      long brCount = pbc.getProvidedBlockReportsCount();
      pbc.scheduleBlockReportNow();
      CloudBlockReportTestHelper.waitForBRCompletion(pbc, brCount + 1);
      assert pbc.getProvidedBlockReportsCount() == brCount + 1;

      long count = cluster.getNamesystem().getMissingBlocksCount();
      assertTrue("Exptected: " + 2 + " Got: " + count, count == 2);

      //partially listed blocks takes 30 sec
      Thread.sleep(25000);

      brCount = pbc.getProvidedBlockReportsCount();
      pbc.scheduleBlockReportNow();
      CloudBlockReportTestHelper.waitForBRCompletion(pbc, brCount + 1);
      assert pbc.getProvidedBlockReportsCount() == brCount + 1;

      //check
      long missingBlkCount = cluster.getNamesystem().getMissingBlocksCount();
      assertTrue("Expected : 4 Got: " + missingBlkCount, missingBlkCount == 4);

      count = CloudTestHelper.getAllCloudBlocks(CloudPersistenceProviderFactory
              .getCloudClient(conf)).size();
      assertTrue(" Expected : " + 18 + " Got: " + count, 18 == count);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private static List<INode> deleteFileMetadata(final String name) throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                INodeDataAccess ida = (INodeDataAccess) HdfsStorageFactory
                        .getDataAccess(INodeDataAccess.class);
                BlockInfoDataAccess bda = (BlockInfoDataAccess) HdfsStorageFactory
                        .getDataAccess(BlockInfoDataAccess.class);
                List<INode> inodes = ida.findINodes(name);
                assert inodes.size() == 1;
                ida.deleteInode(name);
                bda.deleteBlocksForFile(inodes.get(0).getId());
                return null;
              }
            };
    return (List<INode>) handler.handle();
  }

  @Test
  public void TestBlockReportOpenFiles() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {

      final int BLKSIZE = 128 * 1024;
      final int NUM_DN = 3;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_THREAD_SLEEP_INTERVAL_KEY, 1000);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY, 10);
      conf.setInt(DFSConfigKeys.DFS_CLOUD_AWS_S3_NUM_BUCKETS, 2);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_KEY,
              DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_DEFAULT);
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_BLOCKID_BATCH_SIZE, 10);
      final int corruptAfter = 10 * 1000;
      conf.setLong(DFSConfigKeys.DFS_CLOUD_MARK_PARTIALLY_LISTED_BLOCKS_CORRUPT_AFTER_KEY,
              corruptAfter);

      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      ProvidedBlocksChecker pbc =
              cluster.getNamesystem().getBlockManager().getProvidedBlocksChecker();

      long ret = CloudBlockReportTestHelper.waitForBRCompletion(pbc, 1);
      assertTrue("Exptected 1. Got: " + ret, 1 == ret);

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      FSDataOutputStream os = (FSDataOutputStream) dfs.create(new Path("/dir/file"), (short) 1);
      byte[] data = new byte[BLKSIZE];
      os.write(data);

      CloudTestHelper.matchMetadata(conf, true);

      Thread.sleep(corruptAfter + 1000);

      CloudPersistenceProvider cloudConnector =
              CloudPersistenceProviderFactory.getCloudClient(conf);
      Map<Long, CloudBlock> cloudBlocksMap = cloudConnector.getAll("");
      Map<Long, BlockInfoContiguous> dbBlocksMap = pbc.findAllBlocksRange(0, 1000);

      assert cloudBlocksMap.size() == 0;
      assert dbBlocksMap.size() == 1;

      List<BlockInfoContiguous> toMissing = new ArrayList<>();
      List<ProvidedBlocksChecker.BlockToMarkCorrupt> toCorrupt = new ArrayList<>();
      List<CloudBlock> toDelete = new ArrayList<>();
      pbc.reportDiff(dbBlocksMap, cloudBlocksMap, toMissing, toCorrupt, toDelete);

      assertTrue("Exptected 0. Got: " + toDelete.size(), toDelete.size() == 0);
      assertTrue("Exptected 0. Got: " + toMissing.size(), toMissing.size() == 0);
      assertTrue("Exptected 0. Got: " + toCorrupt.size(), toCorrupt.size() == 0);

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
  public void TestCloudRBWBR() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {

      final int BLKSIZE = 64 * 1024 * 1024;
      final int NUM_DN = 1;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_THREAD_SLEEP_INTERVAL_KEY, 1000);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY, 10);
      conf.setInt(DFSConfigKeys.DFS_CLOUD_AWS_S3_NUM_BUCKETS, 2);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_KEY,
              DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_DEFAULT);
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_BLOCKID_BATCH_SIZE, 10);

      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      ProvidedBlocksChecker pbc =
              cluster.getNamesystem().getBlockManager().getProvidedBlocksChecker();

      long ret = CloudBlockReportTestHelper.waitForBRCompletion(pbc, 1);
      assertTrue("Exptected 1. Got: " + ret, 1 == ret);

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      FSDataOutputStream out = (FSDataOutputStream) dfs.create(new Path("/dir/file"), (short) 1);
      byte[] data = new byte[BLKSIZE + BLKSIZE / 2]; // 1 1/2 blocks
      out.write(data);

      String poolId = cluster.getNamesystem().getBlockPoolId();
      Map<DatanodeStorage, BlockReport> brs =
              cluster.getDataNodes().get(0).getFSDataset().getBlockReports(poolId);

      //there should be only one block in the BR for CLOUD volume
      for (DatanodeStorage storage : brs.keySet()) {
        BlockReport br = brs.get(storage);
        if (storage.getStorageType() == StorageType.CLOUD) {
          assert br.getNumberOfBlocks() == 1;
        } else {
          assert br.getNumberOfBlocks() == 0;
        }
      }

      CloudTestHelper.matchMetadata(conf, true);

      CloudPersistenceProvider cloudConnector =
              CloudPersistenceProviderFactory.getCloudClient(conf);
      Map<Long, CloudBlock> cloudBlocksMap = cloudConnector.getAll("");
      assert cloudBlocksMap.size() == 1;

      cluster.getDataNodes().get(0).scheduleAllBlockReport(0);
      Thread.sleep(10000);

      out.close();

      brs = cluster.getDataNodes().get(0).getFSDataset().getBlockReports(poolId);

      //there should be only one block in the BR for CLOUD volume
      for (DatanodeStorage storage : brs.keySet()) {
        BlockReport br = brs.get(storage);
        if (storage.getStorageType() == StorageType.CLOUD) {
          assert br.getNumberOfBlocks() == 0;
        } else {
          assert br.getNumberOfBlocks() == 0;
        }
      }

      cloudBlocksMap = cloudConnector.getAll("");
      assert cloudBlocksMap.size() == 2;

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

  /*
  * Handling of incremental BR of a block that doest not belong to any file
   */
  @Test
  public void TestCloudDanglingIBR() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {

      final int BLKSIZE = 64 * 1024 * 1024;
      final int NUM_DN = 1;
      final int prefixSize = 10;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_THREAD_SLEEP_INTERVAL_KEY, 1000);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY, prefixSize);
      conf.setInt(DFSConfigKeys.DFS_CLOUD_AWS_S3_NUM_BUCKETS, 2);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_KEY,
              DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_DEFAULT);
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_BLOCKID_BATCH_SIZE, 10);

      CloudTestHelper.setRandomBucketPrefix(conf, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();

      File file = new File(cluster.getDataDirectory()+"/tmp-blk");
      FileWriter outblk = new FileWriter(file);
      outblk.close();

      Block blk = new Block(1, 0, 1, (short)0);
      String blkKey = CloudHelper.getBlockKey( prefixSize, blk);
      String metaKey = CloudHelper.getMetaFileKey( prefixSize, blk);

      Map<String, String> metadata = new HashMap<>();
      CloudPersistenceProvider cloudConnector =
              CloudPersistenceProviderFactory.getCloudClient(conf);
      cloudConnector.uploadObject((short)0, blkKey, file, metadata);
      cloudConnector.uploadObject((short)0, metaKey, file, metadata);


      String bpid = cluster.getNamesystem().getBlockPoolId();
      DatanodeRegistration nodeReg = cluster.getDataNodes().get(0).getDNRegistrationForBP(bpid);
      String storageID =
              ((CloudFsDatasetImpl)cluster.getDataNodes().get(0).getFSDataset()).getCloudVolume().getStorageID();
      DatanodeStorage datanodeStorage =
              ((CloudFsDatasetImpl)cluster.getDataNodes().get(0).getFSDataset()).getStorage(storageID);
      assert storageID != null;
      StorageReceivedDeletedBlocks[] receivedAndDeletedBlocks =
              new StorageReceivedDeletedBlocks[1];

      ReceivedDeletedBlockInfo[] blocks = new ReceivedDeletedBlockInfo[1];
      blocks[0] = new ReceivedDeletedBlockInfo(blk,
              ReceivedDeletedBlockInfo.BlockStatus.RECEIVED_BLOCK, null );
      receivedAndDeletedBlocks[0] = new StorageReceivedDeletedBlocks(datanodeStorage, blocks);

      cluster.getNameNodeRpc().blockReceivedAndDeleted(nodeReg, bpid, receivedAndDeletedBlocks);

      Thread.sleep(10000);

      assert cloudConnector.getAll("").size() == 0;

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

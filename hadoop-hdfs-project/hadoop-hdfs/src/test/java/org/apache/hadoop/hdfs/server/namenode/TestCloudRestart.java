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
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedBlocksChecker;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;

import java.io.IOException;

import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.hdfs.HopsFilesTestHelper.*;
import static org.junit.Assert.fail;

public class TestCloudRestart {

  static final Log LOG = LogFactory.getLog(TestCloudRestart.class);
  @Rule
  public TestName testname = new TestName();

  @BeforeClass
  public static void setBucketPrefix(){
    CloudTestHelper.prependBucketPrefix("TCR");
  }

  @Before
  public void setup() {
    Logger.getLogger(ProvidedBlocksChecker.class).setLevel(Level.DEBUG);
  }

  @Test
  public void TestSimpleRestart() throws IOException {
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

      cluster.restartNameNodes();
      cluster.waitActive();

      for (int i = 0; i < 10; i++) {
        verifyFile(dfs, "/dir/file" + i, BLKSIZE * 2);
      }

      for (int i = 0; i < 10; i++) {
        writeFile(dfs, "/dir/file-afterrestart" + i, BLKSIZE * 2);
        verifyFile(dfs, "/dir/file-afterrestart" + i, BLKSIZE * 2);
      }

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

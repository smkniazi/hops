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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.CloudBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedBlocksChecker;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.CloudPersistenceProvider;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.CloudPersistenceProviderFactory;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

public class TestCloudFailures {

  static final Log LOG = LogFactory.getLog(TestCloudFailures.class);

  @Rule
  public TestName testname = new TestName();

  @Before
  public void setup() {
//    Logger.getRootLogger().setLevel(Level.DEBUG);
    Logger.getLogger(ProvidedBlocksChecker.class).setLevel(Level.DEBUG);
    Logger.getLogger(CloudTestHelper.class).setLevel(Level.DEBUG);
  }

  Configuration getConf() {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, /*default 15*/ 1);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRY_MAX_ATTEMPTS_KEY, /*default 10*/ 1);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_BASE_KEY, /*default 500*/ 500);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_SLEEPTIME_MAX_KEY, /*default 15000*/1000);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_KEY, /*default 0*/ 0);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
            /*default 0*/0);
    conf.setInt(DFSConfigKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, /*default
    45*/ 2);
    conf.setInt(DFSConfigKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, /*default 10*/ 1);
    conf.set(HdfsClientConfigKeys.Retry.POLICY_SPEC_KEY, "1000,2");

    return conf;
  }

  @Test
  public void TestClientFailure() throws IOException {
    CloudTestHelper.purgeS3();
    MiniDFSCluster cluster = null;
    try {

      final int BLKSIZE = 128 * 1024;
      final int NUM_DN = 3;

      Configuration conf = getConf();
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
      cluster.setLeasePeriod(3 * 1000, 5 * 1000);

      DistributedFileSystem dfs = cluster.getFileSystem();

      ProvidedBlocksChecker pbc =
              cluster.getNamesystem().getBlockManager().getProvidedBlocksChecker();

      long ret = CloudBlockReportTestHelper.waitForBRCompletion(pbc, 1);
      assertTrue("Exptected 1. Got: " + ret, 1 == ret);

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      FSDataOutputStream out = (FSDataOutputStream) dfs.create(new Path("/dir/file"), (short) 1);
      byte[] data = new byte[BLKSIZE]; //write more than 64 KB to allocate a block
      out.write(data);

      Thread.sleep(6000);
      dfs.getClient().getLeaseRenewer().interruptAndJoin();
      dfs.getClient().abort();

      LOG.info("HopsFS-Cloud. Aborted the client");
      Thread.sleep(10000);

      //this will check that the number of blocks in the cloud and DB are same
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

  @AfterClass
  public static void TestZDeleteAllBuckets() throws IOException {
    CloudTestHelper.purgeS3();
  }

}

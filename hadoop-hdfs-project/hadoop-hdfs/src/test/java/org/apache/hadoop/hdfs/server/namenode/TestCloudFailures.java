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
import org.apache.hadoop.hdfs.CloudTestHelper;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedBlocksChecker;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TestCloudFailures {

  static final Log LOG = LogFactory.getLog(TestCloudFailures.class);
  static String testBucketPrefix = "hopsfs-testing-TCF";
  static Collection params = Arrays.asList(new Object[][]{
          {CloudProvider.AWS},
          {CloudProvider.AZURE}
  });

  @Parameterized.Parameters
  public static Collection<Object> configs() {
    return params;
  }

  CloudProvider defaultCloudProvider = null;
  public TestCloudFailures(CloudProvider cloudProvider) {
    this.defaultCloudProvider = cloudProvider;
  }

  @Rule
  public TestName testname = new TestName();

  @Before
  public void setup() {
    Logger.getRootLogger().setLevel(Level.INFO);
    Logger.getLogger(ProvidedBlocksChecker.class).setLevel(Level.INFO);
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
    CloudTestHelper.purgeCloudData(defaultCloudProvider, testBucketPrefix);
    MiniDFSCluster cluster = null;
    try {

      final int BLKSIZE = 128 * 1024 * 1024;
      final int NUM_DN = 3;

      Configuration conf = getConf();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, defaultCloudProvider.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);

      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_THREAD_SLEEP_INTERVAL_KEY, 1000);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY, 10);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_KEY,
              DFSConfigKeys.DFS_CLOUD_BLOCK_REPORT_DELAY_DEFAULT);
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_BLOCKID_BATCH_SIZE, 10);

      CloudTestHelper.setRandomBucketPrefix(conf, testBucketPrefix, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();
      cluster.setLeasePeriod(3 * 1000, 5 * 1000);

      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      FSDataOutputStream out = dfs.create(new Path("/dir/file"), (short) 1);
      byte[] data = new byte[BLKSIZE];
      out.write(data);

      Thread.sleep(6000);
      dfs.getClient().getLeaseRenewer().interruptAndJoin();
      dfs.getClient().abort();
      LOG.info("HopsFS-Cloud. Aborted the client");

      long startTime = System.currentTimeMillis();
      while (true) {
        if ((System.currentTimeMillis() - startTime) < 60 * 1000) {
          if (cluster.getNamesystem().getLeaseManager().countLease() == 0) {
            break;
          }
        }
        Thread.sleep(1000);
      }

      assertTrue("The NN should have recoverd the lease for the file ",
              cluster.getNamesystem().getLeaseManager().countLease() == 0 );

      Thread.sleep(5000);
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
    Iterator<Object> itr = params.iterator();
    while(itr.hasNext()){
      Object[] obj =(Object[]) itr.next();
      CloudTestHelper.purgeCloudData((CloudProvider) obj[0], testBucketPrefix);
    }
  }
}

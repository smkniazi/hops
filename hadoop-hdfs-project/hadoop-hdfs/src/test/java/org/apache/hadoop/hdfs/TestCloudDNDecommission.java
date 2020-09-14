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
package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CloudProvider;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedBlocksChecker;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.cloud.CloudPersistenceProviderAzureImpl;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.cloud.CloudPersistenceProviderS3Impl;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class tests datanode failure handling during file read and write operations for hopsfs-cloud
 */

@RunWith(Parameterized.class)
public class TestCloudDNDecommission {
  static final Log LOG = AppendTestUtil.LOG;
  static final int HEARTBEAT_INTERVAL = 1; // heartbeat interval in seconds
  static final int BLOCKREPORT_INTERVAL_MSEC = 10000; //block report in msec
  static final int NAMENODE_REPLICATION_INTERVAL = 1; //replication interval
  static String testBucketPrefix = "hopsfs-testing-TCDNF";
  static Collection params = Arrays.asList(new Object[][]{
          {CloudProvider.AWS},
//          {CloudProvider.AZURE}
  });

  @Before
  public void setup() {
    Logger.getLogger(ProvidedBlocksChecker.class).setLevel(Level.DEBUG);
    Logger.getLogger(CloudPersistenceProviderAzureImpl.class).setLevel(Level.DEBUG);
    Logger.getLogger(CloudPersistenceProviderS3Impl.class).setLevel(Level.DEBUG);
  }

  @Parameterized.Parameters
  public static Collection<Object> configs() {
    return params;
  }

  CloudProvider defaultCloudProvider = null;

  public TestCloudDNDecommission(CloudProvider cloudProvider) {
    this.defaultCloudProvider = cloudProvider;
  }

  @Rule
  public TestName testname = new TestName();

  static final String DIR =
          "/" + TestCloudDNDecommission.class.getSimpleName() + "/";

  {
    ((Log4JLogger) CloudPersistenceProviderS3Impl.LOG).getLogger().setLevel(Level.ALL);
  }

  @Test
  public void testFileReadFailureWithOneReplica() throws Exception {
    testDecommission(5);
  }

  public void testDecommission(int numWorker) throws Exception {

    final Configuration conf = new HdfsConfiguration();
    FileSystem localFileSys = FileSystem.getLocal(conf);
    File workingDir = new File(localFileSys.getWorkingDirectory().toUri());
    File excludeFile = new File(workingDir.getAbsoluteFile()+PathUtils.getTestDirName(getClass()) + "/work-dir/decommission");
    if(excludeFile.exists()){
      excludeFile.delete();
    }
    excludeFile.getParentFile().mkdirs();
    excludeFile.createNewFile();
    conf.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, excludeFile.getAbsolutePath());

    boolean enableCloud = false;
    final long BLK_SIZE = 16 * 1024 * 1024;
    ExecutorService es = Executors.newFixedThreadPool(numWorker);

    if (enableCloud) {
      CloudTestHelper.purgeCloudData(defaultCloudProvider, testBucketPrefix);
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, defaultCloudProvider.name());
      conf.setInt(DFSConfigKeys.DFS_CLOUD_MAX_PHANTOM_BLOCKS_FOR_READ_KEY, 1);
      conf.setInt(DFSConfigKeys.DFS_DN_CLOUD_CACHE_DELETE_ACTIVATION_PRECENTAGE_KEY, 99);
      CloudTestHelper.setRandomBucketPrefix(conf, testBucketPrefix, testname);
    }
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY, false);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 2000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, BLOCKREPORT_INTERVAL_MSEC);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_PENDING_TIMEOUT_SEC_KEY, 4);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, NAMENODE_REPLICATION_INTERVAL);

    //if a datanode fails then the unfinished block report entry will linger for some time
    //before it is reclaimed. Untill the entry is reclaimed other datanodes will not be
    //able to block report. Reducing the BR Max process time to quickly reclaim
    //unfinished block reports
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BR_PROCESSING_TIME, 5 * 1000);

    int numDatanodes = 2;
    final MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf).format(true).
            numDataNodes(numDatanodes);
    if (enableCloud) {
      builder.storageTypes(CloudTestHelper.genStorageTypes(numDatanodes));
    }

    final MiniDFSCluster cluster = builder.build();

    try {
      cluster.waitActive();
      final DistributedFileSystem fs = cluster.getFileSystem();
      final Path dir = new Path(DIR);

      final SlowWriter[] slowWriters = new SlowWriter[numWorker];
      final Future[] futures = new Future[numWorker];

      for (int i = 0; i < numWorker; i++) {
        slowWriters[i] = new SlowWriter(fs, new Path(dir, "file" + i), 200L,
                1 * 1024 * 1024);
        slowWriters[i].setMaxDataToWrite(BLK_SIZE * 2);
        futures[i] = es.submit(slowWriters[i]);
      }

      for (Future f : futures) {
        f.get();
      }

      DatanodeInfo[] info = cluster.getFileSystem().dfs.datanodeReport(HdfsConstants.DatanodeReportType.LIVE);
      String excludedDatanodeName = info[0].getName();

      ToolRunner.run(new DFSAdmin(conf), new String[]{"-updateExcludeList",
              excludedDatanodeName});

      cluster.getNamesystem().getBlockManager().getDatanodeManager().refreshNodes(conf);

      int retries = 100;
      for(int i = 0; i < retries; i++){
        if(cluster.getFileSystem().dfs.datanodeReport(HdfsConstants.DatanodeReportType.DECOMMISSIONING).length != 1){
          Thread.sleep(1000);
          LOG.info("Waiting for datanode "+excludedDatanodeName+" to decommission");

          continue;
        } else {
          break;
        }
      }

      if(cluster.getFileSystem().dfs.datanodeReport(HdfsConstants.DatanodeReportType.DECOMMISSIONING).length != 1){
        fail("Node did not decommission");
      }

      //wait for the read to finish
      for (Future f : futures) {
        f.get();
      }

      //read again and check the contents for the file
      for (SlowWriter s : slowWriters) {
        s.verify();
      }

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  static class SlowWriter implements Callable {
    private final Path filepath;
    private HdfsDataOutputStream out;
    private final long sleepms;
    private final int writeSize;
    private volatile boolean running = true;
    private final DistributedFileSystem fs;
    private int fileSize;
    private long maxDataToWrite = Long.MAX_VALUE;

    SlowWriter(DistributedFileSystem fs, Path filepath, final long sleepms,
               final int writeSize) {
      this.filepath = filepath;
      this.sleepms = sleepms;
      this.writeSize = writeSize;
      this.fs = fs;
    }

    @Override
    public Object call() throws Exception {
      running = true;
      slowWrite();
      IOUtils.closeStream(out);
      return null;
    }

    public void setMaxDataToWrite(long maxDataToWrite) {
      this.maxDataToWrite = maxDataToWrite;
    }

    public int getFileSize() {
      return fileSize;
    }

    public Path getFilepath() {
      return filepath;
    }

    void slowWrite() {
      try {
        out = (HdfsDataOutputStream) fs.create(filepath);
        Thread.sleep(sleepms);
        while (running) {
          HopsFilesTestHelper.writeData(out, fileSize, writeSize);
          fileSize += writeSize;
          LOG.info(filepath + " written  " + ((double)fileSize/maxDataToWrite)*100+"%");
          if (fileSize >= maxDataToWrite) {
            return;
          }
          Thread.sleep(sleepms);
        }
      } catch (InterruptedException e) {
        LOG.info(filepath + " interrupted:" + e);
      } catch (IOException e) {
        throw new RuntimeException(filepath.toString(), e);
      } finally {
      }
    }

    void verify() throws IOException {
      HopsFilesTestHelper.verifyFile(fs, filepath.toString(), fileSize);
    }

    void stop() {
      running = false;
    }
  }

  private void startNewDataNode(boolean enableCloud, int num, MiniDFSCluster cluster,
                                Configuration conf) throws IOException {
    //start new datanodes
    StorageType[][] styps = null;
    if (enableCloud) {
      styps = CloudTestHelper.genStorageTypes(num);
    }
    cluster.startDataNodes(conf, num, styps, true, null, null,
            null, null, null, false, false, false, null);
    cluster.waitActive();
  }

  @AfterClass
  public static void TestZDeleteAllBuckets() throws IOException {
    Iterator<Object> itr = params.iterator();
    while (itr.hasNext()) {
      Object[] obj = (Object[]) itr.next();
      CloudTestHelper.purgeCloudData((CloudProvider) obj[0], testBucketPrefix);
    }
  }
}

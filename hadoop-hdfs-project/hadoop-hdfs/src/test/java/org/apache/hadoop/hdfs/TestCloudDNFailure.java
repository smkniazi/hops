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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedBlocksChecker;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.cloud.CloudPersistenceProviderAzureImpl;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.cloud.CloudPersistenceProviderS3Impl;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;
import org.apache.hadoop.hdfs.protocol.Block;

/**
 * This class tests datanode failure handling during file read and write operations for hopsfs-cloud
 */

@RunWith(Parameterized.class)
public class TestCloudDNFailure {
  static final Log LOG = AppendTestUtil.LOG;
  static String testBucketPrefix = "hopsfs-testing-TCDNF";
  static Collection params = Arrays.asList(new Object[][]{
          {CloudProvider.AWS},
          {CloudProvider.AZURE}
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
  public TestCloudDNFailure(CloudProvider cloudProvider) {
    this.defaultCloudProvider = cloudProvider;
  }

  @Rule
  public TestName testname = new TestName();

  static final String DIR =
          "/" + TestCloudDNFailure.class.getSimpleName() + "/";

  {
    ((Log4JLogger) CloudPersistenceProviderS3Impl.LOG).getLogger().setLevel(Level.ALL);
  }

  /**
   * Test replace datanode on failure.
   */

  @Test
  public void testFileReadFailureWithOneReplica() throws Exception {
    try {
      testFileReadOnDataNodeFailure(1, 3);
      fail();
    } catch (Throwable e) {
      if (!(e instanceof ExecutionException &&
              e.getCause().getCause() instanceof BlockMissingException)) {
        throw e;
      }
    }
  }

  @Test
  public void testFileReadWithMultipleReplica() throws Exception {
    try {
      testFileReadOnDataNodeFailure(2, 3);
    } catch (Throwable e) {
      LOG.info(e, e);
      fail("No Exception was expected");
    }
  }

  public void testFileReadOnDataNodeFailure(int phantomReplication, int numWorker) throws Exception {
    final Configuration conf = new HdfsConfiguration();
    boolean enableCloud = true;
    final long BLK_SIZE = 32 * 1024 * 1024;
    ExecutorService es = Executors.newFixedThreadPool(numWorker);

    if (enableCloud) {
      CloudTestHelper.purgeCloudData(defaultCloudProvider, testBucketPrefix);
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, defaultCloudProvider.name());
      conf.setInt(DFSConfigKeys.DFS_CLOUD_MAX_PHANTOM_BLOCKS_FOR_READ_KEY, phantomReplication);
      CloudTestHelper.setRandomBucketPrefix(conf, testBucketPrefix, testname);
    }
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);

    //We want to test if the failed datanode is not marked stale and  if
    // DFS_CLOUD_MAX_PHANTOM_BLOCKS_FOR_READ_KEY is 1 then the
    // read operation should fail.
    //when read operation fails then the client reties the read operation and asks
    //new set of bock location. Here we want to return the same stale DN on failure
    //Increasing the stale time to make sure the NN always return the same stale DN
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 5 * 60 * 1000);

    //if a datanode fails then the unfinished block report entry will linger for some time
    //before it is reclaimed. Untill the entry is reclaimed other datanodes will not be
    //able to block report. Reducing the BR Max process time to quickly reclaim
    //unfinished block reports
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BR_PROCESSING_TIME, 5*1000);

    final int INITIAL_NUM_DN = 1;
    final int ADDITIONAL_NUM_DN = 1;

    final MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf).format(true).
            numDataNodes(INITIAL_NUM_DN);
    if (enableCloud) {
      builder.storageTypes(CloudTestHelper.genStorageTypes(INITIAL_NUM_DN));
    }

    final MiniDFSCluster cluster = builder.build();

    try {
      cluster.waitActive();
      final DistributedFileSystem fs = cluster.getFileSystem();
      final Path dir = new Path(DIR);

      final SlowWriter[] slowWriters = new SlowWriter[numWorker];
      final SlowReader[] slowReaders = new SlowReader[numWorker];
      final Future[] futures = new Future[numWorker];

      for (int i = 0; i < numWorker; i++) {
        //create writers to create one block per file.
        //delay is 0ms for fast writing.
        slowWriters[i] = new SlowWriter(fs, new Path(dir, "file" + i), 0L,
                1 * 1024 * 1024);
        slowWriters[i].setMaxDataToWrite(BLK_SIZE);
        futures[i] = es.submit(slowWriters[i]);
      }

      for (Future f : futures) {
        f.get();
      }

      //start new datanodes
      startNewDataNode(enableCloud, ADDITIONAL_NUM_DN, cluster, conf);

      // Start slow readers
      for (int i = 0; i < numWorker; i++) {
        //create slow readers with different speed
        slowReaders[i] = new SlowReader(fs, slowWriters[i].getFilepath(), (i + 1) * 50L,
                2 * 1024 * 1024, slowWriters[i].getFileSize());
        futures[i] = es.submit(slowReaders[i]);
      }

      //start reading
      sleepSeconds(3);

      for (int i = 0; i < cluster.getDataNodes().size(); i++) {
        LOG.info("HopsFS-Cloud. Datanode : " + i + " ID: " + cluster.getDataNodes().get(i).getDatanodeUuid());
      }
      LOG.info("HopsFS-Cloud. Storring First Datanode");
      //kill DN that stores the block
      MiniDFSCluster.DataNodeProperties dnprop = cluster.stopDataNode(0);

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

  @Test
  public void testFileDNFailureWhileWrite() throws Exception {
    TestFileWriteOnDataNodeFailure(1);
  }

  @Test
  public void testFileDNFailureWhileWriteMT() throws Exception {
    TestFileWriteOnDataNodeFailure(3);
  }

  public void TestFileWriteOnDataNodeFailure(final int numThreads) throws Exception {
    final Configuration conf = new HdfsConfiguration();
    boolean enableCloud = true;
    final long BLK_SIZE = 1 * 1024 * 1024;
    final int NUM_DN = 5;

    ExecutorService es = Executors.newFixedThreadPool(numThreads);
    if (enableCloud) {
      CloudTestHelper.purgeCloudData(defaultCloudProvider, testBucketPrefix);
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, defaultCloudProvider.name());
      CloudTestHelper.setRandomBucketPrefix(conf, testBucketPrefix, testname);
    }
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLK_SIZE);

    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500);
    //if a datanode fails then the unfinished block report entry will linger for some time
    //before it is reclaimed. Untill the entry is reclaimed other datanodes will not be
    //able to block report. Reducing the BR Max process time to quickly reclaim
    //unfinished block reports
    conf.setLong(DFSConfigKeys.DFS_BR_LB_MAX_BR_PROCESSING_TIME, 5*1000);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);

    final MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf).format(true).
            numDataNodes(NUM_DN);
    if (enableCloud) {
      builder.storageTypes(CloudTestHelper.genStorageTypes(NUM_DN));
    }
    final MiniDFSCluster cluster = builder.build();

    try {
      cluster.waitActive();
      final DistributedFileSystem fs = cluster.getFileSystem();
      final Path dir = new Path(DIR);

      int livenodes = cluster.getNamesystem().getNumLiveDataNodes();
      assertTrue("Expecting " + NUM_DN + " Got: " + livenodes,
              cluster.getNamesystem().getNumLiveDataNodes() == NUM_DN);

      //save all datanode properties objects
      List<DataNodeProperties> dnpList = cluster.getDataNodeProperties();
      DataNodeProperties dnProps[] = new DataNodeProperties[NUM_DN];
      assert dnpList.size() == NUM_DN;
      for (int i = 0; i < dnpList.size(); i++) {
        dnProps[i] = dnpList.get(i);
      }

      //stopping all datanodes except one
      for (int i = 0; i < NUM_DN - 1; i++) {
        cluster.stopDataNode(0);
        LOG.info("Stopped a DN at port " + dnProps[i].ipcPort);
      }

      waitDNCount(cluster, 1);


      final SlowWriter[] slowWriters = new SlowWriter[numThreads];
      final Future[] futures = new Future[numThreads];
      for (int i = 0; i < numThreads; i++) {
        //create slow writers in different speed
        slowWriters[i] = new SlowWriter(fs, new Path(dir, "file" + i), (i + 1) * 1000L,
                64 * 1024); //write
        futures[i] = es.submit(slowWriters[i]);
      }

      // sleep to make sure that the client is writing to the first DN
      sleepSeconds(10);


      int numfailures = NUM_DN - 1;
      assert numfailures <= dnProps.length;
      for (int i = 0; i < numfailures; i++) {
        LOG.info("Starting a datanode: " + dnProps[i]);
        cluster.restartDataNode(dnProps[i]);
        waitDNCount(cluster, 2);
        LOG.info("Stopping a datanode");
        DataNodeProperties dnProp = cluster.stopDataNode(0);
        LOG.info("Stoped a datanode: " + dnProp);
        waitDNCount(cluster, 1);
        sleepSeconds(3); // write some more data
      }
      sleepSeconds(10); // write some more data

      LOG.info("Waiting for the threads to stop");
      for (int i = 0; i < slowWriters.length; i++) {
        slowWriters[i].stop();
      }

      for (Future f : futures) {
        f.get();
      }

      for (int i = 0; i < slowWriters.length; i++) {
        slowWriters[i].verify();
      }

      //stop all datanodes
      assert cluster.getNamesystem().getNumLiveDataNodes() == 1;
      cluster.stopDataNode(0);

      LOG.info("HopsFS-Cloud. starting all the datanodes");
      //now start all datanodes. make sure that after the
      //block reporting is done no garbage "rbw" files are
      //left on the DNs
      for (DataNodeProperties dnp : dnProps) {
        cluster.restartDataNode(dnp);
      }
      waitDNCount(cluster, dnProps.length);

      //make sure that all block reports are done and stray blocks have been deleted
      sleepSeconds(60);
      checkForStrayDNBlocks(cluster, NUM_DN);

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void waitDNCount(MiniDFSCluster cluster, int count) throws InterruptedException, IOException {
    for (int i = 0; i < 30; i++) {
      if (count == cluster.getNamesystem().getNumLiveDataNodes()) {
        return;
      }
      LOG.info("Waiting for datanode count to reach to " + count);
      Thread.sleep(1000);
    }
    fail("fail to read datanode count. Expecting: " + count + " Got: " + cluster.getNamesystem().getLiveNodes());

  }

  private void checkForStrayDNBlocks(MiniDFSCluster cluster, int numDataNodes) {

    for (int i = 0; i < numDataNodes; i++) {
      LOG.info("Checking DataNode Index: " + i);
      DataNode dn = cluster.getDataNodes().get(i);
      checkDNBR(cluster, dn);
    }
  }

  private void checkDNBR(MiniDFSCluster cluster, DataNode dn) {
    if (!dn.isDatanodeUp())
      fail("Datanode is not running");
    String poolId = cluster.getNamesystem().getBlockPoolId();
    Map<DatanodeStorage, BlockReport> br = dn.getFSDataset().getBlockReports(poolId);
    for (BlockReport reportedBlocks : br.values()) {
      if(reportedBlocks.getNumberOfBlocks() != 0){
        Iterator<Block> itr = reportedBlocks.blockIterable().iterator();
         while(itr.hasNext()){
           LOG.info("Block stored on DN is "+ itr.next());
         }
        fail(dn + " should have no blocks on disk. ");
      }
    }
  }

  static void sleepSeconds(final int waittime) throws InterruptedException {
    LOG.info("Wait " + waittime + " seconds");
    Thread.sleep(waittime * 1000L);
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
          LOG.info(filepath + " written  " + fileSize);
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

  static class SlowReader implements Callable {
    private final Path filepath;
    private HdfsDataInputStream in;
    private final long sleepms;
    private final int bufferSize;
    private volatile boolean running = true;
    private final DistributedFileSystem fs;
    private final int fileSize;
    private int totRead;

    SlowReader(DistributedFileSystem fs, Path filepath, final long sleepms,
               final int bufferSize, int fileSize) {
      this.filepath = filepath;
      this.sleepms = sleepms;
      this.bufferSize = bufferSize;
      this.fs = fs;
      this.fileSize = fileSize;
    }

    @Override
    public Object call() throws Exception {
      running = true;
      slowRead();
      IOUtils.closeStream(in);
      return null;
    }

    void slowRead() {
      try {
        in = (HdfsDataInputStream) fs.open(filepath);
        Thread.sleep(sleepms);
        for (; running; ) {
          byte[] buffer = new byte[bufferSize];
          int ret = in.read(buffer);
          if (ret > 0) {
            totRead += ret;
            LOG.info(filepath + " read  " + (int) ((double) totRead / (double) fileSize * 100) + "% " +
                    "bytes");
            Thread.sleep(sleepms);
          } else {
            assert fileSize == totRead;
            return;
          }
        }
      } catch (InterruptedException e) {
        LOG.info(filepath + " interrupted:" + e);
      } catch (IOException e) {
        throw new RuntimeException(filepath.toString(), e);
      } finally {
      }
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
    while(itr.hasNext()){
      Object[] obj =(Object[]) itr.next();
      CloudTestHelper.purgeCloudData((CloudProvider) obj[0], testBucketPrefix);
    }
  }
}

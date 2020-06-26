/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.mover;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CloudProvider;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.CloudFsDatasetImpl;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.hdfs.HopsFilesTestHelper.verifyFile;
import static org.apache.hadoop.hdfs.HopsFilesTestHelper.writeFile;
import static org.apache.hadoop.hdfs.server.datanode.TestBlockReport2.sendAndCheckBR;

@RunWith(Parameterized.class)
public class TestCloudMover {
  static final Log LOG = LogFactory.getLog(TestCloudMover.class);

  static String testBucketPrefix = "hopsfs-testing-TCM";

  static Collection params = Arrays.asList(new Object[][]{
          {CloudProvider.AWS},
          {CloudProvider.AZURE}
  });

  @Parameterized.Parameters
  public static Collection<Object> configs() {
    return params;
  }

  CloudProvider defaultCloudProvider = null;
  public TestCloudMover(CloudProvider cloudProvider) {
    this.defaultCloudProvider = cloudProvider;
  }

  @Rule
  public TestName testname = new TestName();

  @Test
  public void testScheduleBlockWithinSameNode1() throws Exception {
    testScheduleBlockWithinSameNode(1);
  }

  @Test
  public void testScheduleBlockWithinSameNode2() throws Exception {
    testScheduleBlockWithinSameNode(3);
  }

  public void testScheduleBlockWithinSameNode(int datanodes) throws Exception {
    CloudTestHelper.purgeCloudData(defaultCloudProvider, testBucketPrefix);

    final int FILE_SIZE = 10 * 1024 * 1024;
    final int BLKSIZE = 1 * 1024 * 1024;
    final Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
    conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
    conf.setBoolean(DFSConfigKeys.DFS_DN_CLOUD_BYPASS_CACHE_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY, 20);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
    CloudTestHelper.setRandomBucketPrefix(conf, testBucketPrefix, testname);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(datanodes)
            .storageTypes(
                    new StorageType[]{StorageType.DISK, StorageType.CLOUD})
            .build();
    try {
      cluster.waitActive();
      int treeDepth = 5;
      final DistributedFileSystem dfs = cluster.getFileSystem();
      Path dir = new Path("/dir");
      dfs.mkdirs(dir);
      dfs.setStoragePolicy(dir, "HOT");

      List<Path> files = new ArrayList<Path>();
      Path curDir = new Path(dir.toString());
      for (int i = 0; i < treeDepth; i++) {
        curDir = new Path(curDir, "depth" + i);
        files.add(new Path(curDir, "file_at_depth_" + i));
      }

      // write to DISK
      for (Path file : files) {
        writeFile(dfs, file.toString(), FILE_SIZE);
        verifyFile(dfs, file.toString(), FILE_SIZE);
      }

      //verify before movement
      for (Path file : files) {
        LocatedBlock lb = dfs.getClient().getLocatedBlocks(file.toString(), 0).get(0);
        StorageType[] storageTypes = lb.getStorageTypes();
        for (StorageType storageType : storageTypes) {
          Assert.assertTrue(StorageType.DISK == storageType);
        }
      }


      // move to CLOUD
      dfs.setStoragePolicy(dir, "CLOUD");
      int tries = 1;
      int rc = 0;
      for(int i = 0; i < tries; i++){
          int index=0;
          String args[] = new String[files.size()+1];
         args[index++] = "-p";
         for(Path path:files){
           args[index++] = path.toString();
         }

        rc = ToolRunner.run(conf, new Mover.Cli(),
                args);
        if(rc == ExitStatus.SUCCESS.getExitCode()){
          break;
        } else if(rc == ExitStatus.IN_PROGRESS.getExitCode()){
          continue;
        } else {
          Assert.assertEquals("Movement to CLOUD should be successfull", 0, rc);
        }
      }
      Assert.assertEquals("Movement to CLOUD should be successfull Code:"+rc,
              ExitStatus.SUCCESS.getExitCode(), rc);

      // Wait till namenode notified
      Thread.sleep(3000);
      //verify
      for (Path file : files) {
        LocatedBlock lb = dfs.getClient().getLocatedBlocks(file.toString(), 0).get(0);
        StorageType[] storageTypes = lb.getStorageTypes();
        for (StorageType storageType : storageTypes) {
          Assert.assertTrue("Storage type for "+file+" has not changed",
                  StorageType.CLOUD == storageType);
        }
        verifyFile(dfs, file.toString(), FILE_SIZE);
      }

      CloudTestHelper.matchMetadata(conf);
      sendAndCheckBR(0, datanodes, cluster, cluster.getNamesystem().getBlockPoolId(), 0,
              conf.getInt(DFSConfigKeys.DFS_NUM_BUCKETS_KEY, DFSConfigKeys.DFS_NUM_BUCKETS_DEFAULT));
    } finally {
      cluster.shutdown();
    }
  }


  /*
  Not supported yet.
  To implement it start by overriding createTemporary(...)  fn in CloudFsDatasetImpl
   */
  @Ignore
  public void testScheduleBlockAcrossNode() throws Exception {
    final int FILE_SIZE = 10 * 1024 * 1024;
    final Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
    conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());
    CloudTestHelper.setRandomBucketPrefix(conf, testBucketPrefix, testname);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(3)
            .storageTypes(
                    new StorageType[][]{{StorageType.DISK, StorageType.ARCHIVE},
                            {StorageType.DISK, StorageType.DISK},
                            {StorageType.ARCHIVE, StorageType.CLOUD}}).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      String dir = "/dir";
      String file = dir + "/" + "file";

      dfs.mkdirs(new Path(dir));
      dfs.setStoragePolicy(new Path(dir), "HOT");

      // write to DISK
      writeFile(dfs, file, FILE_SIZE);
      verifyFile(dfs, file, FILE_SIZE);

      // verify before movement
      LocatedBlock lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      StorageType[] storageTypes = lb.getStorageTypes();
      assert storageTypes.length == 2;
      for (StorageType storageType : storageTypes) {
        Assert.assertTrue(StorageType.DISK == storageType);
      }

      // move to CLOUD
      dfs.setStoragePolicy(new Path(file), "CLOUD");
      int rc = ToolRunner.run(conf, new Mover.Cli(),
              new String[]{"-p", file});
      Assert.assertEquals("Movement to CLOUD should be successfull", 0, rc);

      // Wait till namenode notified
      Thread.sleep(3000);
      lb = dfs.getClient().getLocatedBlocks(file, 0).get(0);
      storageTypes = lb.getStorageTypes();
      int replicas = 0;
      for (StorageType storageType : storageTypes) {
        if (StorageType.CLOUD == storageType) {
          replicas++;
        }
      }
      Assert.assertEquals(replicas, 1);
    } finally {
      cluster.shutdown();
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

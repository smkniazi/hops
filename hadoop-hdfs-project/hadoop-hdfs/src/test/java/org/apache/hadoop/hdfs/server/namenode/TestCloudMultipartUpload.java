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

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.common.CloudHelper;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.cloud.CloudPersistenceProvider;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.cloud.CloudPersistenceProviderAzureImpl;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.cloud.CloudPersistenceProviderFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TestCloudMultipartUpload {

  static final Log LOG = LogFactory.getLog(TestCloudMultipartUpload.class);
  static String testBucketPrefix = "hopsfs-testing-TCMU";
  static Collection params = Arrays.asList(new Object[][]{
          {CloudProvider.AWS},
          {CloudProvider.AZURE}
  });

  @Before
  public void setup() {
    Logger.getRootLogger().setLevel(Level.INFO);
    Logger.getLogger(CloudPersistenceProviderAzureImpl.class).setLevel(Level.DEBUG);
  }

  @Parameterized.Parameters
  public static Collection<Object> configs() {
    return params;
  }

  CloudProvider defaultCloudProvider = null;

  public TestCloudMultipartUpload(CloudProvider cloudProvider) {
    this.defaultCloudProvider = cloudProvider;
  }

  @Rule
  public TestName testname = new TestName();

  @Test
  public void TestSimpleConcurrentReadAndWrite() throws IOException {
    testConcurrentWrit(true);
  }

  @Test
  public void TestSimpleReadAndWrite() throws IOException {
    testConcurrentWrit(false);
  }

  public void testConcurrentWrit(boolean multipart) throws IOException {
    CloudTestHelper.purgeCloudData(defaultCloudProvider, testBucketPrefix);
    MiniDFSCluster cluster = null;
    try {

      final int BLKSIZE = 32 * 1024 * 1024;
      final int FILESIZE = 2 * BLKSIZE;

      final String FILE_NAME1 = "/dir/TEST-FLIE1";
      final int NUM_DN = 3;

      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
      conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, defaultCloudProvider.name());
      conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLKSIZE);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_MULTIPART_SIZE, 5 * 1024 * 1024);
      conf.setLong(DFSConfigKeys.DFS_CLOUD_MIN_MULTIPART_THRESHOLD, 5 * 1024 * 1024);
      conf.setBoolean(DFSConfigKeys.DFS_CLOUD_CONCURRENT_UPLOAD, multipart);
      CloudTestHelper.setRandomBucketPrefix(conf, testBucketPrefix, testname);

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN)
              .storageTypes(CloudTestHelper.genStorageTypes(NUM_DN)).format(true).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      CloudPersistenceProvider cloud = CloudPersistenceProviderFactory.getCloudClient(conf);

      dfs.mkdirs(new Path("/dir"));
      dfs.setStoragePolicy(new Path("/dir"), "CLOUD");

      int numFiles = 1;
      int fileSize = BLKSIZE - (1024 * 1024);
      FSDataOutputStream out[] = new FSDataOutputStream[numFiles];
      byte[] data = new byte[fileSize];
      for (int i = 0; i < numFiles; i++) {
        out[i] = dfs.create(new Path("/dir/file" + i), (short) 1);
        out[i].write(data);
      }

      if (defaultCloudProvider != CloudProvider.AZURE) {
        if (multipart) {
          assert cloud.listMultipartUploads(Lists.newArrayList(CloudHelper.getAllBuckets().keySet())).size() == numFiles;
        } else {
          assert cloud.listMultipartUploads(Lists.newArrayList(CloudHelper.getAllBuckets().keySet())).size() == 0;
        }
      }

      for (int i = 0; i < numFiles; i++) {
        out[i].close();
      }

      Thread.sleep(5000);
      if (defaultCloudProvider != CloudProvider.AZURE) {
        int count =
                cloud.listMultipartUploads(Lists.newArrayList(CloudHelper.getAllBuckets().keySet())).size();
        assertTrue("Expecting 0 multiplar uploads: Got: " + count, count == 0);
      }

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
    while (itr.hasNext()) {
      Object[] obj = (Object[]) itr.next();
      CloudTestHelper.purgeCloudData((CloudProvider) obj[0], testBucketPrefix);
    }
  }
}

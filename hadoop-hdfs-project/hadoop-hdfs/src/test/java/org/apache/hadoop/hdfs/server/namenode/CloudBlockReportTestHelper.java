package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CloudBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedBlocksChecker;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hdfs.server.common.CloudHelper;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.CloudPersistenceProvider;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.CloudPersistenceProviderFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CloudBlockReportTestHelper {
  static final Log LOG = LogFactory.getLog(CloudBlockReportTestHelper.class);

  public static long waitForBRCompletion(ProvidedBlocksChecker pbc, long count) throws IOException {
    try {
      long waitForFirstBR = 30;
      long value = -1;
      do {
        value = pbc.getProvidedBlockReportsCount();
        if (value == count) {
          value = count;
          break;
        }

        LOG.info("HopsFS-Cloud. BR waiting for block report counter to increase");
        Thread.sleep(1000);
        waitForFirstBR--;
      } while (waitForFirstBR > 0);

      if (value != count) {
        return -1;
      }

      waitForFirstBR = 30;
      do {
        if (pbc.getAllTasks().size() == 0 && !pbc.isBRInProgress()) {
          return value;
        }
        LOG.info("HopsFS-Cloud. BR waiting for block report tasks to finish");
        Thread.sleep(1000);
      } while (--waitForFirstBR > 0);

    } catch (InterruptedException e) {

    }
    return -1;
  }

  public static void changeGSOfCloudObjs(Configuration conf, int count) throws IOException {
    int prefixSize = conf.getInt(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY,
            DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_DEFAULT);
    CloudPersistenceProvider cloudConnector = CloudPersistenceProviderFactory.getCloudClient(conf);
    Map<Long, CloudBlock> objMap = cloudConnector.getAll("");
    int corrupted = 0;
    for (CloudBlock blk : objMap.values()) {
      if (blk.isPartiallyListed()) {
        continue;
      }
      short srcBucketID = blk.getBlock().getCloudBucketID();
      short dstBucketID = srcBucketID;

      Block b = blk.getBlock();
      String srcBlkKey = CloudHelper.getBlockKey(prefixSize, b);
      String srcMetaKey = CloudHelper.getMetaFileKey(prefixSize, b);

      b.setGenerationStampNoPersistance(8888);
      String dstBlkKey = CloudHelper.getBlockKey(prefixSize, b);
      String dstMetaKey = CloudHelper.getMetaFileKey(prefixSize, b);

      cloudConnector.renameObject(srcBucketID, dstBucketID, srcBlkKey, dstBlkKey);
      cloudConnector.renameObject(srcBucketID, dstBucketID, srcMetaKey, dstMetaKey);

      if (++corrupted >= count) {
        return;
      }
    }

  }

  public static void deleteMetaObjects(Configuration conf, int count) throws IOException {
    int prefixSize = conf.getInt(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY,
            DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_DEFAULT);
    CloudPersistenceProvider cloudConnector = CloudPersistenceProviderFactory.getCloudClient(conf);
    Map<Long, CloudBlock> objMap = cloudConnector.getAll("");
    int corrupted = 0;
    for (CloudBlock blk : objMap.values()) {
      short srcBucketID = blk.getBlock().getCloudBucketID();
      Block b = blk.getBlock();
      String srcMetaKey = CloudHelper.getMetaFileKey(prefixSize, b);
      cloudConnector.deleteObject(srcBucketID, srcMetaKey);
      if (++corrupted >= count) {
        return;
      }
    }
  }

  public static void deleteBlocksAndMetaObjs(Configuration conf, int count) throws IOException {
    int prefixSize = conf.getInt(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY,
            DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_DEFAULT);
    CloudPersistenceProvider cloudConnector = CloudPersistenceProviderFactory.getCloudClient(conf);
    Map<Long, CloudBlock> objMap = cloudConnector.getAll("");
    int corrupted = 0;
    for (CloudBlock blk : objMap.values()) {
      short srcBucketID = blk.getBlock().getCloudBucketID();
      Block b = blk.getBlock();
      String srcMetaKey = CloudHelper.getMetaFileKey(prefixSize, b);
      String srcBlockKey = CloudHelper.getBlockKey(prefixSize, b);
      cloudConnector.deleteObject(srcBucketID, srcMetaKey);
      cloudConnector.deleteObject(srcBucketID, srcBlockKey);
      if (++corrupted >= count) {
        return;
      }
    }
  }
}

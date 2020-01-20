package org.apache.hadoop.hdfs;

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.*;
import io.hops.metadata.hdfs.entity.*;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CloudProvider;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CloudBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguousUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.common.CloudHelper;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.CloudPersistenceProvider;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.CloudFsDatasetImpl;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.CloudPersistenceProviderFactory;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CloudTestHelper {
  static final Log LOG = LogFactory.getLog(CloudTestHelper.class);
  static String testBucketPrefix = "hopsfs.testing.";

  public static void prependBucketPrefix(String prefix){
    testBucketPrefix += prefix;
  }

  private static List<INode> findAllINodes() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                INodeDataAccess da = (INodeDataAccess) HdfsStorageFactory
                        .getDataAccess(INodeDataAccess.class);
                return da.allINodes();
              }
            };
    return (List<INode>) handler.handle();
  }


  public static Map<Long, BlockInfoContiguous> findAllBlocks() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                Map<Long, BlockInfoContiguous> blkMap = new HashMap<>();
                BlockInfoDataAccess da = (BlockInfoDataAccess) HdfsStorageFactory
                        .getDataAccess(BlockInfoDataAccess.class);

                List<BlockInfoContiguous> blocks = da.findAllBlocks();
                for (BlockInfoContiguous blk : blocks) {
                  blkMap.put(blk.getBlockId(), blk);
                }
                return blkMap;
              }
            };
    return (Map<Long, BlockInfoContiguous>) handler.handle();
  }

  public static Map<Long, ProvidedBlockCacheLoc> findCacheLocations() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                ProvidedBlockCacheLocDataAccess da = (ProvidedBlockCacheLocDataAccess) HdfsStorageFactory
                        .getDataAccess(ProvidedBlockCacheLocDataAccess.class);

                Map<Long, ProvidedBlockCacheLoc> blkMap = da.findAll();
                return blkMap;
              }
            };
    return (Map<Long, ProvidedBlockCacheLoc>) handler.handle();
  }


  public static List<Replica> findAllReplicas() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                ReplicaDataAccess da = (ReplicaDataAccess) HdfsStorageFactory
                        .getDataAccess(ReplicaDataAccess.class);
                return da.findAll();
              }
            };
    return (List<Replica>) handler.handle();
  }


  private static List<ReplicaUnderConstruction> findAllReplicasUC() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                ReplicaUnderConstructionDataAccess da =
                        (ReplicaUnderConstructionDataAccess) HdfsStorageFactory
                                .getDataAccess(ReplicaUnderConstructionDataAccess.class);
                return da.findAll();
              }
            };
    return (List<ReplicaUnderConstruction>) handler.handle();
  }


  private static List<PendingBlockInfo> findAllPendingBlocks() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                PendingBlockDataAccess da = (PendingBlockDataAccess) HdfsStorageFactory
                        .getDataAccess(PendingBlockDataAccess.class);
                return da.findAll();
              }
            };
    return (List<PendingBlockInfo>) handler.handle();
  }

  private static List<CorruptReplica> findAllCorruptReplicas() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                CorruptReplicaDataAccess da = (CorruptReplicaDataAccess) HdfsStorageFactory
                        .getDataAccess(CorruptReplicaDataAccess.class);
                return da.findAll();
              }
            };
    return (List<CorruptReplica>) handler.handle();
  }

  private static List<ExcessReplica> findAllExcessReplicas() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                ExcessReplicaDataAccess da = (ExcessReplicaDataAccess) HdfsStorageFactory
                        .getDataAccess(ExcessReplicaDataAccess.class);
                return da.findAll();
              }
            };
    return (List<ExcessReplica>) handler.handle();
  }

  private static List<InvalidatedBlock> findAllInvalidatedBlocks() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                InvalidateBlockDataAccess da = (InvalidateBlockDataAccess) HdfsStorageFactory
                        .getDataAccess(InvalidateBlockDataAccess.class);
                return da.findAll();
              }
            };
    return (List<InvalidatedBlock>) handler.handle();
  }

  private static List<UnderReplicatedBlock> findAllUnderReplicatedBlocks() throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                UnderReplicatedBlockDataAccess da = (UnderReplicatedBlockDataAccess) HdfsStorageFactory
                        .getDataAccess(UnderReplicatedBlockDataAccess.class);
                return da.findAll();
              }
            };
    return (List<UnderReplicatedBlock>) handler.handle();
  }

  private static boolean match(Map<Long, CloudBlock> cloudView,
                               Map<Long, BlockInfoContiguous> dbView) {
    if (cloudView.size() != dbView.size()) {
      List cv = new ArrayList(cloudView.values());
      Collections.sort(cv);
      List dbv = new ArrayList(dbView.values());
      Collections.sort(cv);
      LOG.info("HopsFS-Cloud Cloud Blocks " + Arrays.toString(cv.toArray()));
      LOG.info("HopsFS-Cloud DB Blocks " + Arrays.toString(dbv.toArray()));
    }

    assert cloudView.size() == dbView.size();

    for (long blkID : dbView.keySet()) {
      CloudBlock cloudBlock = cloudView.get(blkID);

      assert !cloudBlock.isPartiallyListed();

      BlockInfoContiguous dbBlock = dbView.get(blkID);

      assert cloudBlock != null && dbBlock != null;

      assert cloudBlock.getBlock().getCloudBucketID() == dbBlock.getCloudBucketID();
      assert cloudBlock.getBlock().getGenerationStamp() == dbBlock.getGenerationStamp();
      assert cloudBlock.getBlock().getNumBytes() == dbBlock.getNumBytes();

      assert dbBlock.getBlockUCState() == HdfsServerConstants.BlockUCState.COMPLETE;
    }

    return true;
  }

  public static boolean matchMetadata(Configuration conf) throws IOException {
    return matchMetadata(conf, false);
  }

  public static boolean matchMetadata(Configuration conf, boolean expectingUCB) throws IOException {
    int prefixSize = conf.getInt(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY,
            DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_DEFAULT);
    LOG.info("HopsFS-Cloud. CloudTestHelper. Checking Metadata");
    CloudPersistenceProvider cloud = null;

    try {
      cloud = CloudPersistenceProviderFactory.getCloudClient(conf);
      Map<Long, CloudBlock> cloudView = getAllCloudBlocks(cloud);
      Map<Long, BlockInfoContiguous> dbView = findAllBlocks();

      List sortDBBlocks = new ArrayList();
      sortDBBlocks.add(dbView.values());
      Collections.sort(sortDBBlocks);
      List sortCloudBlocks = new ArrayList();
      sortCloudBlocks.add(cloudView.values());
      Collections.sort(sortCloudBlocks);

      LOG.info("HopsFS-Cloud. DB View: " + sortDBBlocks);
      LOG.info("HopsFS-Cloud. Cloud View: " + sortCloudBlocks);


      String cloudProvider = conf.get(DFSConfigKeys.DFS_CLOUD_PROVIDER);
      if (!cloudProvider.equals(CloudProvider.AWS.name())) {
        match(cloudView, dbView); //fails becase of S3 eventual consistent LS, GCE is consistent
      }

      //block cache mapping
      Map<Long, ProvidedBlockCacheLoc> cacheLoc = findCacheLocations();
      assertTrue("Expecting size of the blocks and cache location locations to be same."+
              "Blocks: " +dbView.size()+" Cache: "+cacheLoc.size(),
              cacheLoc.size() == dbView.size());

      for (Block blk : dbView.values()) {
        if (blk instanceof BlockInfoContiguousUnderConstruction && expectingUCB) {
          continue;
        }

        LOG.info("HopsFS-Cloud. Checking Block: " + blk);
        short bucketID = blk.getCloudBucketID();
        String blockKey = CloudHelper.getBlockKey(prefixSize, blk);
        String metaKey = CloudHelper.getMetaFileKey(prefixSize, blk);

        assert cloud.objectExists(bucketID, blockKey) == true;
        assert cloud.objectExists(bucketID, metaKey) == true;
        assert cloud.getObjectSize(bucketID, blockKey) == blk.getNumBytes();

        Map<String, String> metadata = cloud.getUserMetaData(bucketID, blockKey);
        assertTrue("No metadata expected. Got "+metadata.size(), metadata.size() == 0);

        metadata = cloud.getUserMetaData(bucketID, metaKey);
        assert metadata.size() == 2;
        assert Long.parseLong(metadata.get(CloudFsDatasetImpl.OBJECT_SIZE)) == blk.getNumBytes();
        assert Long.parseLong(metadata.get(CloudFsDatasetImpl.GEN_STAMP)) == blk.getGenerationStamp();
        assert cacheLoc.get(blk.getBlockId()) != null;
      }

      assert findAllReplicas().size() == 0;
      if (!expectingUCB)
        assert findAllReplicasUC().size() == 0;
      assert findAllExcessReplicas().size() == 0;
      assert findAllPendingBlocks().size() == 0;
      assert findAllCorruptReplicas().size() == 0;
      assert findAllInvalidatedBlocks().size() == 0;
      assert findAllUnderReplicatedBlocks().size() == 0;

    } finally {
      if (cloud != null) {
        cloud.shutdown();
      }
    }
    return true;
  }

  public static Map<Long, CloudBlock> getAllCloudBlocks(CloudPersistenceProvider cloud)
          throws IOException {
    return cloud.getAll("");
  }

  public static StorageType[][] genStorageTypes(int numDataNodes) {

    StorageType[][] types = new StorageType[numDataNodes][];
    for (int i = 0; i < numDataNodes; i++) {
      types[i] = new StorageType[]{StorageType.CLOUD,StorageType.DISK};
    }
    return types;
  }

  public static void purgeS3() throws IOException {
    purgeS3(testBucketPrefix);
  }

  public static void purgeS3(String prefix) throws IOException {
    LOG.info("HopsFS-Cloud. Purging all buckets");
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_ENABLE_CLOUD_PERSISTENCE, true);
    conf.set(DFSConfigKeys.DFS_CLOUD_PROVIDER, CloudProvider.AWS.name());

    CloudPersistenceProvider cloudConnector =
            CloudPersistenceProviderFactory.getCloudClient(conf);
    cloudConnector.deleteAllBuckets(prefix);
    cloudConnector.shutdown();
  }

  public static void setRandomBucketPrefix(Configuration conf, TestName name) {
    Date date = new Date();
    String prefix = testBucketPrefix + "." + name.getMethodName() +
            "." + date.getHours() + date.getMinutes() + date.getSeconds();
    conf.set(DFSConfigKeys.S3_BUCKET_PREFIX, prefix.toLowerCase());
  }
}

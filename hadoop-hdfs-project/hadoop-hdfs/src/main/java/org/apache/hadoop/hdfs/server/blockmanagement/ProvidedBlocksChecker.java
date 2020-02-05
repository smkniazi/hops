/*
 * Copyright (C) 2019 Logical Clocks AB.
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
package org.apache.hadoop.hdfs.server.blockmanagement;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.hops.exception.StorageException;
import io.hops.leaderElection.LeaderElection;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.HdfsVariables;
import io.hops.metadata.Variables;
import io.hops.metadata.common.entity.LongVariable;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.dal.ProvidedBlockReportTasksDataAccess;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.InvalidatedBlock;
import io.hops.metadata.hdfs.entity.ProvidedBlockReportTask;
import io.hops.metadata.hdfs.entity.StorageId;
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CloudBlock;
import org.apache.hadoop.hdfs.server.common.CloudHelper;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ActiveMultipartUploads;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.CloudPersistenceProvider;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.CloudPersistenceProviderFactory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

/*
 Compare the blocks in the DB and in the cloud bucket(s)
 */
public class ProvidedBlocksChecker extends Thread {
  private final Namesystem ns;
  private boolean run = true;
  private final int prefixSize;
  private final long blockReportDelay;
  private final long sleepInterval;
  private final long marksCorruptBlocksDelay;
  private final long maxSubTasks;
  private final BlockManager bm;
  private final int maxProvidedBRThreads;
  private boolean isBRInProgress = false;
  private final long deleteAbandonedBlocksAfter;
  private final Configuration conf;

  CloudPersistenceProvider cloudConnector;

  static final Log LOG = LogFactory.getLog(ProvidedBlocksChecker.class);

  public ProvidedBlocksChecker(Configuration conf, Namesystem ns, BlockManager bm) {
    this.ns = ns;
    this.bm = bm;
    this.conf = conf;
    this.prefixSize = conf.getInt(
            DFS_CLOUD_PREFIX_SIZE_KEY,
            DFS_CLOUD_PREFIX_SIZE_DEFAULT);
    this.blockReportDelay = conf.getLong(
            DFS_CLOUD_BLOCK_REPORT_DELAY_KEY,
            DFS_CLOUD_BLOCK_REPORT_DELAY_DEFAULT);
    this.sleepInterval = conf.getLong(
            DFS_CLOUD_BLOCK_REPORT_THREAD_SLEEP_INTERVAL_KEY,
            DFS_CLOUD_BLOCK_REPORT_THREAD_SLEEP_INTERVAL_DEFAULT);
    this.marksCorruptBlocksDelay = conf.getLong(
            DFS_CLOUD_MARK_PARTIALLY_LISTED_BLOCKS_CORRUPT_AFTER_KEY,
            DFS_CLOUD_MARK_PARTIALLY_LISTED_BLOCKS_CORRUPT_AFTER_DEFAULT);
    this.maxSubTasks = conf.getLong(DFS_CLOUD_MAX_BR_SUB_TASKS_KEY,
            DFS_CLOUD_MAX_BR_SUB_TASKS_DEFAULT);
    this.maxProvidedBRThreads = conf.getInt(DFS_CLOUD_MAX_BR_THREADS_KEY,
            DFS_CLOUD_MAX_BR_THREADS_DEFAULT);
    this.deleteAbandonedBlocksAfter =
            conf.getLong(DFS_CLOUD_DELETE_ABANDONED_MULTIPART_FILES_AFTER,
            DFS_CLOUD_DELETE_ABANDONED_MULTIPART_FILES_AFTER_DEFAUlT);

    this.cloudConnector = CloudPersistenceProviderFactory.getCloudClient(conf);

  }

  /*
   * If Leader
   * ---------
   *    If it is time to block report then add all the sub tasks in the queue
   *
   * All nodes
   *    Poll the BR tasks queue and find work if any
   */
  @Override
  public void run() {
    while (run) {
      try {

        if (ns.isLeader()) {
          long startTime = getProvidedBlocksScanStartTime();
          long existingTasks = countBRPendingTasks();
          long timeElapsed = System.currentTimeMillis() - startTime;

          if (timeElapsed > blockReportDelay && existingTasks == 0) {
            final long END_ID = HdfsVariables.getMaxBlockID();
            List<ProvidedBlockReportTask> tasks = generateTasks(END_ID);
            addNewBlockReportTasks(tasks);
            checkAbandonedBlocks();
          }
        }

        // poll for work
        if (countBRPendingTasks() != 0) {
          startWork();
        }

      } catch (IOException e) {
        LOG.warn(e, e);
      } finally {
        if (run) {
          try {
            Thread.sleep(sleepInterval);
          } catch (InterruptedException e) {
            currentThread().interrupt();
          }
        }
      }
    }
  }

  private void startWork() throws IOException {
    //start workers and wait for them to finish their work
    Collection workers = new ArrayList<>();
    for (int i = 0; i < maxProvidedBRThreads; i++) {
      workers.add(new BRTasksPullers(i));
    }

    try {
      isBRInProgress = true;
      List<Future<Object>> futures =
              ((FSNamesystem) ns).getFSOperationsExecutor().invokeAll(workers);
      //Check for exceptions
      for (Future<Object> maybeException : futures) {
        maybeException.get();
      }

    } catch (InterruptedException e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    } finally {
      isBRInProgress = false;
    }
  }

  private boolean processTask(ProvidedBlockReportTask task) throws IOException {
    boolean successful = false;
    try {
      for (long start = task.getStartIndex(); start < task.getEndIndex(); ) {
        long end = start + prefixSize;
        String prefix = CloudHelper.getPrefix(prefixSize, start);
        LOG.debug("HopsFS-Cloud. BR Checking prefix: " + prefix);
        Map<Long, CloudBlock> cloudBlocksMap = cloudConnector.getAll(prefix,
                Lists.newArrayList(CloudHelper.getAllBuckets().keySet()));
        Map<Long, BlockInfoContiguous> dbBlocksMap = findAllBlocksRange(start, end);
        LOG.debug("HopsFS-Cloud. BR DB view size: " + dbBlocksMap.size() +
                " Cloud view size: " + cloudBlocksMap.size());

        List<BlockInfoContiguous> toMissing = new ArrayList<>();
        List<BlockToMarkCorrupt> toCorrupt = new ArrayList<>();
        List<CloudBlock> toDelete = new ArrayList<>();
        reportDiff(dbBlocksMap, cloudBlocksMap, toMissing, toCorrupt, toDelete);
        LOG.debug("HopsFS-Cloud. BR toMissing: " + toMissing.size() +
                " toCorrupt: " + toCorrupt.size() +
                " toDelete: " + toDelete.size()+ " Prefix: "+prefix);
        handleMissingBlocks(toMissing);
        handleCorruptBlocks(toCorrupt);
        handleToDeleteBlocks(toDelete);

        start = end;
      }
      successful = true;
    } catch (Exception e) {
      LOG.warn(e, e);
    } finally {
      return successful;
    }
  }

  public List<ProvidedBlockReportTask> generateTasks(long maxBlkID) {
    List<ProvidedBlockReportTask> tasks = new ArrayList<>();
    long prefixesToScan = (long) Math.ceil((double) maxBlkID / (double) prefixSize);
    long taskSize = (long) Math.ceil((double) prefixesToScan / (double) maxSubTasks);
    long startIndex = 0;
    while (startIndex < maxBlkID) {
      long endIndex = (startIndex) + taskSize * prefixSize;
      ProvidedBlockReportTask task = new ProvidedBlockReportTask(
              startIndex,
              endIndex,
              0, LeaderElection.LEADER_INITIALIZATION_ID);
      tasks.add(task);
      startIndex = endIndex;
    }
    return tasks;
  }

  /**
   * This class is used to pull work from the queue and execute the operation
   */
  class BRTasksPullers implements Callable {
    private int id;
    private  int count;
    BRTasksPullers(int id){
      this.id  = id;
    }
    @Override
    public Object call() throws Exception {
      ProvidedBlockReportTask task = null;
      do {
        task = popPendingBRTask();
        if (task != null) {
          processTask(task);
          count++;
        } else {
          LOG.info("HopsFS-Cloud. BR Worker ID: "+id+" processed "+count+" tasks");
          return null;
        }
      } while (!currentThread().isInterrupted());
      return null;
    }
  }


  /*
   * A block is marked corrupt when
   *    1. If the block's metadata exists in the database, but the block is absent form the
   *       bucket listing.
   *    2. Partial block information was obtained using cloud block listing. In S3 ls operation
   *       is eventually consistent. That is, it is possible that for a block the ls operation
   *       might return the meta block information but miss the actual block, or vice versa.
   *       Such blocks are marked corrupt for the time being
   *    3. The generations stamp, block size or bucket id does not match
   *
   *  A block is marked for deletion if it is in the cloud bucket listing but no corresponding
   *    metadata is present in the database.
   */
  @VisibleForTesting
  public void reportDiff(Map<Long, BlockInfoContiguous> dbView, Map<Long, CloudBlock> cView,
                         List<BlockInfoContiguous> toMissing, List<BlockToMarkCorrupt> toCorrupt,
                         List<CloudBlock> toDelete) throws IOException {
    final Set<Long> aggregatedSafeBlocks = new HashSet<>();
    aggregatedSafeBlocks.addAll(dbView.keySet());

    for (BlockInfoContiguous dbBlock : dbView.values()) {

      CloudBlock cblock = cView.get(dbBlock.getBlockId());
      BlockToMarkCorrupt cb = null;

      //under construction blocks should be handled using normal block reports.
      if(dbBlock instanceof  BlockInfoContiguousUnderConstruction){
        aggregatedSafeBlocks.remove(dbBlock.getBlockId());
        continue;
      }

      if (cblock == null  || cblock.isPartiallyListed()) {
        // if the block is partially listed  or totally missing form the listing
        // then only mark the block missing aka corrupt after Δt, because s3 is eventually
        // consistent
        if ((System.currentTimeMillis() - dbBlock.getTimestamp()) > marksCorruptBlocksDelay) {
          toMissing.add(dbBlock);
          aggregatedSafeBlocks.remove(dbBlock.getBlockId()); //this block is not safe now
        }
        cView.remove(dbBlock.getBlockId());
        continue;
      } else if (cblock.getBlock().getGenerationStamp() != dbBlock.getGenerationStamp()) {
        cb = new BlockToMarkCorrupt(cblock, dbBlock, "Generation stamp mismatch",
                CorruptReplicasMap.Reason.GENSTAMP_MISMATCH);
      } else if (cblock.getBlock().getNumBytes() != dbBlock.getNumBytes()) {
        cb = new BlockToMarkCorrupt(cblock, dbBlock, "Block size mismatch",
                CorruptReplicasMap.Reason.SIZE_MISMATCH);
      } else if (cblock.getBlock().getCloudBucket().compareToIgnoreCase(dbBlock.getCloudBucket()) != 0) {
        cb = new BlockToMarkCorrupt(cblock, dbBlock, "Cloud bucket mismatch",
                CorruptReplicasMap.Reason.INVALID_STATE);
      } else {
        //not corrupt
        cView.remove(dbBlock.getBlockId());
      }

      if (cb != null) {
        toCorrupt.add(cb);
        aggregatedSafeBlocks.remove(dbBlock.getBlockId()); //this block is not safe now
        cView.remove(dbBlock.getBlockId());
      }
    }

    for (CloudBlock cloudBlock : cView.values()) {
      toDelete.add(cloudBlock);
    }

    //safe blocks
    if (ns.isInStartupSafeMode()) {
      LOG.debug("HopsFS-Cloud. BR Aggregated safe block #: " + aggregatedSafeBlocks.size());
      ns.adjustSafeModeBlocks(aggregatedSafeBlocks);
    }
  }

  //In this case missing blocks are also considered corrupt.
  //We will never recover from this state hence corruption
  private void handleMissingBlocks(final List<BlockInfoContiguous> missingBlocks)
          throws IOException {
    for (BlockInfoContiguous blk : missingBlocks) {
      addCorrptUnderReplicatedBlock(blk);
    }
  }

  private void handleCorruptBlocks(List<BlockToMarkCorrupt> corruptBlocks)
          throws IOException {
    for (BlockToMarkCorrupt b : corruptBlocks) {
      addCorrptUnderReplicatedBlock(b.stored);
    }
  }

  private void addCorrptUnderReplicatedBlock(final BlockInfoContiguous block) throws IOException {
    new HopsTransactionalRequestHandler(HDFSOperationType.CLOUD_ADD_CORRUPT_BLOCKS) {
      INodeIdentifier inodeIdentifier;

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        LockFactory lf = LockFactory.getInstance();
        locks.add(lf.getIndividualBlockLock(block.getBlockId(), inodeIdentifier))
                .add(lf.getBlockRelated(LockFactory.BLK.UR));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        bm.neededReplications.add(block, 0, 0, 1);
        return null;
      }
    }.handle();

  }


  private void handleToDeleteBlocks(List<CloudBlock> toDelete) throws IOException {
    List<InvalidatedBlock> invblks = new ArrayList<>();
    for (CloudBlock cblock : toDelete) {
      Block block = cblock.getBlock();
      InvalidatedBlock invBlk = new InvalidatedBlock(
              StorageId.CLOUD_STORAGE_ID,
              block.getBlockId(),
              block.getGenerationStamp(),
              CloudHelper.getCloudBucketID(block.getCloudBucket()),
              block.getNumBytes(),
              INode.NON_EXISTING_INODE_ID);
      invblks.add(invBlk);
    }
    bm.getInvalidateBlocks().addAll(invblks);
  }

  public void shutDown() {
    run = false;
    interrupt();
  }

  public Map<Long, BlockInfoContiguous> findAllBlocksRange(final long startID, final long endID)
          throws IOException {
    LightWeightRequestHandler handler =
            new LightWeightRequestHandler(HDFSOperationType.BR_GET_RANGE_OF_BLOCKS) {
              @Override
              public Object performTask() throws IOException {
                Map<Long, BlockInfoContiguous> blkMap = new HashMap<>();
                BlockInfoDataAccess da = (BlockInfoDataAccess) HdfsStorageFactory
                        .getDataAccess(BlockInfoDataAccess.class);

                List<BlockInfoContiguous> blocks = da.findAllBlocks(startID, endID);
                for (BlockInfoContiguous blk : blocks) {
                  blkMap.put(blk.getBlockId(), blk);
                }
                return blkMap;
              }
            };
    return (Map<Long, BlockInfoContiguous>) handler.handle();
  }

  public class BlockToMarkCorrupt {
    /**
     * The corrupted block in a datanode.
     */
    final CloudBlock corrupted;
    /**
     * The corresponding block stored in the BlockManager.
     */
    final BlockInfoContiguous stored;
    /**
     * The reason to mark corrupt.
     */
    final String reason;
    /**
     * The reason code to be stored
     */
    final CorruptReplicasMap.Reason reasonCode;

    BlockToMarkCorrupt(CloudBlock corrupted,
                       BlockInfoContiguous stored, String reason,
                       CorruptReplicasMap.Reason reasonCode) {
      Preconditions.checkNotNull(stored, "stored is null");

      this.corrupted = corrupted;
      this.stored = stored;
      this.reason = reason;
      this.reasonCode = reasonCode;
    }

    @Override
    public String toString() {
      return stored + " Reason: " + reason;
    }
  }

  public static void scheduleBlockReportNow() throws IOException {
    LOG.debug("HopsFS-Cloud. BR Scheduling a block report now");
    setProvidedBlocksScanStartTime(0L);
  }

  public ProvidedBlockReportTask popPendingBRTask()
          throws IOException {
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(
            HDFSOperationType.BR_POP_TASK) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        //take a lock on HdfsVariables.providedBlocksCheckStartTime to sync
        HdfsStorageFactory.getConnector().writeLock();
        LongVariable var = (LongVariable) Variables.getVariable(
                Variable.Finder.providedBlocksCheckStartTime);
      }

      @Override
      public Object performTask() throws IOException {
        EntityManager.preventStorageCall(false);
        ProvidedBlockReportTasksDataAccess da = (ProvidedBlockReportTasksDataAccess) HdfsStorageFactory
                .getDataAccess(ProvidedBlockReportTasksDataAccess.class);
        ProvidedBlockReportTask task = (ProvidedBlockReportTask) da.popTask();
        LOG.debug("HopsFS-Cloud. BR pulled a task from queue Task: " + task);
        return task;
      }
    };
    return (ProvidedBlockReportTask) handler.handle();
  }

  public long countBRPendingTasks()
          throws IOException {
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(
            HDFSOperationType.BR_COUNT_TASKS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        //take a lock on HdfsVariables.providedBlocksCheckStartTime to sync
        HdfsStorageFactory.getConnector().writeLock();
        LongVariable var = (LongVariable) Variables.getVariable(
                Variable.Finder.providedBlocksCheckStartTime);
      }

      @Override
      public Object performTask() throws IOException {
        EntityManager.preventStorageCall(false);
        ProvidedBlockReportTasksDataAccess da = (ProvidedBlockReportTasksDataAccess) HdfsStorageFactory
                .getDataAccess(ProvidedBlockReportTasksDataAccess.class);
        return da.count();
      }
    };
    return (long) handler.handle();
  }

  public List<ProvidedBlockReportTask> getAllTasks()
          throws IOException {
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(
            HDFSOperationType.BR_GET_ALL_TASKS) {

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        //take a lock on HdfsVariables.providedBlocksCheckStartTime to sync
        HdfsStorageFactory.getConnector().writeLock();
        LongVariable var = (LongVariable) Variables.getVariable(
                Variable.Finder.providedBlocksCheckStartTime);
      }

      @Override
      public Object performTask() throws IOException {
        EntityManager.preventStorageCall(false);
        ProvidedBlockReportTasksDataAccess da = (ProvidedBlockReportTasksDataAccess) HdfsStorageFactory
                .getDataAccess(ProvidedBlockReportTasksDataAccess.class);
        return da.getAllTasks();
      }
    };
    return (List<ProvidedBlockReportTask>) handler.handle();
  }

  public List<ProvidedBlockReportTask> addNewBlockReportTasks(
          final List<ProvidedBlockReportTask> tasks)
          throws IOException {
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(
            HDFSOperationType.BR_ADD_TASKS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        //take a lock on HdfsVariables.providedBlocksCheckStartTime to sync
        HdfsStorageFactory.getConnector().writeLock();
        LongVariable var = (LongVariable) Variables.getVariable(
                Variable.Finder.providedBlocksCheckStartTime);
      }

      @Override
      public Object performTask() throws IOException {
        EntityManager.preventStorageCall(false);
        ProvidedBlockReportTasksDataAccess da = (ProvidedBlockReportTasksDataAccess) HdfsStorageFactory
                .getDataAccess(ProvidedBlockReportTasksDataAccess.class);
        da.addTasks(tasks);

        if(LOG.isDebugEnabled()) {
          for (ProvidedBlockReportTask task : tasks) {
            LOG.debug("HopsFS-Cloud. BR Added new block report tasks " + task);
          }
        }

        long startTime = System.currentTimeMillis();
        Variables.updateVariable(
                new LongVariable(
                        Variable.Finder.providedBlocksCheckStartTime,
                        startTime
                ));
        if(LOG.isDebugEnabled()){
          LOG.debug("HopsFS-Cloud. BR set start time to : " +
                  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(startTime)));
        }

        long counter = (long) Variables.getVariable(
                Variable.Finder.providedBlockReportsCount).getValue();
        Variables.updateVariable(
                new LongVariable(
                        Variable.Finder.providedBlockReportsCount,
                        counter + 1
                ));
        LOG.debug("HopsFS-Cloud. BR set counter to : " + (counter + 1));

        return null;
      }
    };
    return (List<ProvidedBlockReportTask>) handler.handle();
  }

  public long getProvidedBlockReportsCount()
          throws IOException {
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(
            HDFSOperationType.BR_COUNT_TASKS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        //take a lock on HdfsVariables.providedBlocksCheckStartTime to sync
        HdfsStorageFactory.getConnector().writeLock();
        LongVariable var = (LongVariable) Variables.getVariable(
                Variable.Finder.providedBlocksCheckStartTime);
      }

      @Override
      public Object performTask() throws IOException {
        EntityManager.preventStorageCall(false);
        long counter = (long) Variables.getVariable(
                Variable.Finder.providedBlockReportsCount).getValue();
        LOG.debug("HopsFS-Cloud. BR get counter : " + counter);

        return counter;
      }
    };
    return (Long) handler.handle();
  }

  public long getProvidedBlocksScanStartTime()
          throws IOException {
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(
            HDFSOperationType.GET_PROVIDED_BLOCK_CHECK_START_TIME) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        //take a lock on HdfsVariables.providedBlocksCheckStartTime to sync
        HdfsStorageFactory.getConnector().writeLock();
        LongVariable var = (LongVariable) Variables.getVariable(
                Variable.Finder.providedBlocksCheckStartTime);
      }

      @Override
      public Object performTask() throws IOException {
        EntityManager.preventStorageCall(false);
        long time = (long) Variables.getVariable(
                Variable.Finder.providedBlocksCheckStartTime).getValue();

        if(LOG.isDebugEnabled()) {
          LOG.trace("HopsFS-Cloud. BR get start time: " +
                  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time)));
        }

        return time;
      }
    };
    return (Long) handler.handle();
  }

  private static void setProvidedBlocksScanStartTime(final long newTime)
          throws IOException {
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(
            HDFSOperationType.UPDATE_PROVIDED_BLOCK_CHECK_START_TIME) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        //take a lock on HdfsVariables.providedBlocksCheckStartTime to sync
        HdfsStorageFactory.getConnector().writeLock();
        LongVariable var = (LongVariable) Variables.getVariable(
                Variable.Finder.providedBlocksCheckStartTime);
      }

      @Override
      public Object performTask() throws IOException {
        EntityManager.preventStorageCall(false);
        Variables.updateVariable(
                new LongVariable(
                        Variable.Finder.providedBlocksCheckStartTime,
                        newTime
                ));
        LOG.debug("HopsFS-Cloud. BR set start time to: " + newTime);
        return null;
      }
    };
    handler.handle();
  }

  public static void deleteAllTask()
          throws IOException {
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(
            HDFSOperationType.BR_DELETE_ALL_TASKS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        //take a lock on HdfsVariables.providedBlocksCheckStartTime to sync
        HdfsStorageFactory.getConnector().writeLock();
        LongVariable var = (LongVariable) Variables.getVariable(
                Variable.Finder.providedBlocksCheckStartTime);
      }

      @Override
      public Object performTask() throws IOException {
        EntityManager.preventStorageCall(false);
        ProvidedBlockReportTasksDataAccess da = (ProvidedBlockReportTasksDataAccess) HdfsStorageFactory
                .getDataAccess(ProvidedBlockReportTasksDataAccess.class);
        da.deleteAll();
        LOG.debug("HopsFS-Cloud. BR Deleted all tasks");
        return null;
      }
    };
    handler.handle();
  }

  void checkAbandonedBlocks() throws IOException {
    for (ActiveMultipartUploads upload : cloudConnector.
            listMultipartUploads(Lists.newArrayList(CloudHelper.getAllBuckets().keySet()))) {
      long elapsedTime = System.currentTimeMillis() - upload.getStartTime();
      if (elapsedTime > deleteAbandonedBlocksAfter) {
        cloudConnector.abortMultipartUpload(upload.getBucket(), upload.getObjectID(), upload.getUploadID());
      }
    }
  }

  public boolean isBRInProgress() {
    return isBRInProgress;
  }
}

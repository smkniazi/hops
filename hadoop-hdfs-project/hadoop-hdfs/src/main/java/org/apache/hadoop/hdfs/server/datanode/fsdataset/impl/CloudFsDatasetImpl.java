package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.amazonaws.services.s3.model.PartETag;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CloudProvider;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.CloudHelper;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.CloudPersistenceProvider;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.util.DataChecksum;

public class CloudFsDatasetImpl extends FsDatasetImpl {
  /**
   * An FSDataset has a directory where it loads its data files.
   *
   * @param datanode
   * @param storage
   * @param conf
   */
  public static final String GEN_STAMP = "GEN_STAMP";
  public static final String OBJECT_SIZE = "OBJECT_SIZE";

  static final Log LOG = LogFactory.getLog(CloudFsDatasetImpl.class);
  private CloudPersistenceProvider cloud;
  private final boolean bypassCache;
  private final int prefixSize;
  private ExecutorService threadPoolExecutor;

  CloudFsDatasetImpl(DataNode datanode, DataStorage storage,
                     Configuration conf) throws IOException {
    super(datanode, storage, conf);
    bypassCache = conf.getBoolean(DFSConfigKeys.DFS_DN_CLOUD_BYPASS_CACHE_KEY,
            DFSConfigKeys.DFS_DN_CLOUD_BYPASS_CACHE_DEFAULT);
    prefixSize = conf.getInt(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY,
            DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_DEFAULT);

    cloud = CloudPersistenceProviderFactory.getCloudClient(conf);
    cloud.checkAllBuckets(CloudHelper.getBucketsFromConf(conf));
    threadPoolExecutor = Executors.newFixedThreadPool(cloud.getXferThreads());
  }

  @Override
  public void preFinalize(ExtendedBlock b) throws IOException {
    if (!b.isProvidedBlock()) {
      super.preFinalize(b);
    } else {
      // upload to cloud
      preFinalizeInternal(b);
    }
  }

  public void preFinalizeInternal(ExtendedBlock b) throws IOException {
    LOG.debug("HopsFS-Cloud. Prefinalize Stage. Uploading... Block: " + b.getLocalBlock());

    ReplicaInfo replicaInfo = getReplicaInfo(b);
    boolean isMultiPart = false;

    if (replicaInfo instanceof ProvidedReplicaBeingWritten) {
      isMultiPart = ((ProvidedReplicaBeingWritten) replicaInfo).isMultipart();
    }

    File blockFile = replicaInfo.getBlockFile();
    File metaFile = replicaInfo.getMetaFile();
    String blockFileKey = CloudHelper.getBlockKey(prefixSize, b.getLocalBlock());
    String metaFileKey = CloudHelper.getMetaFileKey(prefixSize, b.getLocalBlock());

    if (isMultiPart) {
      while (!((ProvidedReplicaBeingWritten) (replicaInfo)).isMultipartComplete()) {
        try {
          Thread.sleep(30);
        } catch (InterruptedException e) {
        }
      }
    } else {
      if (!cloud.objectExists(b.getCloudBucket(), blockFileKey)) {
        cloud.uploadObject(b.getCloudBucket(), blockFileKey, blockFile,
                getBlockFileMetadata(b.getLocalBlock()));
      } else {
        LOG.error("HopsFS-Cloud. Block: " + b + " alreay exists.");
        throw new IOException("Block: " + b + " alreay exists.");
      }
    }

    if (!cloud.objectExists(b.getCloudBucket(), metaFileKey)) {
      cloud.uploadObject(b.getCloudBucket(), metaFileKey, metaFile,
              getMetaMetadata(b.getLocalBlock()));
    } else {
      LOG.error("HopsFS-Cloud. Block: " + b + " meta file alreay exists.");
      throw new IOException("Block: " + b + " meta file alreay exists.");
    }
  }

  @Override
  public synchronized void finalizeBlock(ExtendedBlock b) throws IOException {
    if (!b.isProvidedBlock()) {
      super.finalizeBlock(b);
    } else {
      finalizeBlockInternal(b);
    }
  }

  private synchronized void finalizeBlockInternal(ExtendedBlock b) throws IOException {
    LOG.debug("HopsFS-Cloud. Finalizing bloclk. Block: " + b.getLocalBlock());
    if (Thread.interrupted()) {
      // Don't allow data modifications from interrupted threads
      throw new IOException("Cannot finalize block from Interrupted Thread");
    }

    ReplicaInfo replicaInfo = getReplicaInfo(b);
    File blockFile = replicaInfo.getBlockFile();
    File metaFile = replicaInfo.getMetaFile();
    long dfsBytes = blockFile.length() + metaFile.length();

    // release rbw space
    FsVolumeImpl v = (FsVolumeImpl) replicaInfo.getVolume();
    v.releaseReservedSpace(replicaInfo.getBytesReserved());
    v.decDfsUsed(b.getBlockPoolId(), dfsBytes);

    // remove from volumeMap, so we can get it from s3 instead
    volumeMap.remove(b.getBlockPoolId(), replicaInfo.getBlockId());

    if (bypassCache) {
      blockFile.delete();
      metaFile.delete();
    } else {
      //move the blocks to the cache
      FsVolumeImpl cloudVol = getCloudVolume();
      File cDir = cloudVol.getCacheDir(b.getBlockPoolId());

      File movedBlock = new File(cDir, CloudHelper.getBlockKey(prefixSize, b.getLocalBlock()));
      File movedBlockParent = new File(movedBlock.getParent());
      if (!movedBlockParent.exists()) {
        movedBlockParent.mkdir();
      }

      if (!blockFile.renameTo(movedBlock)) {
        LOG.warn("HopsFS-Cloud. Unable to move finalized block to cache. src: "
                + blockFile.getAbsolutePath() + " dst: " + movedBlock.getAbsolutePath());
        blockFile.delete();
      } else {
        providedBlocksCacheUpdateTS(b.getBlockPoolId(), movedBlock);
        LOG.debug("HopsFS-Cloud. Moved " + movedBlock.getName() + " to cache. path " + movedBlock);
      }

      File movedMetaFile = new File(cDir, CloudHelper.getMetaFileKey(prefixSize,
              b.getLocalBlock()));
      File movedMetaFileParent = new File(movedMetaFile.getParent());
      if (!movedMetaFileParent.exists()) {
        movedMetaFileParent.mkdir();
      }

      if (!metaFile.renameTo(movedMetaFile)) {
        LOG.warn("HopsFS-Cloud. Unable to move finalized block meta file to cache. src: "
                + blockFile.getAbsolutePath() + " dst: " + movedBlock.getAbsolutePath());
        metaFile.delete();
      } else {
        providedBlocksCacheUpdateTS(b.getBlockPoolId(), movedMetaFile);
        LOG.debug("HopsFS-Cloud. Moved " + movedMetaFile.getName() + " to cache. path : " + movedMetaFile);
      }
    }
  }

  @Override // FsDatasetSpi
  public InputStream getBlockInputStream(ExtendedBlock b, long seekOffset)
          throws IOException {

    if (!b.isProvidedBlock()) {
      return super.getBlockInputStream(b, seekOffset);
    } else {

      LOG.debug("HopsFS-Cloud. Get block inputstream " + b.getLocalBlock());
      if (b.isProvidedBlock() && volumeMap.get(b.getBlockPoolId(), b.getBlockId()) != null) {
        return super.getBlockInputStream(b, seekOffset);
      } else {
        FsVolumeImpl cloudVolume = getCloudVolume();
        File localBlkCopy = new File(cloudVolume.getCacheDir(b.getBlockPoolId()),
                CloudHelper.getBlockKey(prefixSize, b.getLocalBlock()));
        String blockFileKey = CloudHelper.getBlockKey(prefixSize, b.getLocalBlock());

        return getInputStreamInternal(b.getCloudBucket(), blockFileKey,
                localBlkCopy, b.getBlockPoolId(), seekOffset);
      }
    }
  }

  @Override // FsDatasetSpi
  public LengthInputStream getMetaDataInputStream(ExtendedBlock b)
          throws IOException {
    if (!b.isProvidedBlock()) {
      return super.getMetaDataInputStream(b);
    } else {
      FsVolumeImpl cloudVolume = getCloudVolume();
      String metaFileKey = CloudHelper.getMetaFileKey(prefixSize, b.getLocalBlock());
      File localMetaFileCopy = new File(cloudVolume.getCacheDir(b.getBlockPoolId()),
              CloudHelper.getMetaFileKey(prefixSize, b.getLocalBlock()));

      InputStream is = getInputStreamInternal(b.getCloudBucket(), metaFileKey,
              localMetaFileCopy, b.getBlockPoolId(), 0);
      LengthInputStream lis = new LengthInputStream(is, localMetaFileCopy.length());

      return lis;
    }
  }

  private InputStream getInputStreamInternal(String cloudBucket, String objectKey,
                                             File localCopy, String bpid,
                                             long seekOffset) throws IOException {
    try {
      //check if object exists
      long startTime = System.currentTimeMillis();

      boolean download = bypassCache;
      if (!bypassCache) {
        if (!(localCopy.exists() && localCopy.length() > 0)) {
          localCopy.delete();
          download = true;
        } else {
          LOG.debug("HopsFS-Cloud. Reading provided block from cache. Block: " + objectKey);
        }
      }
      if (download) {
        cloud.downloadObject(cloudBucket, objectKey, localCopy);
      }

      InputStream ioStream = new FileInputStream(localCopy);
      ioStream.skip(seekOffset);

      providedBlocksCacheUpdateTS(bpid, localCopy);  //after opening the file put it in the cache

      LOG.debug("HopsFS-Cloud. " + objectKey + " GetInputStream Fn took :" + (System.currentTimeMillis() - startTime));
      return ioStream;
    } catch (IOException e) {
      throw new IOException("Could not read " + objectKey + ". ", e);
    }
  }

  @Override // FsDatasetSpi
  public ReplicaInfo getReplica(ExtendedBlock b) {
    if (!b.isProvidedBlock()) {
      return super.getReplica(b);
    } else if (b.isProvidedBlock() && volumeMap.get(b.getBlockPoolId(), b.getBlockId()) != null) {
      return super.getReplica(b);
    } else {
      return getReplicaInternal(b);
    }
  }

  public ReplicaInfo getReplicaInternal(ExtendedBlock b) {
    ReplicaInfo replicaInfo = super.getReplica(b);
    if (replicaInfo != null) {
      return replicaInfo;
    }

    try {
      String metaFileKey = CloudHelper.getMetaFileKey(prefixSize, b.getLocalBlock());
      if (cloud.objectExists(b.getCloudBucket(), metaFileKey)) {

        Map<String, String> metadata = cloud.getUserMetaData(b.getCloudBucket(), metaFileKey);

        long genStamp = Long.parseLong(metadata.get(GEN_STAMP));
        long size = Long.parseLong(metadata.get(OBJECT_SIZE));

        FinalizedReplica info = new FinalizedReplica(b.getBlockId(), size, genStamp,
                b.getCloudBucket(), null, null);
        return info;
      }

    } catch (IOException up) {
      LOG.info(up, up);
    }
    return null;
  }

  // Finalized provided blocks are removed from the replica map
  public boolean isProvideBlockFinalized(ExtendedBlock b) {
    assert b.isProvidedBlock();
    return super.getReplica(b) == null ? true : false;
  }


  private String getCloudProviderName() {
    return conf.get(DFSConfigKeys.DFS_CLOUD_PROVIDER,
            DFSConfigKeys.DFS_CLOUD_PROVIDER_DEFAULT);
  }

  @Override
  FsVolumeImpl getNewFsVolumeImpl(FsDatasetImpl dataset, String storageID,
                                  File currentDir, Configuration conf,
                                  StorageType storageType) throws IOException {
    if (storageType == StorageType.CLOUD) {
      if (getCloudProviderName().compareToIgnoreCase(CloudProvider.AWS.name()) == 0) {
        return new CloudFsVolumeImpl(this, storageID, currentDir, conf, storageType);
      } else {
        throw new UnsupportedOperationException("Cloud provider '" +
                getCloudProviderName() + "' is not supported");
      }
    } else {
      return new FsVolumeImpl(this, storageID, currentDir, conf, storageType);
    }
  }

  /**
   * We're informed that a block is no longer valid.  We
   * could lazily garbage-collect the block, but why bother?
   * just get rid of it.
   */
  @Override // FsDatasetSpi
  public void invalidate(String bpid, Block invalidBlks[]) throws IOException {

    final List<String> errors = new ArrayList<String>();

    for (Block b : invalidBlks) {
      if (b.isProvidedBlock() && volumeMap.get(bpid, b.getBlockId()) != null) {
        super.invalidateBlock(bpid, b, errors);
      } else {
        invalidateProvidedBlock(bpid, b, errors);
      }
    }

    printInvalidationErrors(errors, invalidBlks.length);
  }

  private void invalidateProvidedBlock(String bpid, Block invalidBlk, List<String> errors)
          throws IOException {
    final File f;
    final FsVolumeImpl v;
    ReplicaInfo info;
    info = volumeMap.get(bpid, invalidBlk);

    // case when the block is not yet uploaded to the cloud
    if (info != null) {
      super.invalidateBlock(bpid, invalidBlk, errors);
    } else {
      // block is in the cloud.
      // Edge cases such as deletion of be blocks in flight
      // should be taekn care of by the block reporting system

      FsVolumeImpl cloudVolume = getCloudVolume();

      if (cloudVolume == null) {
        errors.add("HopsFS-Cloud. Failed to delete replica " + invalidBlk);
      }

      File localBlkCopy = new File(cloudVolume.getCacheDir(bpid),
              CloudHelper.getBlockKey(prefixSize, invalidBlk));
      File localMetaFileCopy = new File(cloudVolume.getCacheDir(bpid),
              CloudHelper.getMetaFileKey(prefixSize, invalidBlk));

      LOG.info("HopsFS-Cloud. Scheduling async deletion of block: " + invalidBlk);
      File volumeDir = cloudVolume.getCurrentDir();
      asyncDiskService.deleteAsyncProvidedBlock(new ExtendedBlock(bpid, invalidBlk),
              cloud, localBlkCopy, localMetaFileCopy, volumeDir);
    }
  }

  @Override
  FinalizedReplica updateReplicaUnderRecovery(
          String bpid,
          ReplicaUnderRecovery rur,
          long recoveryId,
          long newBlockId,
          long newlength,
          String cloudBucket) throws IOException {
    LOG.info("HopsFS-Cloud. update replica under recovery rur: "+rur);

    if(!rur.isProvidedBlock()){
      return super.updateReplicaUnderRecovery(bpid, rur, recoveryId, newBlockId, newlength,
              cloudBucket);
    }

    boolean uploadedToTheCloud = true;
    ReplicaInfo ri = volumeMap.get(bpid, rur.getBlockId());
    if (ri != null) {   //the block is open
      try {
        checkReplicaFilesInternal(ri);
        uploadedToTheCloud = true;
      } catch (IOException e) {
        super.checkReplicaFiles(ri);
        uploadedToTheCloud = false;
      }
    }

    if (!uploadedToTheCloud) {
      if (ri instanceof ProvidedReplicaUnderRecovery && // upload is in progress using multipart api
              ((ProvidedReplicaUnderRecovery) ri).isPartiallyUploaded()) {
        String blockFileKey = CloudHelper.getBlockKey(prefixSize,
                ((ProvidedReplicaUnderRecovery) ri).getBlock() );
        cloud.abortMultipartUpload( ri.getCloudBucket(), blockFileKey,
                ((ProvidedReplicaUnderRecovery) ri).getUploadID());
      }

      FinalizedReplica fr = super.updateReplicaUnderRecovery(bpid, rur, recoveryId,
              newBlockId, newlength, cloudBucket);

      uploadFinalizedBlockToCloud(bpid, fr);
      return fr;
    } else {
      return updateReplicaUnderRecoveryInternal(bpid, rur, recoveryId,
              newBlockId, newlength, cloudBucket);
    }
  }

  private void uploadFinalizedBlockToCloud(String bpid, FinalizedReplica fr) throws IOException {
    ExtendedBlock eb = new ExtendedBlock(bpid, new Block(fr.getBlockId(), fr.getVisibleLength(),
            fr.getGenerationStamp(), fr.getCloudBucket()));
    preFinalizeInternal(eb);
    finalizeBlockInternal(eb);
  }

  FinalizedReplica updateReplicaUnderRecoveryInternal(
          String bpid,
          ReplicaUnderRecovery rur,
          long recoveryId,
          long newBlockId,
          long newlength,
          String cloudBlock) throws IOException {
    //check recovery id
    if (rur.getRecoveryID() != recoveryId) {
      throw new IOException("rur.getRecoveryID() != recoveryId = " +
              recoveryId + ", rur=" + rur);
    }

    boolean copyOnTruncate = newBlockId > 0L && rur.getBlockId() != newBlockId;
    if (copyOnTruncate == true) {
      throw new UnsupportedOperationException("Truncate using copy is not supported");
    }

    // Create new truncated block with truncated data and bump up GS
    //update length
    if (rur.getNumBytes() < newlength) {
      throw new IOException(
              "rur.getNumBytes() < newlength = " + newlength + ", rur=" + rur);
    }

    LOG.info("HopsFS-Cloud. update replica under recovery rur: "+rur+". Creating a new replica " +
            "in the cloud");

    if (rur.getNumBytes() >= newlength) { // Create a new block even if zero bytes are truncated,
      // because GS needs to be increased.
      truncateProvidedBlock(bpid, rur, rur.getNumBytes(), newlength, recoveryId);
      // update RUR with the new length
      rur.setNumBytesNoPersistance(newlength);
      rur.setGenerationStampNoPersistance(recoveryId);
    }

    return new FinalizedReplica(rur, null, null);
  }

  private void truncateProvidedBlock(String bpid, ReplicaInfo rur, long oldlen,
                                     long newlen, long newGS) throws IOException {
    LOG.info("HopsFS-Cloud. Truncating a block: " + rur.getBlockId() + "_" + rur.getGenerationStamp());

    Block bOld = new Block(rur.getBlockId(), rur.getNumBytes(), rur.getGenerationStamp(),
            rur.getCloudBucket());
    String oldBlkKey = CloudHelper.getBlockKey(prefixSize, bOld);
    String oldBlkMetaKey = CloudHelper.getMetaFileKey(prefixSize, bOld);

    if (newlen > oldlen) {
      throw new IOException("Cannot truncate block to from oldlen (=" + oldlen +
              ") to newlen (=" + newlen + ")");
    }

    //download the block
    FsVolumeImpl vol = getCloudVolume();
    File blockFile = new File(vol.getCacheDir(bpid), oldBlkKey);
    File metaFile = new File(vol.getCacheDir(bpid), oldBlkMetaKey);

    if (!(blockFile.exists() && blockFile.length() == bOld.getNumBytes())) {
      blockFile.delete(); //delete old files if any
      cloud.downloadObject(rur.getCloudBucket(), oldBlkKey, blockFile);
      providedBlocksCacheUpdateTS(bpid, blockFile);
    }

    if (!(metaFile.exists() && metaFile.length() > 0)) {
      metaFile.delete(); //delete old files if any
      cloud.downloadObject(rur.getCloudBucket(), oldBlkMetaKey, metaFile);
      providedBlocksCacheUpdateTS(bpid, metaFile);
    }

    //truncate the disk block and update the metafile
    DataChecksum dcs = BlockMetadataHeader.readHeader(metaFile).getChecksum();
    int checksumsize = dcs.getChecksumSize();
    int bpc = dcs.getBytesPerChecksum();
    long n = (newlen - 1) / bpc + 1;
    long newmetalen = BlockMetadataHeader.getHeaderSize() + n * checksumsize;
    long lastchunkoffset = (n - 1) * bpc;
    int lastchunksize = (int) (newlen - lastchunkoffset);
    byte[] b = new byte[Math.max(lastchunksize, checksumsize)];

    RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
    try {
      //truncate blockFile
      blockRAF.setLength(newlen);

      //read last chunk
      blockRAF.seek(lastchunkoffset);
      blockRAF.readFully(b, 0, lastchunksize);
    } finally {
      blockRAF.close();
    }

    //compute checksum
    dcs.update(b, 0, lastchunksize);
    dcs.writeValue(b, 0, false);

    //update metaFile
    RandomAccessFile metaRAF = new RandomAccessFile(metaFile, "rw");
    try {
      metaRAF.setLength(newmetalen);
      metaRAF.seek(newmetalen - checksumsize);
      metaRAF.write(b, 0, checksumsize);
    } finally {
      metaRAF.close();
    }

    //update the blocks
    LOG.info("HopsFS-Cloud. Truncated on disk copy of the block: " + bOld);

    Block bNew = new Block(rur.getBlockId(), newlen, newGS, rur.getCloudBucket());
    String newBlkKey = CloudHelper.getBlockKey(prefixSize, bNew);
    String newBlkMetaKey = CloudHelper.getMetaFileKey(prefixSize, bNew);

    if (!cloud.objectExists(rur.getCloudBucket(), newBlkKey)
            && !cloud.objectExists(rur.getCloudBucket(), newBlkMetaKey)) {
      LOG.info("HopsFS-Cloud. Uploading Truncated Block: " + bNew);
      cloud.uploadObject(rur.getCloudBucket(), newBlkKey, blockFile,
              getBlockFileMetadata(bNew));
      cloud.uploadObject(rur.getCloudBucket(), newBlkMetaKey, metaFile,
              getMetaMetadata(bNew));
    } else {
      LOG.error("HopsFS-Cloud. Block: " + b + " alreay exists.");
      throw new IOException("Block: " + b + " alreay exists.");
    }

    LOG.info("HopsFS-Cloud. Deleting old block from cloud. Block: " + bOld);
    cloud.deleteObject(rur.getCloudBucket(), oldBlkKey);
    cloud.deleteObject(rur.getCloudBucket(), oldBlkMetaKey);

    LOG.info("HopsFS-Cloud. Deleting disk tmp copy: " + bOld);
    blockFile.delete();
    metaFile.delete();

    //remove the entry from replica map
    volumeMap.remove(bpid, bNew.getBlockId());
  }

  @Override
  public void checkReplicaFiles(final ReplicaInfo r) throws IOException {
    //the block files has to be somewhere, either in the cloud on disk and case of non finalized
    // blocks

    try {
      checkReplicaFilesInternal(r);
    } catch (IOException e) {
      super.checkReplicaFiles(r);
    }
  }

  public void checkReplicaFilesInternal(final ReplicaInfo r) throws IOException {
    //check replica's file
    // make sure that the block and the meta objects exist in S3.
    Block b = new Block(r.getBlockId(), r.getNumBytes(),
            r.getGenerationStamp(), r.getCloudBucket());
    String blockKey = CloudHelper.getBlockKey(prefixSize, b);
    String metaKey = CloudHelper.getMetaFileKey(prefixSize, b);

    if (!cloud.objectExists(r.getCloudBucket(), blockKey)) {
      throw new IOException("Block: " + b + " not found in the cloud storage");
    }

    long blockSize = cloud.getObjectSize(r.getCloudBucket(), blockKey);
    if (blockSize != r.getNumBytes()) {
      throw new IOException(
              "File length mismatched. Expected: " + r.getNumBytes() + " Got: " + blockSize);
    }

    if (!cloud.objectExists(r.getCloudBucket(), metaKey)) {
      throw new IOException("Meta Object for Block: " + b + " not found in the cloud " +
              "storage");
    }

    long metaFileSize = cloud.getObjectSize(r.getCloudBucket(), metaKey);
    if (metaFileSize == 0) {
      throw new IOException("Metafile is empty. Block: " + b);
    }
  }

  @Override
  public synchronized FsVolumeImpl getVolume(final ExtendedBlock b) {

    if (!b.isProvidedBlock()) {
      return super.getVolume(b);
    } else {
      return getVolumeInternal(b);
    }
  }

  @Override // FsDatasetSpi
  public Map<DatanodeStorage, BlockReport> getBlockReports(String bpid) {
    return super.getBlockReports(bpid);
  }

  public synchronized FsVolumeImpl getVolumeInternal(final ExtendedBlock b) {
    if (!b.isProvidedBlock()) {
      return super.getVolume(b);

    } else {
      return getCloudVolume();
    }
  }

  @Override
  public void shutdown() {
    super.shutdown();
    cloud.shutdown();
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaHandler createRbw(
          StorageType storageType, ExtendedBlock b)
          throws IOException {
    ReplicaHandler handler = super.createRbw(storageType,b);
    FsVolumeReference ref = handler.getVolumeReference();
    ProvidedReplicaBeingWritten providedReplicaBeingWritten = new
            ProvidedReplicaBeingWritten((ReplicaBeingWritten)handler.getReplica(),
            cloud.getPartSize());
    volumeMap.add(b.getBlockPoolId(), providedReplicaBeingWritten);
    return new ReplicaHandler(providedReplicaBeingWritten, ref);
  }

  public void uploadPart(ExtendedBlock b) throws IOException {
    ProvidedReplicaBeingWritten pReplicaInfo = (ProvidedReplicaBeingWritten) getReplicaInfo(b);
    if(!pReplicaInfo.isPartAvailable()){
      throw new IOException("Not enough data available for multipart upload");
    }

    String blockKey = CloudHelper.getBlockKey(prefixSize, b.getLocalBlock());
    int partId = pReplicaInfo.incrementAndGetNextPart();
    if(partId == 1){
      if (!cloud.objectExists(b.getCloudBucket(), blockKey)){
        String uploadID = cloud.startMultipartUpload(b.getCloudBucket(), blockKey,
                getBlockFileMetadata(b.getLocalBlock()));
        pReplicaInfo.setUploadID(uploadID);
        pReplicaInfo.setMultipart(true);
      } else {
        LOG.error("HopsFS-Cloud. Block: " + b + " alreay exists.");
        throw new IOException("Block: " + b + " alreay exists.");
      }
    }

    File blockFile = pReplicaInfo.getBlockFile();
    long start  = (partId - 1) * pReplicaInfo.getPartSize();
    long end  = (partId) * pReplicaInfo.getPartSize();
    PartUploadWorker worker = new PartUploadWorker(b.getCloudBucket(), blockKey, pReplicaInfo.getUploadID(),
            partId, blockFile, start, end);
    pReplicaInfo.addUploadTask(threadPoolExecutor.submit(worker));
  }

  public void finalizeMultipartUpload(ExtendedBlock b) throws IOException {
    ProvidedReplicaBeingWritten pReplicaInfo = (ProvidedReplicaBeingWritten) getReplicaInfo(b);

    assert pReplicaInfo.isMultipart();

    //upload remaining data as single part
    long currentPart = pReplicaInfo.getCurrentPart();
    String blockKey = CloudHelper.getBlockKey(prefixSize, b.getLocalBlock());
    if( pReplicaInfo.getBytesOnDisk() > (currentPart * pReplicaInfo.getPartSize())){
      File blockFile = pReplicaInfo.getBlockFile();
      int newPartID = pReplicaInfo.incrementAndGetNextPart();
      long start  = (currentPart) * pReplicaInfo.getPartSize();
      long end  = pReplicaInfo.getBytesOnDisk();

      PartUploadWorker worker = new PartUploadWorker(b.getCloudBucket(), blockKey,
              pReplicaInfo.getUploadID(),
              newPartID, blockFile, start, end);
      pReplicaInfo.addUploadTask(threadPoolExecutor.submit(worker));
    }

    waitForPartsUpload(pReplicaInfo);

    cloud.finalizeMultipartUpload(b.getCloudBucket(), blockKey, pReplicaInfo.getUploadID(),
            pReplicaInfo.getPartETags());
    pReplicaInfo.setMultipartComplete(true);

    LOG.info("HopsFS-Cloud. Finalized the multipart upload ");
  }

  private void waitForPartsUpload(ProvidedReplicaBeingWritten prbw) throws IOException {
    for(Future future : prbw.getAllUploadTasks()){
      try{
        PartETag tag = (PartETag) future.get();
        prbw.addEtag(tag);
      } catch (ExecutionException e) {
        LOG.error("Exception was thrown during uploading a block to cloud", e);
        Throwable throwable = e.getCause();
        if (throwable instanceof IOException) {
          throw (IOException) throwable;
        } else {
          throw new IOException(e);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }

  class PartUploadWorker implements Callable{
    private final String bucket;
    private final String key;
    private final String uploadID;
    private final int partID;
    private final File file;
    private final long startPos;
    private final long endPos;

    PartUploadWorker(String bucket, String key, String uploadID, int partID, File file,
                     long startPos, long endPos){
      this.bucket = bucket;
      this.key = key;
      this.uploadID = uploadID;
      this.partID = partID;
      this.file = file;
      this.startPos = startPos;
      this.endPos = endPos;
    }

    @Override
    public Object call() throws Exception {
      PartETag etag = cloud.uploadPart(bucket, key, uploadID,
              partID, file, startPos, endPos);
      LOG.info("HopsFS-Cloud. Part id to upload "+partID+
              " start "+startPos+" end "+endPos+ " payload size "+(endPos-startPos));
      return etag;
    }
  }

  @VisibleForTesting
  public FsVolumeImpl getCloudVolume() {
    for (FsVolumeImpl vol : getVolumes()) {
      if (vol.getStorageType() == StorageType.CLOUD) {
        return vol;
      }
    }
    return null;
  }

  private Map<String, String> getBlockFileMetadata(Block b) {
    Map<String, String> metadata = new HashMap<>();
    return metadata;
  }

  private Map<String, String> getMetaMetadata(Block b) {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(GEN_STAMP, Long.toString(b.getGenerationStamp()));
    metadata.put(OBJECT_SIZE, Long.toString(b.getNumBytes()));
    return metadata;
  }

  public void providedBlocksCacheUpdateTS(String bpid, File f) throws IOException {
    FsVolumeImpl cloudVolume = getCloudVolume();
    cloudVolume.getBlockPoolSlice(bpid).fileAccessed(f);
  }

  public void providedBlocksCacheDelete(String bpid, File f) throws IOException {
    FsVolumeImpl cloudVolume = getCloudVolume();
    cloudVolume.getBlockPoolSlice(bpid).fileDeleted(f);
  }

  @VisibleForTesting
  public CloudPersistenceProvider getCloudConnector() {
    return cloud;
  }

  @VisibleForTesting
  public void installMockCloudConnector(CloudPersistenceProvider mock) {
    cloud = mock;
  }
}


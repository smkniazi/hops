package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystemCommon;
import org.apache.hadoop.fs.s3a.UploadInfo;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.ReflectionUtils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;

import static org.apache.hadoop.hdfs.protocol.Block.BLOCK_FILE_PREFIX;
import static org.apache.hadoop.hdfs.protocol.Block.METADATA_EXTENSION;

public class S3DatasetImpl extends FsDatasetImpl {
    private String bucket;
    private S3AFileSystemCommon s3afs;
    private DatanodeProtocolClientSideTranslatorPB namenode;
    
    // Map of block pool Id to another map of block Id to S3FinalizedReplica
    private Map<String, Map<Long, S3FinalizedReplica>> downloadedBlocksMap = new HashMap<>();
    
    /**
     * An FSDataset has a directory where it loads its data files.
     *
     * @param datanode
     * @param storage
     * @param conf
     */ 
    
    S3DatasetImpl(DataNode datanode, DataStorage storage, Configuration conf) throws IOException {
        super(datanode, storage, conf);
        // create new voluemMap that checks S3 if block is not found locally
        super.volumeMap = new S3ReplicaMap(this, this);
        bucket = conf.get(DFSConfigKeys.S3_DATASET_BUCKET, "");
        
        
        Class<? extends S3AFileSystemCommon> s3AFilesystemClass = conf.getClass(
                DFSConfigKeys.S3A_IMPL, S3AFileSystemCommon.class,
                S3AFileSystemCommon.class);
        
        s3afs = ReflectionUtils.newInstance(s3AFilesystemClass, conf);
        URI rootURI = URI.create("s3a://" + bucket);
        
        s3afs.initialize(rootURI, conf);
        s3afs.setWorkingDirectory(new Path("/"));
        
        // use fake s3 for test
        if (conf.getBoolean("test.use_local_s3", false)) {
            s3afs.getS3Client().setEndpoint("http://localhost:4567");
            s3afs.getS3Client().setS3ClientOptions(
                    new S3ClientOptions().withPathStyleAccess(true).disableChunkedEncoding());
        }
        
        // also connect to NN
        List<InetSocketAddress> addrs = DFSUtil.getNameNodesServiceRpcAddresses(conf);
        namenode = datanode.connectToNN(addrs.get(0));
    }

    @Override // FsDatasetSpi
    public ReplicaInPipeline createRbw(StorageType storageType,
                                                    ExtendedBlock b) throws IOException {
        // checks local filesystem and S3 for the block
        // TODO: this might make this block eventually consistent since it's checked before created
        //   FIX: just dont check S3, since it's impossible for this block to exist anyway. 
        ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
        if (replicaInfo != null) {
            throw new ReplicaAlreadyExistsException("Block " + b +
                    " already exists in state " + replicaInfo.getState() +
                    " and thus cannot be created.");
        }

        ReplicaBeingWritten newReplicaInfo;
        synchronized (this) {
            // create a new block
            FsVolumeImpl v = volumes.getNextVolume(storageType, b.getNumBytes());

            // create an rbw file to hold block in the designated volume
            File f = v.createRbwFile(b.getBlockPoolId(), b.getLocalBlock());
            newReplicaInfo = new ReplicaBeingWritten(b.getBlockId(),
                    b.getGenerationStamp(), v, f.getParentFile(), b.getNumBytes());
            volumeMap.add(b.getBlockPoolId(), newReplicaInfo);
        }
        
        return newReplicaInfo;
    }
    
    public void downloadS3BlockTo(ExtendedBlock b, File dest) throws IOException {
        // TODO: check cache if block exists locally?
        S3ConsistentRead s3read = new S3ConsistentRead(this);
        InputStream blockInputStream = s3read.getS3BlockInputStream(b, 0);
        FileUtils.copyInputStreamToFile(blockInputStream, dest);
    }
    

    public void downloadS3BlockMetaTo(ExtendedBlock b, File dest) throws IOException {
        // TODO: check cache if block exists locally?
        S3ConsistentRead s3read = new S3ConsistentRead(this);
        InputStream blockMetaInputStream = s3read.getS3BlockMetaInputStream(b, 0);
        FileUtils.copyInputStreamToFile(blockMetaInputStream, dest);
    }
    
    
    @Override // FsDatasetSpi
    public InputStream getBlockInputStream(ExtendedBlock b, long seekOffset) throws IOException {
        // only check for a local block. If not found, assume it's in S3 immediately
        // (since BlockSender checks that before anyway, and if the block doesnt exist we query NN to make sure)
        ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), b.getBlockId());
        
        if (replicaInfo == null || replicaInfo.getState() == HdfsServerConstants.ReplicaState.FINALIZED) {
            S3ConsistentRead read = new S3ConsistentRead(this);
            return read.getS3BlockInputStream(b, seekOffset);
        } else {
            return super.getBlockInputStream(b, seekOffset);
        }
    }

    @Override // FsDatasetSpi
    public LengthInputStream getMetaDataInputStream(ExtendedBlock b) throws IOException {
        return getMetaDataInputStream(b, 0);
    }
    
    // Same as getMetaDataInputStream() but with extra offset
    public LengthInputStream getMetaDataInputStream(ExtendedBlock b, long seekOffset) throws IOException {
        // only check for a local block. If not found, assume it's in S3 immediately.
        // (since BlockSender checks that before anyway, and if the block doesnt exist we query NN to make sure)
        ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), b.getBlockId());

        if (replicaInfo == null || replicaInfo.getState() == HdfsServerConstants.ReplicaState.FINALIZED) {
            S3ConsistentRead read = new S3ConsistentRead(this);
            return read.getS3BlockMetaInputStream(b, seekOffset);
        } else {
            return super.getMetaDataInputStream(b);
        }
    }

    public static String getBlockKey(ExtendedBlock b) {
        return getBlockKey(b.getBlockPoolId(), b.getBlockId(), b.getGenerationStamp());
    }
    public static String getBlockKey(String blockPoolId, long blockId, long genStamp) {
        return blockPoolId + "/" + BLOCK_FILE_PREFIX + blockId + "_" + genStamp;
    }
    public static String getMetaKey(ExtendedBlock b) {
        return getMetaKey(b.getBlockPoolId(), b.getBlockId(), b.getGenerationStamp());
    }
    public static String getMetaKey(String blockPoolId, long blockId, long genStamp) {
        return getBlockKey(blockPoolId, blockId, genStamp) + METADATA_EXTENSION;
    }
    
    @Override // FsDatasetSpi
    public ReplicaRecoveryInfo initReplicaRecovery(BlockRecoveryCommand.RecoveringBlock rBlock) throws IOException {
        String bpid = rBlock.getBlock().getBlockPoolId();
        
        // get the correct replica from S3 that matches given GS
        ReplicaInfo replica = volumeMap.get(bpid, rBlock.getBlock().getLocalBlock());
        
        if (replica != null && replica.getState() == HdfsServerConstants.ReplicaState.FINALIZED) {
            // set the finalized dir on the replica
            File finalizedDir = replica.getVolume().getFinalizedDir(bpid);
            File blockDir = DatanodeUtil.idToBlockDir(finalizedDir, replica.getBlockId());
            if (!blockDir.exists()) {
                if (!blockDir.mkdirs()) {
                    throw new IOException("Failed to mkdirs " + blockDir);
                }
            }
            replica.setDir(blockDir);

            
            // Write block to disk [for performance, dont synchronize this]
            downloadS3BlockTo(rBlock.getBlock(), replica.getBlockFile());
            downloadS3BlockMetaTo(rBlock.getBlock(), replica.getMetaFile());

            synchronized (this) {
                // add downloaded block to volumemap so we can find this exact replica class again
                volumeMap.add(bpid, replica);
            }
        }
        synchronized (this) {
            return initReplicaRecovery(bpid, volumeMap,
                    rBlock.getBlock().getLocalBlock(), rBlock.getNewGenerationStamp(), datanode.getDnConf().getXceiverStopTimeout());    
        }
    }


    /**
     * Get the meta info of a block stored in volumeMap. To find a block,
     * block pool Id, block Id and generation stamp must match.
     *
     * @param b
     *     extended block
     * @return the meta replica information; null if block was not found
     * @throws ReplicaNotFoundException
     *     if no entry is in the map or
     *     there is a generation stamp mismatch
     */
    @Override
    public ReplicaInfo getReplicaInfo(ExtendedBlock b) throws ReplicaNotFoundException {
        ReplicaInfo info = volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
        if (info == null) {
            throw new ReplicaNotFoundException(ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
        }
        return info;
    }

    /**
     * Get the meta info of a block stored in volumeMap. Block is looked up
     * without matching the generation stamp.
     *
     * @param bpid
     *     block pool Id
     * @param blkid
     *     block Id
     * @return the meta replica information; null if block was not found
     * @throws ReplicaNotFoundException
     *     if no entry is in the map or
     *     there is a generation stamp mismatch
     */
    // shouldnt have gen stamp check when a block is already downloaded... but before it DL it should check
    @Override
    protected ReplicaInfo getReplicaInfo(String bpid, long blkid) throws ReplicaNotFoundException {
        // volume map only contains non-s3 replicas
        ReplicaInfo info = volumeMap.get(bpid, blkid);
        if (info == null) {
            throw new ReplicaNotFoundException("Replica not found on local file system. Use getReplicaInfo(block) to get from S3 (generation stamp needed). Block" + bpid + ":" + blkid);
        }
        return info;
    }

    @Override // FsDatasetSpi
    public synchronized long getReplicaVisibleLength(final ExtendedBlock block) throws IOException {
        final Replica replica = getReplicaInfo(block);
        if (replica.getGenerationStamp() < block.getGenerationStamp()) {
            throw new IOException(
                    "replica.getGenerationStamp() < block.getGenerationStamp(), block=" +
                            block + ", replica=" + replica);
        }
        return replica.getVisibleLength();
    }

    // Returns S3FinalizedReplica and also checks generation stamps matches
    // Query the namenode for a completed block instead of S3 for consistency --> doesnt work b/c NN has old GS for recovery
    public S3FinalizedReplica getS3FinalizedReplica(ExtendedBlock b) {
        S3ConsistentRead consistentRead = new S3ConsistentRead(this);
        Block block = consistentRead.getS3Block(b);
        
        if (block != null ) {
            return new S3FinalizedReplica(block, b.getBlockPoolId(), volumes.getVolumes().get(0), bucket);
        }
        return null;
    }


    // This is NOT a read - it doesnt read the block - only gets block information
    // doesnt check GS
//    @Override // FsDatasetSpi
//    public Block getStoredBlock(String bpid, long blkId) {
//        // Look at the volume map for the block; if not found it will query namenode
////        return volumeMap.get(bpid, blkId);
//        // just get the meta
//        return getReplicaInfo(bpid, blkId);
////        S3ConsistentRead consistentRead = new S3ConsistentRead(this);
////        return consistentRead.getS3Block(bpid, blkId);
//    }


    /**
     * Complete the block write!
     */
    @Override // FsDatasetSpi
        public void finalizeBlock(ExtendedBlock b) throws IOException {
        if (Thread.interrupted()) {
            // Don't allow data modifications from interrupted threads
            throw new IOException("Cannot finalize block from Interrupted Thread");
        }
        Date start_time_upload = new Date();
        
        ReplicaInfo replicaInfo = getReplicaInfo(b);
        if (replicaInfo.getState() == HdfsServerConstants.ReplicaState.FINALIZED) {
            // this is legal, when recovery happens on a file that has
            // been opened for append but never modified
            return;
        }
        finalizeReplica(b.getBlockPoolId(), replicaInfo);

        long diffInMillies_upload = (new Date()).getTime() - start_time_upload.getTime();
        LOG.info("=== Upload block " + diffInMillies_upload + " ms ");
        
        
        // just delete older block even if it's not there
        Date start_del_time = new Date();
        
        // doesnt need to be synchronized, only changes S3
        ExtendedBlock old_b = new ExtendedBlock(b.getBlockPoolId(), b.getBlockId(), b.getNumBytes(), b.getGenerationStamp());
        old_b.setGenerationStamp(old_b.getGenerationStamp() - 1);
        
        // TODO: performance improvement... defer delete until later?
        if (contains(old_b)) {
            s3afs.delete(new Path(getBlockKey(old_b)), false);
            s3afs.delete(new Path(getMetaKey(old_b)), false);
            LOG.info("Deleted old finalized block " + old_b + " for append.");
        }
        long diffInMillies_delete = (new Date()).getTime() - start_del_time.getTime();
        LOG.info("=== Delete prev block time - " + diffInMillies_delete + " ms for append safety.");
    }
    
    /**
     * Upload file to S3 and update volumeMap (replica map)
     * Override method from FsDatasetImpl to support recovery
     */
    @Override
    protected S3FinalizedReplica finalizeReplica (String bpid, ReplicaInfo replicaInfo) throws IOException {
        File local_block_file = replicaInfo.getBlockFile();
        File local_meta_file = replicaInfo.getMetaFile();
        
        // Upload a text string as a new object.
        String s3_block_key = getBlockKey(bpid, replicaInfo.getBlockId(), replicaInfo.getGenerationStamp());
        String s3_block_meta_key = getMetaKey(bpid, replicaInfo.getBlockId(), replicaInfo.getGenerationStamp());

        // Upload a file as a new object with ContentType and title specified.
        PutObjectRequest putReqBlock = new PutObjectRequest(bucket, s3_block_key, local_block_file);
        LOG.info("Uploading block file " + s3_block_key);
        UploadInfo uploadBlock = s3afs.putObject(putReqBlock);
        
        LOG.info("Uploading block meta file " + s3_block_key);
        PutObjectRequest putReqMeta = new PutObjectRequest(bucket, s3_block_meta_key, local_meta_file);
        UploadInfo uploadBlockMeta = s3afs.putObject(putReqMeta);
        
        try {
            uploadBlock.getUpload().waitForUploadResult();
            uploadBlockMeta.getUpload().waitForUploadResult();
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOG.error(e);
        }

        Date start_close_blk = new Date();
        synchronized (this) {
            // release rbw space again on volume
            FsVolumeImpl v = (FsVolumeImpl) replicaInfo.getVolume();
            v.releaseReservedSpace(replicaInfo.getBytesReserved());

            // remove from volumeMap, so we can get it from s3 instead
            volumeMap.remove(bpid, replicaInfo.getBlockId());
        }

        long diffInMillies = (new Date()).getTime() - start_close_blk.getTime();
        LOG.info("finalize_close_blk_time: " + diffInMillies);
        
        S3FinalizedReplica newReplicaInfo = new S3FinalizedReplica(
                replicaInfo.getBlockId(), 
                bpid,
                local_block_file.length(), 
                replicaInfo.getGenerationStamp(),
                volumes.getVolumes().get(0),
                bucket);
        
        // free up space by removing uploaded files
        local_block_file.delete();
        local_meta_file.delete();
        
        return newReplicaInfo;
    }

    /**
     * Append to a finalized replica
     * Change a finalized replica to be a RBW replica and
     * bump its generation stamp to be the newGS
     *
     * @param bpid
     *     block pool Id
     * @param finalizedReplicaInfo
     *     a finalized replica
     * @param newGS
     *     new generation stamp
     * @param estimateBlockLen
     *     estimate generation stamp
     * @return a RBW replica
     * @throws IOException
     *     if moving the replica from finalized directory
     *     to rbw directory fails
     */
    @Override
    protected ReplicaBeingWritten append(String bpid, FinalizedReplica finalizedReplicaInfo, long newGS, long estimateBlockLen)
            throws IOException {
        synchronized (this) {
            // If the block is cached, start uncaching it.
            cacheManager.uncacheBlock(bpid, finalizedReplicaInfo.getBlockId());
            // unlink the finalized replica
            finalizedReplicaInfo.unlinkBlock(1);
        }

        // TODO: finalizedReplica shouldnt have a vol
        FsVolumeImpl v = (FsVolumeImpl) finalizedReplicaInfo.getVolume();
        if (v.getAvailable() < estimateBlockLen - finalizedReplicaInfo.getNumBytes()) {
            throw new DiskChecker.DiskOutOfSpaceException(
                    "Insufficient space for appending to " + finalizedReplicaInfo);
        }

        File newBlkFile = new File(v.getRbwDir(bpid), finalizedReplicaInfo.getBlockName());

        ReplicaBeingWritten newReplicaInfo = new ReplicaBeingWritten(
                finalizedReplicaInfo.getBlockId(), finalizedReplicaInfo.getNumBytes(), newGS,
                v, newBlkFile.getParentFile(), Thread.currentThread(), estimateBlockLen);
        File newmeta = newReplicaInfo.getMetaFile();

        // download block file to rbw directory
        ExtendedBlock block_to_download = new ExtendedBlock(bpid, newReplicaInfo.getBlockId());
        block_to_download.setGenerationStamp(finalizedReplicaInfo.getGenerationStamp());
        
        
        LOG.info("Downloading block file to " + newBlkFile + ", file length=" + finalizedReplicaInfo.getBytesOnDisk());
        try {
            // Download block file to RBW location
            downloadS3BlockTo(block_to_download, newBlkFile);
        } catch (IOException e) {
            throw new IOException("Block " + finalizedReplicaInfo + " reopen failed. " +
                    " Unable to download meta file  to rbw dir " + newmeta, e);
        }

        // rename meta file to rbw directory
        LOG.info("Downloading block meta file to " + newmeta);
        try {
            // Download block meta file to RBW location
            downloadS3BlockMetaTo(block_to_download, newmeta);
        } catch (IOException e) {
            // delete downloaded meta file 
            newmeta.delete();
            throw new IOException("Block " + finalizedReplicaInfo + " reopen failed. " +
                    " Unable to move download block to rbw dir " + newBlkFile, e);
        }

        synchronized (this) {
            // Replace finalized replica by a RBW replica in replicas map
            // so that getReplicaInfo() will return newReplicaInfo
            volumeMap.add(bpid, newReplicaInfo);
            v.reserveSpaceForRbw(estimateBlockLen - finalizedReplicaInfo.getNumBytes());

            // we dont delete from s3 incase something fails, 
            // the finalizeBlock operation will delete the previous block version.   
        }
        
        return newReplicaInfo;
    }

    /**
     * @deprecated use {@link #fetchReplicaInfo(String, long)} instead.
     */
    @Override
    @Deprecated
    public ReplicaInfo getReplica(String bpid, long blockId) {
        throw new NotImplementedException();
    }

    @Override
    ReplicaInfo fetchReplicaInfo(String bpid, long blockId) {
        ReplicaInfo r = volumeMap.get(bpid, blockId);
        if (r == null) {
            throw new CustomRuntimeException("To get an S3 Finalized Replica you need to provide the generation stamp.");
//            return getS3FinalizedReplica(bpid, blockId);
        }
        switch (r.getState()) {
            case FINALIZED:
                LOG.error("Finalized replica must be S3FinalizedReplica - something went wrong here");
                return null;
            case RBW:
                return new ReplicaBeingWritten((ReplicaBeingWritten) r);
            case RWR:
                return new ReplicaWaitingToBeRecovered((ReplicaWaitingToBeRecovered) r);
            case RUR:
                return new ReplicaUnderRecovery((ReplicaUnderRecovery) r);
            case TEMPORARY:
                return new ReplicaInPipeline((ReplicaInPipeline) r);
        }
        return null;
    }


    @Override // FsDatasetSpi
    public boolean contains(final ExtendedBlock block) {
        try {
            // S3guard marks deleted files, so this would return properly
            // TODO: might need to check GS still
            return s3afs.exists(new Path(getBlockKey(block)));    
        } catch (IOException err) {
            LOG.error(err);
            return false;
        }
    }

    @Override // FsDatasetSpi
    public String toString() {
        return "S3Dataset{bucket='" + bucket + "'}";
    }

    //
    // Methods not needed for s3:
    //
    
    /**
     * Get the list of finalized blocks from in-memory blockmap for a block pool.
     */
    @Override
    public synchronized List<FinalizedReplica> getFinalizedBlocks(String bpid) {
        // TODO: this is for block pool & directory scanners
        //  return empty list for now since s3 is already safe
        return new ArrayList<FinalizedReplica>();
    }
    
    @Override // FsDatasetSpi
    public void cache(String bpid, long[] blockIds) {
        // TODO: preemptively download S3 block and store it in a map?
    }

    @Override // FsDatasetSpi
    public void uncache(String bpid, long[] blockIds) {
        // TODO: preemptively download S3 block and store it in a map?
    }
    
    /**
     * Find the block's on-disk length
     */
    @Override // FsDatasetSpi
    public long getLength(ExtendedBlock b) throws IOException {
        return getReplicaInfo(b).getNumBytes();
    }
    
    @Override // FsDatasetSpi
    public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block) throws IOException {
        throw new NotImplementedException();
    }
    
     
    // checkAndUpdate() - directory scanner
    @Override
    public void checkAndUpdate(String bpid, long blockId, File diskFile, File diskMetaFile, FsVolumeSpi vol) {
        throw new NotImplementedException();
    }

    public String getBucket() {
        return bucket;
    }

    public DatanodeProtocolClientSideTranslatorPB getNameNodeClient() {
        return namenode;
    }
    
    public S3AFileSystemCommon getS3AFileSystem() {
        return s3afs;
    }
}

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.S3DatasetImpl;

// S3 does not have Volumes - they are not needed. 
// Instead, it has a bucket and blockPoolId to identify where this block is.
public class S3FinalizedReplica extends FinalizedReplica {
    
    private String bucket;
    private String blockPoolID;

    public S3FinalizedReplica(long blockId, String bpid, long len, long genStamp, FsVolumeImpl vol, String bucket) {
        // vol isnt really used except for cases when finalized blocks go back to RBW
        super(blockId, len, genStamp, vol, null);
        this.blockPoolID = bpid;
        this.bucket = bucket;
        
    }
    
    public S3FinalizedReplica (Block block, String bpid, FsVolumeImpl vol, String bucket) {
        super(block.getBlockId(), block.getNumBytes(), block.getGenerationStamp(), vol, null);
        this.blockPoolID = bpid;
        this.bucket = bucket;
    }

    @Override  // Replica
    public HdfsServerConstants.ReplicaState getState() {
        return HdfsServerConstants.ReplicaState.FINALIZED;
    }

    // TODO: return the default vol that this block would be downloaded to?
    @Override
    public String getStorageUuid() {
        return null;
    }
    
    public String getBucket() {
        return bucket;
    }

    public String getBlockPoolID() {
        return blockPoolID;
    }

    @Override // ReplicaInfo
    public boolean isUnlinked() {
        return true; // no need to be unlinked
    }
    
    @Override  //Object
    public String toString() {
        return getClass().getSimpleName() + ", " +
                getState() + "\n  getNumBytes()     = " + getNumBytes() +
                "\n  getBytesOnDisk()  = " + getBytesOnDisk() +
                "\n  getVisibleLength()= " + getVisibleLength() +
                "\n  getBucket()       = " + getBucket() + 
                "\n  getKey()    = " + S3DatasetImpl.getBlockKey(blockPoolID, getBlockId(), getGenerationStamp());
    }
            
}

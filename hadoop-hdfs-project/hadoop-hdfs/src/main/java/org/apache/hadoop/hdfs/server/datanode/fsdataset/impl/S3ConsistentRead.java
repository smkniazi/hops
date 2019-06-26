package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static java.lang.Thread.sleep;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.LOG;

/**
 * Guarantees to return fresh blocks from S3 by checking with the namenode
 */
public class S3ConsistentRead {
    private final S3DatasetImpl s3dataset;

    private final int TIMEOUT = 10; // seconds
    private final long SLEEP_TIME = 1000; // milliseconds
    private final int MAX_TRIES = (int) ((double)TIMEOUT / ((double)SLEEP_TIME/1000));

    private Block blockMetadata;
    private int tries = 0;

    public S3ConsistentRead(S3DatasetImpl s3dataset) {
        this.s3dataset = s3dataset;
    }

    public Block getS3Block(ExtendedBlock b) {
        return getS3Block(b.getBlockId(), b.getBlockPoolId(), b.getGenerationStamp());
    }

    // TODO: detect when we know block is supposed to be null and dont bother checking?
    public Block getS3Block(long blockId, String bpid, long genStamp) {
        // first check NN if this block is even supposed to exist
        // TODO if we do get a NULL suddenly from NN, do we abandon this??
//        // also Dont get blocks belonging to other blockpools... some tests will fail
//        if (Arrays.asList(s3dataset.volumeMap.getBlockPoolList()).contains(bpid)) {
//            try {
//                blockMetadata = s3dataset.getNameNodeClient().getCompletedBlockMeta(blockId);
//                LOG.info("Got block " + blockId + " from NN: " + blockMetadata);
//            } catch (IOException e) {
//                LOG.error(e);
//            }
//        } else {
//            LOG.error("DN tried to access a blockpool it doesnt own: " + bpid);
//        }
//        if (blockMetadata == null) {
//            return null;
//        }

        String block_aws_key_str = S3DatasetImpl.getBlockKey(bpid, blockId, genStamp);
//        String block_meta_aws_key_str = S3DatasetImpl.getMetaKey(bpid, blockId, genStamp);
        Path block_aws_key = new Path(block_aws_key_str);
        try {
            ObjectMetadata s3Object_meta = s3dataset.getS3AFileSystem().getObjectMetadata(block_aws_key);
            // TODO: why does normal HDFS have GS on meta filename but not block filename??
            // Since the GS is part of the key, we dont need to take it from the filename or metadata
            long blockGS = genStamp;
//            long blockGS = Long.parseLong(s3Object_meta.getUserMetadata().get("generationstamp"));
            Block b = new Block(blockId, s3Object_meta.getInstanceLength(), blockGS);

            if (genStamp != blockGS) {
                throw new AmazonS3Exception("Block Generation Stamp mismatch. Expected: " + genStamp + " Actual: " + blockGS);
            }
            return b;
        } catch (IOException err) {
            // S3Guard exceptions appear here
            throw new CustomRuntimeException(err.getMessage());
        } catch (AmazonS3Exception err) {
            LOG.error(block_aws_key + " : " + err);
            if (! err.toString().contains("404 Not Found") && ! err.toString().contains("Generation Stamp mismatch")) {
                throw err;
            }
        }
        if (doesBlockExist(bpid, blockId)) {
            return getS3Block(blockId, bpid, genStamp);
        } else {
            return null;
        }
    }


    public LengthInputStream getS3BlockMetaInputStream(ExtendedBlock b, long seekOffset) {
        String block_meta_aws_key_str = S3DatasetImpl.getMetaKey(b.getBlockPoolId(), b.getBlockId(), b.getGenerationStamp());
        Path block_meta_aws_key = new Path(block_meta_aws_key_str);
        LOG.info("Getting meta " + s3dataset.getBucket() + ":" + block_meta_aws_key);
        try {
            // TODO: call this with num_bytes of meta block file so that we avoid RR to S3
//            FSDataInputStream in = s3dataset.getS3AFileSystem().open(block_meta_aws_key);
//            return in.getWrappedStream();
//            
            GetObjectRequest metaObjReq = new GetObjectRequest(s3dataset.getBucket(), block_meta_aws_key_str);
            if (seekOffset > 0) {
                metaObjReq.setRange(seekOffset);
            }
            S3Object meta_s3_obj = s3dataset.getS3AFileSystem().getS3Client().getObject(metaObjReq);
            return new LengthInputStream(meta_s3_obj.getObjectContent(), meta_s3_obj.getObjectMetadata().getContentLength());
            
//        } catch (IOException err) {
//            // S3Guard exceptions appear here
//            throw new CustomRuntimeException(err.getMessage());
        } catch (AmazonS3Exception err) {
            LOG.error(block_meta_aws_key_str + " : " + err);
            if (! err.toString().contains("404 Not Found")) {
                throw new CustomRuntimeException(err.getMessage());
            }
        }

        if (doesBlockExist(b.getBlockPoolId(), b.getBlockId())) {
            return getS3BlockMetaInputStream(b, seekOffset);
        } else {
            return null;
        }
    }


    public InputStream getS3BlockInputStream(ExtendedBlock b, long seekOffset) {
        String block_aws_key_str = S3DatasetImpl.getBlockKey(b.getBlockPoolId(), b.getBlockId(), b.getGenerationStamp());
        Path block_aws_key = new Path(block_aws_key_str);

        LOG.info("Getting block " + s3dataset.getBucket() + ":" + block_aws_key + " with seekOffset " + seekOffset);

        try {
            // use the new custom open method that skips doing a fileStatus check
//            FSDataInputStream in = s3dataset.getS3AFileSystem().open(block_aws_key, b.getNumBytes());
//            FSDataInputStream in = s3dataset.getS3AFileSystem().open(block_aws_key);
//            if (seekOffset > 0) {
//                in.seek(seekOffset);
//            }
//            return in.getWrappedStream();

            GetObjectRequest objReq = new GetObjectRequest(s3dataset.getBucket(), block_aws_key_str);
            if (seekOffset > 0) {
                objReq.setRange(seekOffset);
            }
            S3Object s3_obj = s3dataset.getS3AFileSystem().getS3Client().getObject(objReq);
            return s3_obj.getObjectContent();

//        } catch (IOException err) {
//            // S3Guard exceptions appear here
//            throw new CustomRuntimeException(err.getMessage());
        } catch (AmazonS3Exception err) {
            LOG.error(block_aws_key_str + " : " + err);
            if (! err.toString().contains("404 Not Found")) {
                throw new CustomRuntimeException(err.getMessage());
            }
        }

        if (doesBlockExist(b.getBlockPoolId(), b.getBlockId())) {
            return getS3BlockInputStream(b, seekOffset);
        } else {
            return null;
        }
    }

    private boolean doesBlockExist(String bpid, long blockId) {
        // wait to try again soon
        LOG.info("Block " + bpid + ":" + blockId + " not found on S3 bucket " + s3dataset.getBucket());

        // query NN for this block if it's the first time since we dont want to spam the NN with queries
        if (tries == 0) {
            try {
                // Dont get blocks belonging to other blockpools... some tests will fail
                if (Arrays.asList(s3dataset.volumeMap.getBlockPoolList()).contains(bpid)) {
                    blockMetadata = s3dataset.getNameNodeClient().getCompletedBlockMeta(blockId);
                    LOG.info("Got block " + blockId + " from NN: " + blockMetadata);
                } else {
                    LOG.error("DN tried to access a blockpool it doesnt own: " + bpid);
                }
            } catch (IOException err) {
                LOG.error(err);
            }
        } else {
            // only sleep after the first time trying incase we are checking for null.
            try {
                // TODO: exponential backoff
                sleep(SLEEP_TIME);
            } catch(InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        tries++;
        if (tries > MAX_TRIES) {
            LOG.error("Consistency Error: Failed to get block " + blockId + " from S3; timed out after " +
                    TIMEOUT + " seconds. Block exists in the Namenode.");
            return false;
        }
        return blockMetadata != null;
    }
}
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystemCommon;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static java.lang.Thread.sleep;
import static org.apache.hadoop.hdfs.server.common.Storage.LOG;

public class TestS3ConsistentRead {
    final private int BLOCK_SIZE = 512;
    
    final private static int FINALIZED = 0;
    final private static int TEMPORARY = 1;
    final private static int RBW = 2;
    final private static int RWR = 3;
    final private static int RUR = 4;
    final private static int NON_EXISTENT = 5;

    private static Random rand = new Random();
    private static final String[] BLOCK_POOL_IDS = {"bpid-" + rand.nextInt(1000), "bpid-" +  + rand.nextInt(1000)};

    private MiniDFSCluster cluster;
    private DataNode dn;
    private S3DatasetImpl s3dataset;
    private Configuration conf;
    
    @Before
    public void setUp() throws IOException {
        this.conf = new HdfsConfiguration();
        cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
        cluster.waitActive();
        dn = cluster.getDataNodes().get(0);
        s3dataset = (S3DatasetImpl) DataNodeTestUtils.getFSDataset(dn);
        s3dataset.addBlockPool(BLOCK_POOL_IDS[0], dn.getConf());
    }
    
    /**
     * Simple test reading a file after writing - no inconsistencies.
     * @throws IOException
     */
    @Test
    public void testWriteReadFile() throws IOException {
        FileSystem fs = cluster.getFileSystem();
        Path new_file = new Path("/foo/bar1");
        
        String test = "This is bananas! I was retrieved from S3 and not local file system.";
        DFSTestUtil.writeFile(fs, new_file, test);
        
        // now read file, should fail
        String contents = DFSTestUtil.readFile(fs, new_file);

        Assert.assertEquals(test, contents);
    }


    
    private void update_block_gs(long blockId, long oldGenStamp, long newGenStamp) throws IOException {
        // Copy block to change metadata
        String block_key_old = s3dataset.getBlockKey(cluster.getNamesystem().getBlockPoolId(), blockId, oldGenStamp);
        String block_key_new = s3dataset.getBlockKey(cluster.getNamesystem().getBlockPoolId(), blockId, newGenStamp);

//        ObjectMetadata new_meta = new ObjectMetadata();
//        new_meta.addUserMetadata("generationstamp", String.valueOf(newGenStamp));

//        CopyObjectRequest copy_req = new CopyObjectRequest(s3dataset.getBucket(), block_key, s3dataset.getBucket(), block_key)
//                .withSourceBucketName(s3dataset.getBucket())
//                .withSourceKey(block_key)
//                .withNewObjectMetadata(new_meta);

        S3AFileSystemCommon s3afs = s3dataset.getS3AFileSystem();
        s3afs.rename(new Path(block_key_old), new Path(block_key_new));
    }
    
    private void upload_block(long blockId, String test_str, String block_key, long GS) {
        // Create a new temp file and just upload the contents back to S3 
        File the_file = new File("foobar4");
        try {
            FileUtils.writeStringToFile(the_file, test_str);
        } catch (IOException err) {
            LOG.error(err);
        }

        PutObjectRequest putReqBlock = new PutObjectRequest(s3dataset.getBucket(), block_key, the_file);
        ObjectMetadata blockMetadata = new ObjectMetadata();
        blockMetadata.addUserMetadata("generationstamp", String.valueOf(GS));
        putReqBlock.setMetadata(blockMetadata);
        
        // upload to s3
        S3AFileSystemCommon s3afs = s3dataset.getS3AFileSystem();
        s3afs.getS3Client().putObject(putReqBlock);
        
        // cleanup
        the_file.delete();
    }

    /**
     * Tests reading a file from datanode (S3) after a slow update to the block - the block file exists but gen stamp
     * is incorrect. The DN should wait until the correct GS appears.
     * @throws IOException
     */
    @Test
    public void testReadFileSlowUpdateS3() throws IOException {
        FileSystem fs = cluster.getFileSystem();
        Path new_file = new Path("/foo/bar3");
        final String test_str = "This is bananas";
        DFSTestUtil.writeFile(fs, new_file, test_str);
        

        // first change to incorrect GS
        final ExtendedBlock file_block = DFSTestUtil.getFirstBlock(fs, new_file);
        update_block_gs(file_block.getBlockId(), file_block.getGenerationStamp(), 1234);

        // start a thread to update S3 in parallel after sleeping
        Thread update_s3_later = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                
                try {
                    LOG.info("Now update block " + file_block.getBlockId() + " back to correct GS " + file_block.getGenerationStamp());
                    update_block_gs(file_block.getBlockId(), 1234, file_block.getGenerationStamp());
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new CustomRuntimeException(e.getMessage());
                }
                // used to test s3guard wrong file 
//                String block_key = s3dataset.getBlockKey(cluster.getNamesystem().getBlockPoolId(), file_block.getBlockId());
//                upload_block(file_block.getBlockId(), test_str, block_key, 9102);
            }
        });
        update_s3_later.start();

        // now read file, should pass
        String contents = DFSTestUtil.readFile(fs, new_file);
        Assert.assertEquals(test_str, contents);

        update_s3_later.stop();
    }


    /**
     * Tests reading a file after a slow write where the block file does not exist yet.
     * @throws IOException
     */
    @Test
    public void testReadFileMissingBlockSlowS3() throws IOException {
        FileSystem fs = cluster.getFileSystem();
        Path new_file = new Path("/foo/bar4");
        final String test_str = "This is bananas!!";
        DFSTestUtil.writeFile(fs, new_file, test_str);


        // delete block from S3 entirely
        final ExtendedBlock file_block = DFSTestUtil.getFirstBlock(fs, new_file);
        final String delete_block_key = s3dataset.getBlockKey(cluster.getNamesystem().getBlockPoolId(), file_block.getBlockId(), file_block.getGenerationStamp());
        // dont go thru S3A so s3guard marks this as deleted
        s3dataset.getS3AFileSystem().getS3Client().deleteObject(s3dataset.getBucket(), delete_block_key);

        // start a thread to update S3 in parallel after sleeping
        Thread update_s3_later = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                LOG.info("Now put block " + file_block.getBlockId() + " back with GS " + file_block.getGenerationStamp());
                upload_block(file_block.getBlockId(), test_str, delete_block_key, file_block.getGenerationStamp());
            }
        });
        update_s3_later.start();

        // Try to read file immediately, after above thread puts block back this should pass
        String contents = DFSTestUtil.readFile(fs, new_file);
        Assert.assertEquals(test_str, contents);

        update_s3_later.stop();
    }



    /**
     * Tests reading a block file with incorrect GS that never updates. DN tries for awhile and eventually gives up.
     * @throws IOException
     */
//    @Test
//    public void testReadFileInvalidGS() throws IOException {
//        FileSystem fs = cluster.getFileSystem();
//        Path new_file = new Path("/foo/bar2");
//        String test = "This is bananas";
//        DFSTestUtil.writeFile(fs, new_file, test);
//
//        final ExtendedBlock file_block = DFSTestUtil.getFirstBlock(fs, new_file);
//        update_block_gs(file_block.getBlockId(), file_block.getGenerationStamp(), 9990);
//
//        // now read file, should fail
//        try {
//            String contents = DFSTestUtil.readFile(fs, new_file);
//            Assert.fail("Should not have retrieved block with incorrect generation stamp");
//        } catch (BlockMissingException err) {
//            Assert.assertTrue(err.getMessage().startsWith("Could not obtain block"));
//        }
//    }
}

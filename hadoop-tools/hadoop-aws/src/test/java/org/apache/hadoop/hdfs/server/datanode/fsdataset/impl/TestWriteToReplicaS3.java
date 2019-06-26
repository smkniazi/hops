package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class TestWriteToReplicaS3 {
    final private static int FINALIZED = 0;
    final private static int TEMPORARY = 1;
    final private static int RBW = 2;
    final private static int RWR = 3;
    final private static int RUR = 4;
    final private static int NON_EXISTENT = 5;

    // Stuff to mock dataset & classes
    private static final String BASE_DIR = new FileSystemTestHelper().getTestRootDir();
    private static final int NUM_INIT_VOLUMES = 1;
    private static Random rand = new Random();
    private static final String[] BLOCK_POOL_IDS = {"bpid-" + rand.nextInt(1000), "bpid-" +  + rand.nextInt(1000)};

    private Configuration conf;

    private DataNode dn_real;
    private DataNode datanode_fake;
    private DataStorage storage;
    private S3DatasetImpl dataset_fake;
    private S3DatasetImpl s3dataset;

    private MiniDFSCluster cluster;

    // Use to generate storageUuid
    private static final DataStorage dsForStorageUuid = new DataStorage(
            new StorageInfo(HdfsServerConstants.NodeType.DATA_NODE));


    @Before
    public void setUp() throws IOException {
        datanode_fake = Mockito.mock(DataNode.class);
        storage = Mockito.mock(DataStorage.class);
        this.conf = new HdfsConfiguration();
        final DNConf dnConf = new DNConf(conf);

        // also setup real datanode

        cluster = new MiniDFSCluster.Builder(conf).build();
        cluster.waitActive();
        dn_real = cluster.getDataNodes().get(0);
        s3dataset = (S3DatasetImpl) DataNodeTestUtils.getFSDataset(dn_real);
        s3dataset.addBlockPool(BLOCK_POOL_IDS[0], dn_real.getConf());

        when(datanode_fake.getConf()).thenReturn(conf);
        when(datanode_fake.getDnConf()).thenReturn(dnConf);
        when(datanode_fake.getFSDataset()).thenReturn((FsDatasetSpi) dataset_fake);
        createStorageDirs(storage, conf, NUM_INIT_VOLUMES);
        
        dataset_fake = new S3DatasetImpl(datanode_fake, storage, conf);
        for (String bpid : BLOCK_POOL_IDS) {
            dataset_fake.addBlockPool(bpid, conf);
        }
        
        // TODO: how to fake NN?
        when(dataset_fake.getNameNodeClient()).thenReturn(s3dataset.getNameNodeClient());

        assertEquals(NUM_INIT_VOLUMES, dataset_fake.getVolumes().size());
        assertEquals(0, dataset_fake.getNumFailedVolumes());
    }


    /**
     * Generate testing environment and return a collection of blocks
     * on which to run the tests.
     *
     * @param bpid    Block pool ID to generate blocks for
     * @param _dataset Namespace in which to insert blocks
     * @return Contrived blocks for further testing.
     * @throws IOException
     */
    private ExtendedBlock[] setup_blocks(String bpid, S3DatasetImpl _dataset) throws IOException {
        s3dataset.addBlockPool(bpid, dn_real.getConf());
        // setup replicas map

        ExtendedBlock[] blocks = new ExtendedBlock[]{
                new ExtendedBlock(bpid, 1, 1, 2001),
                new ExtendedBlock(bpid, 2, 1, 2002),
                new ExtendedBlock(bpid, 3, 1, 2003),
                new ExtendedBlock(bpid, 4, 1, 2004),
                new ExtendedBlock(bpid, 5, 1, 2005),
                new ExtendedBlock(bpid, 6, 1, 2006),
                new ExtendedBlock(bpid, 7, 1, 2007)
        };

        // create an initial block and finalize it
//        ReplicaBeingWritten rbwInfo = (ReplicaBeingWritten) _dataset.createRbw(StorageType.DEFAULT, blocks[FINALIZED]);
//        FsVolumeImpl vol = _dataset.volumes.getNextVolume(StorageType.DEFAULT, 0);
//        ReplicaBeingWritten rbwInfo = new ReplicaBeingWritten(blocks[FINALIZED].getLocalBlock(), vol,
//                vol.createRbwFile(bpid, blocks[RBW].getLocalBlock()).getParentFile(),
//                null);
//        rbwInfo.getBlockFile().createNewFile();
//        rbwInfo.getMetaFile().createNewFile();
//        // upload to s3
//       
        
        
        // get vol Replica Map and get a volume
        ReplicaMap replicasMap = _dataset.volumeMap;
        FsVolumeImpl vol = _dataset.volumes.getNextVolume(StorageType.DEFAULT, 0);
        
        // Need to create new ReplicaBeingWritten and then finalize it. cant call _dataset.createRbw because
        // it sets the numBytes to 0 b/c the file is empty.
        ReplicaInfo replicaInfo = _dataset.createRbw(StorageType.DEFAULT, blocks[FINALIZED]);
//        ReplicaBeingWritten replicaInfo = new ReplicaBeingWritten(blocks[FINALIZED].getLocalBlock(), vol, 
//                vol.createRbwFile(bpid, blocks[FINALIZED].getLocalBlock()).getParentFile(),
//                null);
//        replicasMap.add(bpid, replicaInfo);
        replicaInfo.getBlockFile().createNewFile();
        replicaInfo.getMetaFile().createNewFile();
        // write 1 byte to the file for testing
        FileOutputStream out = new FileOutputStream(replicaInfo.getBlockFile());
        out.write(0);
        out.close();
        // upload to s3 now
        _dataset.finalizeBlock(blocks[FINALIZED]);
        
        
        // add some replicas 
        replicasMap.add(bpid, new ReplicaInPipeline(
                blocks[TEMPORARY].getBlockId(),
                blocks[TEMPORARY].getGenerationStamp(), vol,
                vol.createTmpFile(bpid, blocks[TEMPORARY].getLocalBlock()).getParentFile(), 0));

        replicaInfo = new ReplicaBeingWritten(blocks[RBW].getLocalBlock(), vol,
                vol.createRbwFile(bpid, blocks[RBW].getLocalBlock()).getParentFile(),
                null);
        replicasMap.add(bpid, replicaInfo);
        replicaInfo.getBlockFile().createNewFile();
        replicaInfo.getMetaFile().createNewFile();

        replicasMap.add(bpid,
                new ReplicaWaitingToBeRecovered(blocks[RWR].getLocalBlock(), vol,
                        vol.createRbwFile(bpid, blocks[RWR].getLocalBlock())
                                .getParentFile()));

        replicasMap.add(bpid, new ReplicaUnderRecovery(
                new FinalizedReplica(blocks[RUR].getLocalBlock(), vol,
                        vol.getCurrentDir().getParentFile()), 2007));

        return blocks;
    }

    @Test
    public void testFinalizeBlock() throws Exception {
        ExtendedBlock eb = new ExtendedBlock(BLOCK_POOL_IDS[0], 20, 1, 2010);
        
        ReplicaBeingWritten rbwInfo = (ReplicaBeingWritten) s3dataset.createRbw(StorageType.DEFAULT, eb);
        rbwInfo.getBlockFile().createNewFile();
        rbwInfo.getMetaFile().createNewFile();

        // finalized & upload a block
        s3dataset.finalizeBlock(eb);

        // get meta about this block
        // query S3 for the new block
        S3FinalizedReplica finalizedReplica = (S3FinalizedReplica) s3dataset.getReplicaInfo(eb);

        // check data is correct
        assertEquals(eb.getBlockId(), finalizedReplica.getBlockId());
        assertEquals(eb.getGenerationStamp(), finalizedReplica.getGenerationStamp());
        assertEquals(HdfsServerConstants.ReplicaState.FINALIZED, finalizedReplica.getState());
    }


    // Creates volume directories
    private static void createStorageDirs(DataStorage storage, Configuration conf,
                                          int numDirs) throws IOException {
        List<Storage.StorageDirectory> dirs =
                new ArrayList<Storage.StorageDirectory>();
        List<String> dirStrings = new ArrayList<String>();
        for (int i = 0; i < numDirs; i++) {
            File loc = new File(BASE_DIR + "/data" + i);
            dirStrings.add(loc.toString());
            loc.mkdirs();
            dirs.add(createStorageDirectory(loc));
            when(storage.getStorageDir(i)).thenReturn(dirs.get(i));
        }

        String dataDir = StringUtils.join(",", dirStrings);
        conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataDir);
        when(storage.getNumStorageDirs()).thenReturn(numDirs);
    }

    private static Storage.StorageDirectory createStorageDirectory(File root) {
        Storage.StorageDirectory sd = new Storage.StorageDirectory(root);
        dsForStorageUuid.createStorageID(sd);
        return sd;
    }


    @Test
    public void testAppend() throws Exception {
        String test_str = "this is bananas!";
        // set up replicasMap
        String bpid = cluster.getNamesystem().getBlockPoolId();
        ExtendedBlock eb = new ExtendedBlock(bpid, 21, test_str.getBytes().length, 2020);
        
        ReplicaBeingWritten rbwInfo = (ReplicaBeingWritten) s3dataset.createRbw(StorageType.DEFAULT, eb);
        rbwInfo.getBlockFile().createNewFile();
        rbwInfo.getMetaFile().createNewFile();
        
        // write some real data
        File blockFile = rbwInfo.getBlockFile();
        FileUtils.writeStringToFile(blockFile, test_str);
        
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] hash = md.digest(test_str.getBytes());
        File metaFile = rbwInfo.getMetaFile();
        FileUtils.writeByteArrayToFile(metaFile, hash);
        

        // finalized & upload a block
        s3dataset.finalizeBlock(eb);
        
        // check data is correct
        S3FinalizedReplica finalizedReplica = (S3FinalizedReplica) s3dataset.getReplicaInfo(eb);
        assertEquals(eb.getBlockId(), finalizedReplica.getBlockId());
        assertEquals(eb.getGenerationStamp(), finalizedReplica.getGenerationStamp());
        assertEquals(HdfsServerConstants.ReplicaState.FINALIZED, finalizedReplica.getState());

        // Now do the append after increasing GS and expectedLen
        // copy eb to new_eb
        ExtendedBlock new_eb = new ExtendedBlock(eb.getBlockPoolId(), eb.getBlockId(), eb.getNumBytes(), eb.getGenerationStamp());
        new_eb.setNumBytes(eb.getNumBytes());
        
        long newGS = new_eb.getGenerationStamp() + 1;
        s3dataset.append(new_eb, newGS, new_eb.getNumBytes());

        // make sure local files exist (downloaded)
        ReplicaBeingWritten new_rbw_info = (ReplicaBeingWritten) s3dataset.getReplicaInfo(new_eb);
        Assert.assertTrue(new_rbw_info.getBlockFile().exists());
        Assert.assertTrue(new_rbw_info.getMetaFile().exists());
        Assert.assertEquals(newGS, new_rbw_info.getGenerationStamp());
        
        // now finish this block again
        new_eb.setGenerationStamp(newGS); // this GS bump happens somewhere else in DN
        s3dataset.finalizeBlock(new_eb);

        // check data is correct
        S3FinalizedReplica finalizedAppend = (S3FinalizedReplica) s3dataset.getReplicaInfo(new_eb);
        assertEquals(new_eb.getBlockId(), finalizedAppend.getBlockId());
        assertEquals(new_eb.getGenerationStamp(), finalizedAppend.getGenerationStamp());
        assertEquals(HdfsServerConstants.ReplicaState.FINALIZED, finalizedAppend.getState());
        
        // check that old EB doesnt exist
        try {
            S3FinalizedReplica oldBlock = (S3FinalizedReplica) s3dataset.getReplicaInfo(eb);
            Assert.assertNull(oldBlock);
        } catch (ReplicaNotFoundException err) {
            Assert.assertTrue(err.getMessage().startsWith(ReplicaNotFoundException.NON_EXISTENT_REPLICA));
        }
    }

    // test append
    @Test
    public void testAppendFailures() throws Exception {
        // set up replicasMap
        String bpid = cluster.getNamesystem().getBlockPoolId();
        ExtendedBlock[] blocks = setup_blocks(bpid, s3dataset);


        long newGS = blocks[FINALIZED].getGenerationStamp() + 1;
        final FsVolumeImpl v = (FsVolumeImpl) s3dataset.getS3FinalizedReplica(blocks[FINALIZED]).getVolume();
        long available = v.getCapacity() - v.getDfsUsed();
        long expectedLen = blocks[FINALIZED].getNumBytes();
        try {
            v.decDfsUsed(bpid, -available);
            blocks[FINALIZED].setNumBytes(expectedLen + 100);
            s3dataset.append(blocks[FINALIZED], newGS, expectedLen);
            Assert.fail(
                    "Should not have space to append to an RWR replica" + blocks[RWR]);
        } catch (DiskChecker.DiskOutOfSpaceException e) {
            Assert.assertTrue(
                    e.getMessage().startsWith("Insufficient space for appending to "));
        }
        v.decDfsUsed(bpid, available);
        blocks[FINALIZED].setNumBytes(expectedLen);

        newGS = blocks[RBW].getGenerationStamp() + 1;
        s3dataset.append(blocks[FINALIZED], newGS,
                blocks[FINALIZED].getNumBytes());  // successful
        blocks[FINALIZED].setGenerationStamp(newGS);

        try {
            s3dataset
                    .append(blocks[TEMPORARY], blocks[TEMPORARY].getGenerationStamp() + 1,
                            blocks[TEMPORARY].getNumBytes());
            Assert.fail("Should not have appended to a temporary replica " +
                    blocks[TEMPORARY]);
        } catch (ReplicaNotFoundException e) {
            Assert.assertEquals(
                    ReplicaNotFoundException.UNFINALIZED_REPLICA + blocks[TEMPORARY],
                    e.getMessage());
        }

        try {
            s3dataset.append(blocks[RBW], blocks[RBW].getGenerationStamp() + 1,
                    blocks[RBW].getNumBytes());
            Assert.fail("Should not have appended to an RBW replica" + blocks[RBW]);
        } catch (ReplicaNotFoundException e) {
            Assert.assertEquals(
                    ReplicaNotFoundException.UNFINALIZED_REPLICA + blocks[RBW],
                    e.getMessage());
        }

        try {
            s3dataset.append(blocks[RWR], blocks[RWR].getGenerationStamp() + 1,
                    blocks[RBW].getNumBytes());
            Assert.fail("Should not have appended to an RWR replica" + blocks[RWR]);
        } catch (ReplicaNotFoundException e) {
            Assert.assertEquals(
                    ReplicaNotFoundException.UNFINALIZED_REPLICA + blocks[RWR],
                    e.getMessage());
        }

        try {
            s3dataset.append(blocks[RUR], blocks[RUR].getGenerationStamp() + 1,
                    blocks[RUR].getNumBytes());
            Assert.fail("Should not have appended to an RUR replica" + blocks[RUR]);
        } catch (ReplicaNotFoundException e) {
            Assert.assertEquals(
                    ReplicaNotFoundException.UNFINALIZED_REPLICA + blocks[RUR],
                    e.getMessage());
        }

        try {
            s3dataset.append(blocks[NON_EXISTENT],
                    blocks[NON_EXISTENT].getGenerationStamp(),
                    blocks[NON_EXISTENT].getNumBytes());
            Assert.fail("Should not have appended to a non-existent replica " +
                    blocks[NON_EXISTENT]);
        } catch (ReplicaNotFoundException e) {
            Assert.assertEquals(
                    ReplicaNotFoundException.NON_EXISTENT_REPLICA + blocks[NON_EXISTENT],
                    e.getMessage());
        }

        newGS = blocks[FINALIZED].getGenerationStamp() + 1;
        s3dataset.recoverAppend(blocks[FINALIZED], newGS,
                blocks[FINALIZED].getNumBytes());  // successful
        blocks[FINALIZED].setGenerationStamp(newGS);

        try {
            s3dataset.recoverAppend(blocks[TEMPORARY],
                    blocks[TEMPORARY].getGenerationStamp() + 1,
                    blocks[TEMPORARY].getNumBytes());
            Assert.fail("Should not have appended to a temporary replica " +
                    blocks[TEMPORARY]);
        } catch (ReplicaNotFoundException e) {
            Assert.assertTrue(e.getMessage()
                    .startsWith(ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA));
        }

        newGS = blocks[RBW].getGenerationStamp() + 1;
        s3dataset.recoverAppend(blocks[RBW], newGS, blocks[RBW].getNumBytes());
        blocks[RBW].setGenerationStamp(newGS);

        try {
            s3dataset.recoverAppend(blocks[RWR], blocks[RWR].getGenerationStamp() + 1,
                    blocks[RBW].getNumBytes());
            Assert.fail("Should not have appended to an RWR replica" + blocks[RWR]);
        } catch (ReplicaNotFoundException e) {
            Assert.assertTrue(e.getMessage()
                    .startsWith(ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA));
        }

        try {
            s3dataset.recoverAppend(blocks[RUR], blocks[RUR].getGenerationStamp() + 1,
                    blocks[RUR].getNumBytes());
            Assert.fail("Should not have appended to an RUR replica" + blocks[RUR]);
        } catch (ReplicaNotFoundException e) {
            Assert.assertTrue(e.getMessage()
                    .startsWith(ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA));
        }

        try {
            s3dataset.recoverAppend(blocks[NON_EXISTENT],
                    blocks[NON_EXISTENT].getGenerationStamp(),
                    blocks[NON_EXISTENT].getNumBytes());
            Assert.fail("Should not have appended to a non-existent replica " +
                    blocks[NON_EXISTENT]);
        } catch (ReplicaNotFoundException e) {
            Assert.assertTrue(e.getMessage()
                    .startsWith(ReplicaNotFoundException.NON_EXISTENT_REPLICA));
        }
    }

    @Test
    public void testGetS3BlockInvalidGS() throws Exception {
        ExtendedBlock eb = new ExtendedBlock(BLOCK_POOL_IDS[0], 2, 1, 2002);

        ReplicaBeingWritten rbwInfo = (ReplicaBeingWritten) s3dataset.createRbw(StorageType.DEFAULT, eb);
        rbwInfo.getBlockFile().createNewFile();
        rbwInfo.getMetaFile().createNewFile();

        // finalized & upload a block
        s3dataset.finalizeBlock(eb);

        // get meta about this block
        // query S3 for the new block
        eb.setGenerationStamp(9000);
        try {
            S3FinalizedReplica finalizedReplica = (S3FinalizedReplica) s3dataset.getReplicaInfo(eb);
            Assert.fail("Should not have gotten block" + finalizedReplica);
        } catch (ReplicaNotFoundException err) {
            Assert.assertTrue(err.getMessage().startsWith(ReplicaNotFoundException.NON_EXISTENT_REPLICA));
        }
    }
}
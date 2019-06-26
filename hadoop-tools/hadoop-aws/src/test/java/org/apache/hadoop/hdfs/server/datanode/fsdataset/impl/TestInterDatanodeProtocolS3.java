package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.junit.Assert;

import java.io.IOException;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestInterDatanodeProtocolS3 extends TestInterDatanodeProtocol {
    
    // we need this method to call getReplicaInfo instead of getStoredBlock 
    // b/c the timestamp has to be checked. 
    private void checkMetaInfoS3(ExtendedBlock b, DataNode dn) throws IOException {
        Block metainfo = DataNodeTestUtils.getFSDataset(dn).getReplicaInfo(b);
        Assert.assertEquals(b.getBlockId(), metainfo.getBlockId());
        Assert.assertEquals(b.getNumBytes(), metainfo.getNumBytes());
    }

    @Override
    protected void checkBlockMetaDataInfo(boolean useDnHostname) throws Exception {
        MiniDFSCluster cluster = null;

        conf.setBoolean(DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME, useDnHostname);
        if (useDnHostname) {
            // Since the mini cluster only listens on the loopback we have to
            // ensure the hostname used to access DNs maps to the loopback. We
            // do this by telling the DN to advertise localhost as its hostname
            // instead of the default hostname.
            conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, "localhost");
        }

        try {
            cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
                    .checkDataNodeHostConfig(true).build();
            cluster.waitActive();

            //create a file
            DistributedFileSystem dfs = cluster.getFileSystem();
            String filestr = "/foo";
            Path filepath = new Path(filestr);
            DFSTestUtil.createFile(dfs, filepath, 1024L, (short) 3, 0L);
            assertTrue(dfs.exists(filepath));

            //get block info
            LocatedBlock locatedblock =
                    getLastLocatedBlock(DFSClientAdapter.getDFSClient(dfs).getNamenode(),
                            filestr);
            DatanodeInfo[] datanodeinfo = locatedblock.getLocations();
            assertTrue(datanodeinfo.length > 0);

            //connect to a data node
            DataNode datanode = cluster.getDataNode(datanodeinfo[0].getIpcPort());
            InterDatanodeProtocol idp = DataNodeTestUtils
                    .createInterDatanodeProtocolProxy(datanode, datanodeinfo[0], conf,
                            useDnHostname);

            //stop block scanner, so we could compare lastScanTime
            DataNodeTestUtils.shutdownBlockScanner(datanode);

            //verify BlockMetaDataInfo
            ExtendedBlock b = locatedblock.getBlock();
            InterDatanodeProtocol.LOG.info("b=" + b + ", " + b.getClass());
            checkMetaInfoS3(b, datanode);
            long recoveryId = b.getGenerationStamp() + 1;
            idp.initReplicaRecovery(
                    new BlockRecoveryCommand.RecoveringBlock(b, locatedblock.getLocations(), recoveryId));

            //verify updateBlock
            ExtendedBlock newblock =
                    new ExtendedBlock(b.getBlockPoolId(), b.getBlockId(),
                            b.getNumBytes() / 2, b.getGenerationStamp() + 1);
            idp.updateReplicaUnderRecovery(b, recoveryId, newblock.getNumBytes());
            checkMetaInfoS3(newblock, datanode);

            // Verify correct null response trying to init recovery for a missing block
            ExtendedBlock badBlock =
                    new ExtendedBlock("fake-pool", b.getBlockId(), 0, 0);
            assertNull(idp.initReplicaRecovery(
                    new BlockRecoveryCommand.RecoveringBlock(badBlock, locatedblock.getLocations(),
                            recoveryId)));
        } finally {
            if (cluster != null) {
                cluster.shutdown();
            }
        }
    }
}

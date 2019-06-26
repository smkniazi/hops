package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.TestWriteRead;
import org.junit.Before;

public class TestWriteReadS3 extends TestWriteRead {

    @Override
    @Before
    public void initJunitModeTest() throws Exception {
        LOG.info("initJunitModeTest");

        conf = new HdfsConfiguration();
        // blocksize
        conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize); // 100K
        conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);

        cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
        cluster.waitActive();

        mfs = cluster.getFileSystem();
        mfc = FileContext.getFileContext();

        Path rootdir = new Path(ROOT_DIR);
        mfs.mkdirs(rootdir);
    }
}

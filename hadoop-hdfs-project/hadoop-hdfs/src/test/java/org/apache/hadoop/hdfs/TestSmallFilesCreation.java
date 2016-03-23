package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.Test;
import static org.junit.Assert.fail;


import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;

/**
 * Created by salman on 2016-03-22.
 */
public class TestSmallFilesCreation {
  @Test
  public void testSmallFiles() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 32;
      final int CHECK_SUM_SIZE = 32;
      final int FILE_SIZE = 32;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int SMALL_FILE_MAX_SIZE = 10;

      final String FILE_NAME = "/TEST-FLIE";

      conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, SMALL_FILE_MAX_SIZE);
      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFS_BYTES_PER_CHECKSUM_KEY, CHECK_SUM_SIZE);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE); // 4 byte


      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);

      byte data[] = new byte[FILE_SIZE];
      System.out.println("Before Writing the data");
      out.write(data);
      Thread.sleep(1);
      System.out.println("After Writing the data");
      System.out.println("Before Closing the file.");
      out.close();
      System.out.println("After Closing the file.");


      FSDataInputStream dfsIs = dfs.open(new Path(FILE_NAME));
      byte buffer[] = new byte[FILE_SIZE];
      int readSize = 0;
      int curRead = -1;

      while((curRead = dfsIs.read(buffer)) != -1 ){
        readSize += curRead;
      }


      if(  readSize != FILE_SIZE){
        fail("Wrong amount of data read from the file. Data read  was: "+readSize);
      }
      dfsIs.close();


      DFSClient c = dfs.getClient();
      LocatedBlocks lb = c.getLocatedBlocks(FILE_NAME, 0, Long.MAX_VALUE);
      if(lb.isStoredInDB() != false){
        fail("The file should have been stored on the datanodes");
      }



    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}

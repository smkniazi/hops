package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.Test;
import static org.junit.Assert.fail;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

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

      final int BLOCK_SIZE = 1024*1024;
      final int CHECK_SUM_SIZE = 32;
      final int FILE_SIZE = 32*1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int SMALL_FILE_MAX_SIZE = 32*1024;





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
      Random rand = new Random(System.currentTimeMillis());
      long total = 0;
      for(int i = 0; i < data.length; i++)
      {
        data[i] = (byte)rand.nextInt(128);
        total += data[i];
      }
      System.out.println("SMALL_FILE Before Writing the data. Sum of all the data is : "+total);

      out.write(data);
     // Thread.sleep(5000);

      System.out.println("SMALL_FILE After Writing the data");
      System.out.println("SMALL_FILE Before Closing the file.");
      out.close();
      System.out.println("SMALL_FILE After Closing the file.");

//
//      FSDataInputStream dfsIs = dfs.open(new Path(FILE_NAME));
//      byte buffer[] = new byte[FILE_SIZE];
//      int readSize = 0;
//      int curRead = -1;
//
//      while((curRead = dfsIs.read(buffer)) != -1 ){
//        readSize += curRead;
//      }
//
//
//      if(  readSize != FILE_SIZE ){
//        fail("Wrong amount of data read from the file. Data read  was: "+readSize);
//      }
//      dfsIs.close();
//
//
//      DFSClient c = dfs.getClient();
//      LocatedBlocks lb = c.getLocatedBlocks(FILE_NAME, 0, Long.MAX_VALUE);
//
//      if(lb != null && lb.isStoredInDB() != false){
//        fail("The file should have been stored on the datanodes");
//      }



    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}

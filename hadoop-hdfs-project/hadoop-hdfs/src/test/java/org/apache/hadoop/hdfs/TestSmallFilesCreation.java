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

      final int BLOCK_SIZE = 1024 * 1024;
      final int CHECK_SUM_SIZE = 32;
      final int FILE_SIZE = 1 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int SMALL_FILE_MAX_SIZE = 32 * 1024;
      final String FILE_NAME = "/TEST-FLIE";

      conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, SMALL_FILE_MAX_SIZE);
      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY, ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFS_BYTES_PER_CHECKSUM_KEY, CHECK_SUM_SIZE);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE); // 4 byte


      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);
      writeFile(out, FILE_SIZE);
      out.close();


      FSDataInputStream dfsIs = dfs.open(new Path(FILE_NAME));
      readFile(dfsIs, FILE_SIZE);
      dfsIs.close();

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  static void writeFile(FSDataOutputStream os, int size) throws IOException {
    byte[] data = new byte[size];
    for (int i = 0; i < size; i++) {
      byte number = (byte) (i % 128);
      data[i] = number;
    }
    os.write(data);
  }

  /**
   * This method reads the file using different read methods.
   */
  static void readFile(DistributedFileSystem dfs, String file, int size) throws IOException {
    //reading one byte at a time.
    FSDataInputStream is = dfs.open(new Path(file));
    byte[] onebyte = new byte[1];
    for (int i = 0; i < size; i++) {
      if (is.read(onebyte, 0, 1) != 1) {
        fail("failed to read");
      }
      byte number = (byte) (i % 128);
      if (number != onebyte[0]) {
        fail("Wrong data read");
      }
    }
    //next read should return -1
    if (is.read(onebyte, 0, 1) != -1) {
      fail("Read Failed. Expecting End of File.");
    }
    is.close();

    //read the whole file
    is = dfs.open(new Path(file));
    byte[] buffer = new byte[size];
    if (size != is.read(buffer, 0, size)) {
      fail("Wrong amount of data read from the file");
    }
    if (-1 != is.read(buffer, 0, size)) {
      fail("Read Failed. Expecting End of File.");
    }
    is.close();

    ByteBuffer byteBuffer = ByteBuffer.allocate(size);
    is = dfs.open(new Path(file));
    if( size != is.read(byteBuffer)){
      fail("Wrong amount of data read using read(ByteBuffer) function");
    }
    is.close();

    is = dfs.open(new Path(file));
    is.readFully(0, buffer);


  }
}

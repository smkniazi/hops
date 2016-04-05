package org.apache.hadoop.hdfs;

import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.FileInodeDataDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;

/**
 * Created by salman on 2016-03-22.
 */
public class TestSmallFilesCreation {
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
  static void readFileUsingMultipleMethods(DistributedFileSystem dfs,
      String file, int size) throws IOException {
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
    //--------------------------------------------------------------------------

    is = dfs.open(new Path(file));
    byte[] buffer = new byte[size];
    if (size != is.read(buffer, 0, size)) {
      fail("Wrong amount of data read from the file");
    }
    for (int i = 0; i < size; i++) {
      if ((i % 128) != buffer[i]) {
        fail("Data is corrupted. Expecting: " + i + " got: " + buffer[i] +
            " index: " +
            "" + i);
      }
    }
    if (-1 != is.read(buffer, 0, size)) {
      fail("Read Failed. Expecting End of File.");
    }
    is.close();
    //--------------------------------------------------------------------------

    ByteBuffer byteBuffer = ByteBuffer.allocate(size);
    is = dfs.open(new Path(file));
    if (size != is.read(byteBuffer)) {
      fail("Wrong amount of data read using read(ByteBuffer) function");
    }
    for (int i = 0; i < size; i++) {
      if ((i % 128) != buffer[i]) {
        fail("Data is corrupted");
      }
    }
    is.close();
    //--------------------------------------------------------------------------

    is = dfs.open(new Path(file));
    is.readFully(0, buffer);
    for (int i = 0; i < size; i++) {
      if ((i % 128) != buffer[i]) {
        fail("Data is corrupted");
      }
    }
    is.close();
    //--------------------------------------------------------------------------

    is = dfs.open(new Path(file));
    is.readFully(0, buffer, 0, size);
    for (int i = 0; i < size; i++) {
      if ((i % 128) != buffer[i]) {
        fail("Data is corrupted");
      }
    }
    is.close();
    //--------------------------------------------------------------------------

  }

  /**
   * Simple read and write test
   *
   * @throws IOException
   */
  @Test
  public void TestSimpleReadAndWrite() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final int FILE_SIZE = 1 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int SMALL_FILE_MAX_SIZE = 32 * 1024;
      final String FILE_NAME = "/TEST-FLIE";

      conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, SMALL_FILE_MAX_SIZE);
      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY,
          ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE); // 4 byte


      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);
      writeFile(out, FILE_SIZE);
      out.close();

      readFileUsingMultipleMethods(dfs, FILE_NAME, FILE_SIZE);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Write large file and make sure that it is stored on the datanodes
   *
   * @throws IOException
   */
  @Test
  public void TestWriteLargeFile() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final int FILE_SIZE = 32 * 1024 + 1;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int SMALL_FILE_MAX_SIZE = 32 * 1024;
      final String FILE_NAME = "/TEST-FLIE";

      conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, SMALL_FILE_MAX_SIZE);
      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY,
          ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE); // 4 byte


      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();

      DistributedFileSystem dfs = cluster.getFileSystem();
      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);
      writeFile(out, FILE_SIZE);
      out.close();


      FSDataInputStream dfsIs = dfs.open(new Path(FILE_NAME));
      LocatedBlocks lblks =
          dfs.getClient().getLocatedBlocks(FILE_NAME, 0, Long.MAX_VALUE);
      assertFalse("The should not have been stored in the database",
          lblks.hasPhantomBlock());
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

  /**
   * if the file is small but the client calls flush method before the
   * close operation the save the file on the datanodes. This is because the
   * final size of the file is not known before the file is closed
   *
   * @throws IOException
   */
  @Test
  public void TestSmallFileHflush() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final int FILE_SIZE = 1 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int SMALL_FILE_MAX_SIZE = 32 * 1024;
      final String FILE_NAME = "/TEST-FLIE";

      conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, SMALL_FILE_MAX_SIZE);
      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY,
          ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE); // 4 byte

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);
      writeFile(out, FILE_SIZE);
      out.flush();
      out.hflush();
      out.close();


      FSDataInputStream dfsIs = dfs.open(new Path(FILE_NAME));
      LocatedBlocks lblks =
          dfs.getClient().getLocatedBlocks(FILE_NAME, 0, Long.MAX_VALUE);
      assertFalse("The should not have been stored in the database",
          lblks.hasPhantomBlock());
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

  /**
   * if the file is small but the client calls sync ethod before the
   * close operation the save the file on the datanodes. This is because the
   * final size of the file is not known before the file is closed
   *
   * @throws IOException
   */
  @Test
  public void TestSmallFileHsync() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final int FILE_SIZE = 1 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int SMALL_FILE_MAX_SIZE = 32 * 1024;
      final String FILE_NAME = "/TEST-FLIE";

      conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, SMALL_FILE_MAX_SIZE);
      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY,
          ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE); // 4 byte

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);
      writeFile(out, FILE_SIZE);
      out.hsync();
      out.close();

      FSDataInputStream dfsIs = dfs.open(new Path(FILE_NAME));
      LocatedBlocks lblks =
          dfs.getClient().getLocatedBlocks(FILE_NAME, 0, Long.MAX_VALUE);
      assertFalse("The should not have been stored in the database",
          lblks.hasPhantomBlock());
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

  /**
   * delete file stored in the database
   * @throws IOException
   */
  @Test
  public void TestDeleteSmallFile() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final int FILE_SIZE = 1 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int SMALL_FILE_MAX_SIZE = 32 * 1024;
      final String FILE_NAME = "/TEST-FLIE";

      conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, SMALL_FILE_MAX_SIZE);
      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY,
          ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE); // 4 byte

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);
      writeFile(out, FILE_SIZE);
      out.close();

      dfs.delete(new Path(FILE_NAME));

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  @Test
  public void TestRenameSmallFile() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final int FILE_SIZE = 1 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int SMALL_FILE_MAX_SIZE = 32 * 1024;
      final String FILE_NAME = "/TEST-FLIE";

      conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, SMALL_FILE_MAX_SIZE);
      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY,
          ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE); // 4 byte

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      FSDataOutputStream out = dfs.create(new Path(FILE_NAME), (short) 3);
      writeFile(out, FILE_SIZE);
      out.close();

      dfs.rename(new Path(FILE_NAME), new Path(FILE_NAME+"1"));

      readFileUsingMultipleMethods(dfs, FILE_NAME+"1",FILE_SIZE);

      assertTrue("Count of db file should be 1", countDBFiles() == 1);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void TestRenameSmallFiles2() throws IOException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();

      final int BLOCK_SIZE = 1024 * 1024;
      final int FILE_SIZE = 1 * 1024;
      final boolean ENABLE_STORE_SMALL_FILES_IN_DB = true;
      final int SMALL_FILE_MAX_SIZE = 32 * 1024;
      final String FILE_NAME = "/TEST-FLIE";

      conf.setInt(DFSConfigKeys.DFS_DB_FILE_MAX_SIZE_KEY, SMALL_FILE_MAX_SIZE);
      conf.setBoolean(DFSConfigKeys.DFS_STORE_SMALL_FILES_IN_DB_KEY,
          ENABLE_STORE_SMALL_FILES_IN_DB);
      conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE); // 4 byte

      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();

      FSDataOutputStream out = dfs.create(new Path(FILE_NAME+"1"), (short) 3);
      writeFile(out, FILE_SIZE);
      out.close();

      out = dfs.create(new Path(FILE_NAME+"2"), (short) 3);
      writeFile(out, FILE_SIZE);
      out.close();

      assertTrue("Count of db file should be 2", countDBFiles() == 2);

      dfs.rename(new Path(FILE_NAME+"1"), new Path(FILE_NAME+"2"));

      readFileUsingMultipleMethods(dfs, FILE_NAME+"2",FILE_SIZE);

      assertTrue("Count of db file should be 1", countDBFiles() == 1);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public static int countDBFiles() throws IOException {
    LightWeightRequestHandler countDBFiles =
            new LightWeightRequestHandler(HDFSOperationType.TEST_DB_FILES) {
              @Override
              public Object performTask() throws StorageException, IOException {
                FileInodeDataDataAccess fida = (FileInodeDataDataAccess) HdfsStorageFactory
                        .getDataAccess(FileInodeDataDataAccess.class);
                return fida.count();
              }
            };
    return (Integer)countDBFiles.handle();
  }

}

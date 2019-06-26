package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

import static org.apache.hadoop.fs.s3a.Constants.FAST_UPLOAD;

public class S3PerformanceTest {
    private AmazonS3 s3client;
    private S3AFileSystem s3afs;
    private String bucket;
    
    // test settings./autogen.sh
    private final boolean use_hdfs_s3_dataset = true;
    private final boolean use_local_s3 = false;
    private final boolean use_fast_upload_s3a = false;
    private final boolean do_append = false;
    
    private static final boolean verboseOption = false;
    protected static final String ROOT_DIR = "/tmp/";
    private static final boolean positionReadOption = false;

    protected static Log LOG = LogFactory.getLog(S3PerformanceTest.class);
    private Random random = new Random();

    private MiniDFSCluster cluster;
    protected FileSystem mfs;
    

    @Before
    public void initJunitModeTest() throws Exception {
        LOG.info("initJunitModeTest");
    }

    //
    // hdfs tests
    //
    @Test
    public void testHDFSRecordTime100MB() throws IOException {
        long FILE_SIZE = 1024 * 100000; // bytes
        int WR_NTIMES = 30;
        String run_id = "/perf_run" + random.nextInt(10000);
        testHDFS(FILE_SIZE, FILE_SIZE, WR_NTIMES, run_id);
    }
    
    @Test
    public void testHDFSRecordTime10MB() throws IOException {
        long FILE_SIZE = 1024 * 10000; // bytes
        int WR_NTIMES = 30;
        String run_id = "/perf_run" + random.nextInt(10000);
        testHDFS(FILE_SIZE, FILE_SIZE, WR_NTIMES, run_id);
    }

    @Test
    public void testHDFSRecordTime1MB() throws IOException {
        long FILE_SIZE = 1048576; // bytes
        int WR_NTIMES = 30;
        String run_id = "/perf_run" + random.nextInt(10000);
        testHDFS(FILE_SIZE, FILE_SIZE, WR_NTIMES, run_id);
    }

    @Test
    public void testHDFSRecordTime10MB_multi_blocks() throws IOException {
        long FILE_SIZE = 512 * 10000; // bytes
        long BLOCK_SIZE = 1024 * 10000; // bytes
        int WR_NTIMES = 30;
        String run_id = "/perf_run" + random.nextInt(10000);
        testHDFS(FILE_SIZE, BLOCK_SIZE, WR_NTIMES, run_id);
    }

    @Test
    public void testHDFSRecordTime100k() throws IOException {
        long FILE_SIZE = 1024 * 100; // bytes
        int WR_NTIMES = 30;
        String run_id = "/perf_run" + random.nextInt(10000);
        testHDFS(FILE_SIZE, FILE_SIZE, WR_NTIMES, run_id);
    }

    //
    // s3 tests
    //
    @Test
    public void testS3RecordTime100MB() throws IOException {
        long BLOCK_SIZE = 1024 * 100000; // bytes
        long BLOCK_SIZE_META = 800007; // bytes
        int WR_NTIMES = 30;
        String run_id = "/s3_perf_run" + random.nextInt(10000);
        testS3(BLOCK_SIZE, BLOCK_SIZE_META, WR_NTIMES, run_id);
    }
    
    @Test
    public void testS3RecordTime10MB() throws IOException {
        long BLOCK_SIZE = 1024 * 10000; // bytes
        long BLOCK_SIZE_META = 80007; // bytes
        int WR_NTIMES = 30;
        String run_id = "/s3_perf_run" + random.nextInt(10000);

        testS3(BLOCK_SIZE, BLOCK_SIZE_META, WR_NTIMES, run_id);
    }

    @Test
    public void testS3RecordTime1MB() throws IOException {
        long BLOCK_SIZE = 1048576; // bytes
        long BLOCK_SIZE_META = 8199; // bytes   
        int WR_NTIMES = 1000;
        String run_id = "/s3_perf_run" + random.nextInt(100000);

        testS3(BLOCK_SIZE, BLOCK_SIZE_META, WR_NTIMES, run_id);
    }

    @Test
    public void testS3RecordTime1MB_parallel_clients() throws IOException {
        final int NUM_CLIENTS = 20;
        class FakeClient implements Runnable {
            long BLOCK_SIZE = 1048576; // bytes
            long BLOCK_SIZE_META = 8199; // bytes
            int WR_NTIMES = 1000 / NUM_CLIENTS ;
            String run_id = "/s3_perf_run" + random.nextInt(1000000);
            
            public void run() {
                try {
                    testS3(BLOCK_SIZE, BLOCK_SIZE_META, WR_NTIMES, run_id);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
        // start all threads
        ArrayList<Thread> threads = new ArrayList<Thread>(NUM_CLIENTS);
        for (int i=0; i<10; i++) {
            FakeClient client = new FakeClient();
            Thread t = new Thread(client);
            t.start();
            threads.add(t);
        }


        // wait for them to finish
        for (int i=0; i<10; i++) {
            try {
                threads.get(i).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testS3RecordTime100K() throws IOException {
        long BLOCK_SIZE = 1024 * 100; // bytes
        long BLOCK_SIZE_META = 807; // bytes
        int WR_NTIMES = 30;
        String run_id = "/s3_perf_run" + random.nextInt(10000);

        testS3(BLOCK_SIZE, BLOCK_SIZE_META, WR_NTIMES, run_id);
    }
    
    
    public void parse_log_file(long total_time, String out) {
        // read captured stdout and count data
        int dfs_create_time = 0;
        int createRBW_time = 0;
        int receiveBlock_time = 0;
        int new_BlockReceiver_time = 0;
        int packet_responder_time = 0;
        int finalizeBlk_time=0;
        int upload_time = 0;
        int delete_time = 0;
        int opWriteBlock = 0;
        int completeFile_time = 0;
        
        String[] lines = out.split("\n");
        for (int i=0; i<lines.length;i++) {
            String[] parts = lines[i].split(" ");
            if (lines[i].contains("createRBW time")) {
                createRBW_time += Integer.parseInt(parts[2]);
                LOG.info(lines[i]);
            } else if (lines[i].contains("DFS.create")) {
                dfs_create_time += Integer.parseInt(parts[1]);
                LOG.info(lines[i]);
            } else if (lines[i].contains("new_BlockReceiver")) {
                new_BlockReceiver_time += Integer.parseInt(parts[1]);
                LOG.info(lines[i]);
            } else if (lines[i].contains("receiveBlock_time")) {
                receiveBlock_time += Integer.parseInt(parts[1]);
                LOG.info(lines[i]);
            } else if (lines[i].contains("packet_responder")) {
                packet_responder_time += Integer.parseInt(parts[1]);
                LOG.info(lines[i]);
            } else if (lines[i].contains("finalizeBlk_time")) {
                finalizeBlk_time += Integer.parseInt(parts[1]);
                LOG.info(lines[i]);
            } else if (lines[i].contains("Upload")) {
                upload_time += Integer.parseInt(parts[3]);
                LOG.info(lines[i]);
            } else if (lines[i].contains("Delete")) {
                delete_time += Integer.parseInt(parts[6]);
                LOG.info(lines[i]);
            } else if (lines[i].contains("opWriteblock")) {
                opWriteBlock += Integer.parseInt(parts[1]);
                LOG.info(lines[i]);
            } else if (lines[i].contains("completeFile")) {
                completeFile_time += Integer.parseInt(parts[1]);
                LOG.info(lines[i]);
            }
        }

        LOG.info("------------\n");
        LOG.info("dfs_create_time: " + dfs_create_time);
        LOG.info("new_BlockReceiver time: " + new_BlockReceiver_time + " (createRBW time: " + createRBW_time + ")" );
        LOG.info("receiveBlock_time : " + receiveBlock_time);
        LOG.info("packet_responder_time: " + packet_responder_time);
        LOG.info("finalizeBlk_time: " + finalizeBlk_time);
        LOG.info("Upload time: " + upload_time);
        LOG.info("S3Finalized.delete time: " + delete_time);
        LOG.info("opWriteBlock time: " + opWriteBlock);
        LOG.info("completeFile_time time: " + completeFile_time);

        double diffInSec = total_time / 1000.0;
        LOG.info("-----------------------\n" +
                "It took " + diffInSec + " seconds to write" +
                "\n---------------------------\n\n\n");
    }
    
    
    
    private void testHDFS(long fileSize, long blocksize, int wr_ntimes, String run_id) throws IOException {
        Configuration conf = new HdfsConfiguration();
        conf.setBoolean("test.use_local_s3", use_local_s3);
        conf.setBoolean(DFSConfigKeys.S3_DATASET, use_hdfs_s3_dataset);
        
        conf.setBoolean(FAST_UPLOAD, use_fast_upload_s3a);
        
        conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blocksize);
        // setup a cluster to run with
        if (use_hdfs_s3_dataset) {
            conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
            cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
        } else {
            conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
            cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
        }
        
        cluster.waitActive();
        mfs = cluster.getFileSystem();

        Path rootdir = new Path(ROOT_DIR);
        mfs.mkdirs(rootdir);

        // capture stdout
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        // read & write from HDFS
        long total_time = testWriteRead(mfs, run_id, false, fileSize, 0, wr_ntimes);
        
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        
        String out = baos.toString();
        parse_log_file(total_time, out);
        
        cluster.shutdown();
    }
    
    private void testS3(long fileSize, long fileSizeMeta, int wr_ntimes, String run_id) throws IOException {
        Configuration conf = new HdfsConfiguration();
        conf.setBoolean(FAST_UPLOAD, use_fast_upload_s3a);
        
        // set up a normal S3 file system
        this.s3afs = new S3AFileSystem();
        bucket = conf.get(DFSConfigKeys.S3_DATASET_BUCKET, "");
        URI rootURI = URI.create("s3a://" + bucket);
        s3afs.setWorkingDirectory(new Path("/"));
        s3afs.initialize(rootURI, conf);
        this.s3client = s3afs.getS3Client();

        // use fake s3
        if (use_local_s3) {
            s3client.setEndpoint("http://localhost:4567");
            s3client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true).disableChunkedEncoding());
        }
        
        // write/read block files
        long time1 = testWriteRead(s3afs, run_id, true, fileSize, fileSizeMeta, wr_ntimes);
        
        double diffInSec = time1 / 1000.0;
        LOG.info("In total=" + diffInSec + " seconds to write " + wr_ntimes + " files with size " + fileSize);
    }
    

    
    private long testWriteRead(FileSystem fs, String run_id, boolean is_s3, long fileSize, long fileSizeMeta, int num_files) {
        int countOfFailures = 0;
        byte[] outBuffer = new byte[(int)fileSize];
        byte[] inBuffer = new byte[(int)fileSize];

        byte[] outMetaBuffer = new byte[(int)fileSizeMeta];
        byte[] inMetaBuffer = new byte[(int)fileSizeMeta];

        int fileSizeAppend = (int) (fileSize / 2);
        byte[] out_append_buffer = new byte[fileSizeAppend];
        
        for (int i = 0; i < fileSize; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }
        if (is_s3) {
            for (int i = 0; i < fileSizeMeta; i++) {
                outMetaBuffer[i] = (byte) (i & 0x00fa);
            }    
        }

        if (do_append) {
            for (int i = 0; i < fileSizeAppend; i++) {
                out_append_buffer[i] = (byte) (i & 0x00fb);
            }
        }
        
        
        // counters
        long time_create = 0;
        int time_create_count = 0;
        
        long time_create_nn = 0;
        int time_create_nn_count = 0;
        
        long time_write = 0;
        int time_write_count = 0;
        
        long time_close = 0;
        int time_close_count = 0;
        
        long time_read = 0;
        int time_read_count = 0;
        
        Date beginTime = new Date();
        LOG.info("=== Starting WRITE process");
        for (int i =0; i < num_files; i++) {
            String fname = run_id + "/block_" + i;
            String fname_meta = fname + ".meta";
            
            Path f = new Path(fname);
            try {
                // simulate new block data
                for (int j = 0; j < fileSize; j++) {
                    outBuffer[j] = (byte) (j & 0x00ff);
                }
//                time_create += (new Date()).getTime() - start_create2.getTime();
//                time_create_count++;
                
//                Date start_create = new Date();
//                // WRite the file
//                FSDataOutputStream out = fs.create(f);
//                // counters
//                long diffInMillies_create = (new Date()).getTime() - start_create.getTime();
//                time_create += diffInMillies_create;
//                time_create_count++;
//                LOG.info("=== Client create file: " + diffInMillies_create + " ms to create new file & return output handle");
//
//                Date start_write = new Date();
//                writeData(out, outBuffer, (int) fileSize);
//                long diffInMillies_write = (new Date()).getTime() - start_write.getTime();
//                time_write += diffInMillies_write;
//                time_write_count++;
//                LOG.info("=== Client write file: " + diffInMillies_write + " ms to write all data to file");
//
//                Date start_close = new Date();
//                out.close();
//                long diffInMillies_close = (new Date()).getTime() - start_close.getTime();
//                time_close += diffInMillies_close;
//                time_close_count++;
//                LOG.info("=== Client close file: " + diffInMillies_close + " ms to upload file to s3" );
                
                if (do_append) {
                    Date start_append_create = new Date();    
                    FSDataOutputStream out_append = fs.append(f);
                    time_create += (new Date()).getTime() - start_append_create.getTime();
                    time_create_count++;

                    Date start_append_write = new Date();
                    out_append.write(out_append_buffer, 0, out_append_buffer.length);
                    out_append.write(out_append_buffer, 0, out_append_buffer.length);
                    time_write += (new Date()).getTime() - start_append_write.getTime();
                    time_write_count++;

                    Date start_append_close = new Date();
                    out_append.close();
                    time_close += (new Date()).getTime() - start_append_close.getTime();
                    time_close_count++;
                }
                
                if (is_s3) {
                    // simulate new block metadata...
                    Date start_create_meta = new Date();
                    for (int j = 0; j < fileSizeMeta; j++) {
                        outMetaBuffer[j] = (byte) (j & 0x00fa);
                    }
                    // fake checking status of the file once
                    // do this OR s3afs.contains
                    // fake synchronized GET (DN does this).
//                    synchronized (this ) {
//                        s3afs.exists(new Path(fname));
//                    }
//                        ObjectMetadata s3Object_meta = s3afs.getObjectMetadata(f);
                    long diffInMillies_create_file = (new Date()).getTime() - start_create_meta.getTime();
                    time_create += diffInMillies_create_file;
                    time_create_count++;
                    LOG.info(" Client create meta file: " + diffInMillies_create_file + " ms");

//                     WRITE new meta file
                    Date start_write_meta2 = new Date();
                    File local_meta_file  = new File("tmp" + fname_meta);
                    FileUtils.writeByteArrayToFile(local_meta_file, outMetaBuffer);
                    long diffInMillies_write_meta2 = (new Date()).getTime() - start_write_meta2.getTime();
                    time_write += diffInMillies_write_meta2;
                    LOG.info(" Client write meta file: " + diffInMillies_write_meta2 + " ms to create new file & return output handle");

//
                    // WRITE BLOCK FILE
                    // for local s3, we write otuput to file and then upload that file manually
                    Date start_write = new Date();
                    File local_block_file = new File("tmp" + fname);
                    FileUtils.writeByteArrayToFile(local_block_file, outBuffer);
                    long diffInMillies_write = (new Date()).getTime() - start_write.getTime();
                    time_write += diffInMillies_write;
                    time_write_count++; // only once
                    LOG.info(" Client write file: " + diffInMillies_write + " ms to write all data to file");


                    // CLOSE BLOCK
                    Date start_close = new Date();
                    PutObjectRequest putReqBlock = new PutObjectRequest(bucket, fname.substring(1), local_block_file);
                    PutObjectResult putResult = s3afs.getS3Client().putObject(putReqBlock);

                    // close meta block
                    PutObjectRequest putReqMeta = new PutObjectRequest(bucket, fname_meta.substring(1), local_meta_file);
                    PutObjectResult putMetaResult = s3afs.getS3Client().putObject(putReqMeta);

                    long diffInMillies_close_meta = (new Date()).getTime() - start_close.getTime();
                    time_close += diffInMillies_close_meta;
                    time_close_count++; // only do this once
                    LOG.info(" Client close file: " + diffInMillies_close_meta + " ms to upload file to s3" );
                    
                    local_block_file.delete();
                    local_meta_file.delete();
                    

                    // reads this from S3 now
//                        readData(fname, inBuffer, outBuffer.length, 0, fs, blockSize);
//                        readData(fname_meta, inMetaBuffer, outMetaBuffer.length, 0, fs, blockSizeMeta);
                    
                } else {
                    // HDFS runs
                    Date start_create = new Date();
                    // WRite the file
                    FSDataOutputStream out = fs.create(f);
//                    FSDataOutputStream out2 = fs.create(f, (short) 1);

                    // counters
                    long diffInMillies_create = (new Date()).getTime() - start_create.getTime();
                    time_create += diffInMillies_create;
                    time_create_count++;
                    LOG.info("=== Client create file: " + diffInMillies_create + " ms to create new file & return output handle");

                    Date start_write = new Date();
                    writeData(out, outBuffer, (int) fileSize);
                    long diffInMillies_write = (new Date()).getTime() - start_write.getTime();
                    time_write += diffInMillies_write;
                    time_write_count++;
                    LOG.info("=== Client write file: " + diffInMillies_write + " ms to write all data to file");

                    Date start_close = new Date();
                    out.close();
                    long diffInMillies_close = (new Date()).getTime() - start_close.getTime();
                    time_close += diffInMillies_close;
                    time_close_count++;
                    LOG.info("=== Client close file: " + diffInMillies_close + " ms to upload file to s3" );
                    
//                    // Read the file
//                    Date start_read = new Date();
//
//                    File dest1  = new File("tmp" + fname + ".downloaded");
//                    FileUtils.copyInputStreamToFile(fs.open(new Path(fname)).getWrappedStream(), dest1);
//
//                    // make sure file exists
//                    if (! dest1.exists()) {
//                        Assert.fail("File " + dest1 + " failed to download");
//                    }
//                    
//                    long diffInMillies_read = (new Date()).getTime() - start_read.getTime();
//                    time_read += diffInMillies_read;
//                    time_read_count++;
//                    // cleanup
//                    dest1.delete();

                }
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        }

        LOG.info("=== Starting READ process");
        for (int i =0; i < num_files; i++) {
            try {
                String fname = run_id + "/block_" + i;
                String fname_meta = fname + ".meta";
                File local_block_file = new File("tmp" + fname);
                File local_meta_file  = new File("tmp" + fname_meta);

                LOG.info("Reading file " + fname);
                Date start_read = new Date();
                
                if (is_s3) {
                    // fake checking status of the file once, since DN does this
                    // do this OR s3afs.contains
                    // fake synchronized GET (DN does this).
//                    synchronized (this ) {
//                        s3afs.getFileStatus(new Path(fname));    
//                    }
                }
                
                File dest1 = new File(local_block_file + ".downloaded");
                FileUtils.copyInputStreamToFile(fs.open(new Path(fname)).getWrappedStream(), dest1);
                Assert.assertEquals(fileSize, dest1.length());

                // make sure file exists
                if (! dest1.exists()) {
                    Assert.fail("File " + dest1 + " failed to download");
                }
                // cleanup
                dest1.delete();

                if (is_s3) {
                    File dest2 = new File(local_meta_file + ".downloaded");
                    FileUtils.copyInputStreamToFile(fs.open(new Path(fname_meta)).getWrappedStream(), dest2);
                    if (! dest2.exists()) {
                        Assert.fail("File " + dest2 + " failed to download");
                    }
                    Assert.assertEquals(fileSizeMeta, dest2.length());
                    dest2.delete();
                }
                
                time_read += (new Date()).getTime() - start_read.getTime();
                time_read_count++;
                
            } catch (IOException e) {
                LOG.error(e);
            }
            
        }
        
        Date endTime = new Date();
        Assert.assertEquals(0, countOfFailures);
        
        // counters
        double time_create_avg = time_create / time_create_count;
        double time_write_avg = time_write / time_write_count;
        double time_close_avg = time_close / time_close_count;
        double time_read_avg = time_read / time_read_count;
        
        LOG.info("------------------ \n");
        LOG.info("Create avg time: " + time_create_avg);
        LOG.info("Write avg time: " + time_write_avg);
        LOG.info("Close avg time: " + time_close_avg);
        LOG.info("READ avg time: " + time_read_avg);
        LOG.info("------------------ ");
        LOG.info("Create total time: " + time_create);
        LOG.info("Write total time: " + time_write);
        LOG.info("Close total time: " + time_close);
        LOG.info("READ total time: " + time_read);
        LOG.info("------------------ \n");
        
        long diffInMillies = endTime.getTime() - beginTime.getTime();
        return diffInMillies;
    }

    // copied from TestWriteRead
    private void writeData(FSDataOutputStream out, byte[] buffer, int length)
            throws IOException {

        int totalByteWritten = 0;
        int remainToWrite = length;

        while (remainToWrite > 0) {
            int toWriteThisRound =
                    remainToWrite > buffer.length ? buffer.length : remainToWrite;
            
            out.write(buffer, 0, toWriteThisRound);
            totalByteWritten += toWriteThisRound;
            remainToWrite -= toWriteThisRound;
        }
        if (totalByteWritten != length) {
            throw new IOException(
                    "WriteData: failure in write. Attempt to write " + length +
                            " ; written=" + totalByteWritten);
        }
    }

    /**
     * Open the file to read from begin to end. Then close the file.
     * Return number of bytes read.
     * Support both sequential read and position read.
     */
    // copied from TestWriteRead
    private long readData(String fname, byte[] buffer, long byteExpected, long beginPosition, FileSystem fs, long blockSize) throws IOException {
        long totalByteRead = 0;
        Path path = fs.makeQualified(new Path(fname));

        FSDataInputStream in = null;
        try {
            in = fs.open(path);

            long visibleLenFromReadStream = blockSize;
//                    ((HdfsDataInputStream) in).getVisibleLength();

            if (visibleLenFromReadStream < byteExpected) {
                throw new IOException(visibleLenFromReadStream +
                        " = visibleLenFromReadStream < bytesExpected= " + byteExpected);
            }

            totalByteRead =
                    readUntilEnd(in, buffer, buffer.length, fname, beginPosition,
                            visibleLenFromReadStream, positionReadOption);
            in.close();

            // reading more data than visibleLeng is OK, but not less
            if (totalByteRead + beginPosition < byteExpected) {
                throw new IOException(
                        "readData mismatch in byte read: expected=" + byteExpected +
                                " ; got " + (totalByteRead + beginPosition));
            }
            return totalByteRead + beginPosition;

        } catch (IOException e) {
            throw new IOException(
                    "##### Caught Exception in readData. " + "Total Byte Read so far = " +
                            totalByteRead + " beginPosition = " + beginPosition, e);
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    /**
     * read chunks into buffer repeatedly until total of VisibleLen byte are
     * read.
     * Return total number of bytes read
     */
    private long readUntilEnd(FSDataInputStream in, byte[] buffer, long size,
                              String fname, long pos, long visibleLen, boolean positionReadOption)
            throws IOException {

        if (pos >= visibleLen || visibleLen <= 0) {
            return 0;
        }

        int chunkNumber = 0;
        long totalByteRead = 0;
        long currentPosition = pos;
        int byteRead = 0;
        long byteLeftToRead = visibleLen - pos;
        int byteToReadThisRound = 0;

        if (!positionReadOption) {
            in.seek(pos);
            currentPosition = in.getPos();
        }
        if (verboseOption) {
            LOG.info("reader begin: position: " + pos + " ; currentOffset = " +
                    currentPosition + " ; bufferSize =" + buffer.length +
                    " ; Filename = " + fname);
        }
        try {
            while (byteLeftToRead > 0 && currentPosition < visibleLen) {
                byteToReadThisRound =
                        (int) (byteLeftToRead >= buffer.length ? buffer.length :
                                byteLeftToRead);
                if (positionReadOption) {
                    byteRead = in.read(currentPosition, buffer, 0, byteToReadThisRound);
                } else {
                    byteRead = in.read(buffer, 0, byteToReadThisRound);
                }
                if (byteRead <= 0) {
                    break;
                }
                chunkNumber++;
                totalByteRead += byteRead;
                currentPosition += byteRead;
                byteLeftToRead -= byteRead;

                if (verboseOption) {
                    LOG.info("reader: Number of byte read: " + byteRead +
                            " ; totalByteRead = " + totalByteRead + " ; currentPosition=" +
                            currentPosition + " ; chunkNumber =" + chunkNumber +
                            "; File name = " + fname);
                }
            }
        } catch (IOException e) {
            throw new IOException(
                    "#### Exception caught in readUntilEnd: reader  currentOffset = " +
                            currentPosition + " ; totalByteRead =" + totalByteRead +
                            " ; latest byteRead = " + byteRead + "; visibleLen= " +
                            visibleLen + " ; bufferLen = " + buffer.length +
                            " ; Filename = " + fname, e);
        }

        if (verboseOption) {
            LOG.info("reader end:   position: " + pos + " ; currentOffset = " +
                    currentPosition + " ; totalByteRead =" + totalByteRead +
                    " ; Filename = " + fname);
        }

        return totalByteRead;
    }
    
        
    


    /**
     * If run as normal script,  
     */
    public static void main(String[] args) {
        try {
            S3PerformanceTest test = new S3PerformanceTest();
            
            String file_path = args[0];
            
            String log_file_str = new String(Files.readAllBytes(Paths.get(file_path)), StandardCharsets.UTF_8);
            
            test.parse_log_file(0, log_file_str);
            System.exit(0);
        } catch (IOException e) {
//            LOG.info("#### Exception in Main");
            e.printStackTrace();
            System.exit(-2);
        }
    }
}

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.fs.s3a.s3guard.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Date;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.TEST_FS_S3A_NAME;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.test.TestJUnitSetup.LOG;

public class S3GuardPerformanceTest {

    private static String BUCKET;
    private static String S3URI;

    private static DynamoDBMetadataStore dynamo_ms = new DynamoDBMetadataStore();
    private Configuration dynamo_conf;
    private S3AFileSystem dynamo_s3afs;

    private static NDBMetadataStore ndb_ms = new NDBMetadataStore();
    private Configuration ndb_conf;
    private S3AFileSystem ndb_s3afs;
    private  boolean has_setup = false;
    
//    private String NAMENODE_RCP_ADDRESS = null;
    private String NAMENODE_RCP_ADDRESS = "10.24.0.15:8020";
    private final int NUM_FILES = 100;
    private final int NUM_SUBDIRS = 100;
    private final int fileSize = 1024;
    

    private static MiniDFSCluster cluster;

    @Before
    public void initJunitModeTest() throws Exception {
        LOG.info("initJunitModeTest");

        dynamo_conf = new Configuration();
        // set in auth-keys.xml 'test.fs.s3a.name' and 'fs.s3a.endpoint' to correct bucket settings
        BUCKET = dynamo_conf.get(TEST_FS_S3A_NAME, "s3guard-ndb-vs-dynamo-test");
        S3URI = URI.create(FS_S3A + "://" + BUCKET + "/").toString();

        // Setup DynamoDB
        dynamo_conf.set(S3_METADATA_STORE_IMPL, S3GUARD_METASTORE_DYNAMO);
        dynamo_conf.setClass(S3_CLIENT_FACTORY_IMPL, DefaultS3ClientFactory.class, S3ClientFactory.class);
        dynamo_conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, S3URI);
        dynamo_conf.setBoolean(S3GUARD_DDB_TABLE_CREATE_KEY, true);
        dynamo_conf.setClass(S3Guard.S3GUARD_DDB_CLIENT_FACTORY_IMPL,
                DynamoDBClientFactory.DefaultDynamoDBClientFactory.class, DynamoDBClientFactory.class);

        // always create new file system object for a test contract
        dynamo_s3afs = (S3AFileSystem) FileSystem.newInstance(dynamo_conf);
        dynamo_ms.initialize(dynamo_s3afs);


        // setup NDB
        ndb_conf = new Configuration();
        if (NAMENODE_RCP_ADDRESS != null) {
            ndb_conf.set(DFS_NAMENODE_RPC_ADDRESS_KEY, NAMENODE_RCP_ADDRESS);    
        } else {
            cluster = new MiniDFSCluster.Builder(ndb_conf).numDataNodes(1).build();
            cluster.waitActive();
        }
        
        ndb_conf.set(S3_METADATA_STORE_IMPL, S3GUARD_METASTORE_NDB);
        ndb_conf.setClass(S3_CLIENT_FACTORY_IMPL, DefaultS3ClientFactory.class, S3ClientFactory.class);
        ndb_conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, S3URI);
        // table should already exist
        ndb_conf.setBoolean(S3GUARD_DDB_TABLE_CREATE_KEY, false);

        // always create new file system object for a test contract
        ndb_s3afs = (S3AFileSystem) FileSystem.newInstance(ndb_conf);
        ndb_ms.initialize(ndb_s3afs);

        // clear the DB for every test
//        ndb_ms.getNameNodeClient().s3MetadataDeleteBucket(BUCKET);
//        dynamo_ms.deleteSubtree("/");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (dynamo_ms != null) {
//            dynamo_ms.destroy();
        }
    }

    private  void setup_bucket() throws IOException {
        if (has_setup) {
            return;
        }

        byte[] outBuffer = new byte[(int) fileSize];
        for (int i = 0; i < fileSize; i++) {
            outBuffer[i] = (byte) (i & 0x00ff);
        }
        String file_path = "tmp/test_file";
        File file = new File(file_path);
        FileUtils.writeByteArrayToFile(file, outBuffer);

        LOG.info(dynamo_ms.toString());

        for (int i = 0; i < NUM_FILES; i++) {
            for (int j = 0; j < NUM_SUBDIRS; j++) {

                String key;
                if (j % 2 == 0) {
                    key = "subdir" + j + "/subdir" + j + 1 + "/file" + i;
                } else {
                    key = "subdir" + j + "/file" + i;
                }
                LOG.info("Uploading " + key + " to S3 and meta into DynamoDB and NDB.");
                dynamo_s3afs.copyFromLocalFile(new Path(file_path), new Path(key));
                ndb_s3afs.copyFromLocalFile(new Path(file_path), new Path(key));
            }
        }
        
        has_setup = true;
    }
    
    private int ls_tree(int count, DirListingMetadata dir, MetadataStore ms, boolean delete ) throws IOException {
        for (PathMetadata path : dir.getListing()) {
            LOG.info(path);
            count++;
            if (path.getFileStatus().isDirectory()) {
                count = ls_tree(count, ms.listChildren(path.getFileStatus().getPath()), ms, delete);
            }
            if (delete) {
                ms.delete(path.getFileStatus().getPath());
            }
        }
        return count;
    }


    @Test
    public void testLSAuthMode() throws IOException {
        LOG.info("=== Starting test");
        setup_bucket();

        Path rootDir = new Path(S3URI );
        
        // TODO measure time taken by s3guard with DynamoDB to LS dirs
        Date start_list_dynamo = new Date();
        
        DirListingMetadata rootDirList_dynamo = dynamo_ms.listChildren(rootDir);
        LOG.info(rootDirList_dynamo.prettyPrint());
        int tree_size_dynamo = ls_tree(0, rootDirList_dynamo, dynamo_ms, false);
        LOG.info("Found " + tree_size_dynamo + " DynamoDB files");
        
        long diffInMillies_dynamo = (new Date()).getTime() - start_list_dynamo.getTime();


        // TODO measure time taken by s3guard with NDB to LS dirs

        Date start_list_ndb = new Date();
        DirListingMetadata rootDirList_ndb = ndb_ms.listChildren(rootDir);
        LOG.info(rootDirList_ndb.prettyPrint());
        int tree_size_ndb = ls_tree(0, rootDirList_ndb, ndb_ms, false);
        LOG.info("Found " + tree_size_ndb + " NDB files");

        long diffInMillies_ndb = (new Date()).getTime() - start_list_ndb.getTime();

        // summary
        LOG.info("diffInMillies_dynamo: " + diffInMillies_dynamo + " ms");
        LOG.info("diffInMillies_ndb: " + diffInMillies_ndb  + " ms");

        Assert.assertEquals(tree_size_dynamo, tree_size_ndb);
    }

    @Test
    public void testLSDelete() throws IOException {
        LOG.info("=== Starting test");
        setup_bucket();
        Path rootDir = new Path(S3URI );

        // TODO measure time taken by s3guard with DynamoDB to LS dirs then delete
        Date start_list_dynamo = new Date();

        DirListingMetadata rootDirList_dynamo = dynamo_ms.listChildren(rootDir);
        LOG.info(rootDirList_dynamo.prettyPrint());
        int tree_size_dynamo = ls_tree(0, rootDirList_dynamo, dynamo_ms, true);
        LOG.info("Found " + tree_size_dynamo + " DynamoDB files");

        long diffInMillies_dynamo = (new Date()).getTime() - start_list_dynamo.getTime();


        // TODO measure time taken by s3guard with NDB to LS dirs then delete
        Date start_list_ndb = new Date();
        
        DirListingMetadata rootDirList_ndb = ndb_ms.listChildren(rootDir);
        LOG.info(rootDirList_ndb.prettyPrint());
        int tree_size_ndb = ls_tree(0, rootDirList_ndb, ndb_ms, true);
        LOG.info("Found " + tree_size_ndb + " NDB files");
        
        long diffInMillies_ndb = (new Date()).getTime() - start_list_ndb.getTime();

        // summary
        LOG.info("diffInMillies_dynamo: " + diffInMillies_dynamo + " ms");
        LOG.info("diffInMillies_ndb: " + diffInMillies_ndb  + " ms");

        Assert.assertEquals(tree_size_dynamo, tree_size_ndb);
        
    }
}

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CloudBlock;
import org.apache.hadoop.hdfs.server.common.CloudHelper;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.CloudPersistenceProvider;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CloudPersistenceProviderS3Impl implements CloudPersistenceProvider {

  @VisibleForTesting
  public static final Log LOG = LogFactory.getLog(CloudPersistenceProviderS3Impl.class);

  private final Configuration conf;
  private final AmazonS3 s3Client;
  private final String bucketPrefix;
  private final String bucketIDSeparator = ".";
  private final Regions region;
  private final int numBuckets;
  private final int prefixSize;
  private TransferManager transfers;
  private ExecutorService threadPoolExecutor;
  private final int bucketDeletionThreads;

  CloudPersistenceProviderS3Impl(Configuration conf) {
    this.conf = conf;
    this.bucketPrefix = conf.get(DFSConfigKeys.S3_BUCKET_PREFIX,
            DFSConfigKeys.S3_BUCKET_PREFIX_DEFAULT);
    this.region = Regions.fromName(conf.get(DFSConfigKeys.DFS_CLOUD_AWS_S3_REGION,
            DFSConfigKeys.DFS_CLOUD_AWS_S3_REGION_DEFAULT));
    this.numBuckets = conf.getInt(DFSConfigKeys.DFS_CLOUD_AWS_S3_NUM_BUCKETS,
            DFSConfigKeys.DFS_CLOUD_AWS_S3_NUM_BUCKETS_DEFAULT);
    this.bucketDeletionThreads =
            conf.getInt(DFSConfigKeys.DFS_NN_MAX_THREADS_FOR_FORMATTING_CLOUD_BUCKETS_KEY,
                    DFSConfigKeys.DFS_NN_MAX_THREADS_FOR_FORMATTING_CLOUD_BUCKETS_DEFAULT);
    this.prefixSize = conf.getInt(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY,
            DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_DEFAULT);

    this.s3Client = connect();

    initTransferManager();
  }

  private AmazonS3 connect() {
    LOG.info("HopsFS-Cloud. Connecting to S3. Region " + region);
    ClientConfiguration s3conf =  new ClientConfiguration();
    int retryCount = conf.getInt(DFSConfigKeys.DFS_CLOUD_FAILED_OPS_RETRY_COUNT_KEY,
            DFSConfigKeys.DFS_CLOUD_FAILED_OPS_RETRY_COUNT_DEFAULT);
    s3conf.withThrottledRetries(true);
    s3conf.setMaxErrorRetry(retryCount);
    LOG.info("Max retry "+s3conf.getMaxErrorRetry());
    AmazonS3 s3client = AmazonS3ClientBuilder.standard()
            .withRegion(region)
            .build();

    return s3client;
  }

  private void initTransferManager() {
    int maxThreads = conf.getInt(DFSConfigKeys.DFS_DN_CLOUD_MAX_TRANSFER_THREADS,
            DFSConfigKeys.DFS_DN_CLOUD_MAX_TRANSFER_THREADS_DEFAULT);
    if (maxThreads < 2) {
      LOG.warn(DFSConfigKeys.DFS_DN_CLOUD_MAX_TRANSFER_THREADS +
              " must be at least 2: forcing to 2.");
      maxThreads = 2;
    }

    long keepAliveTime = conf.getLong(DFSConfigKeys.DFS_CLOUD_KEEPALIVE_TIME,
            DFSConfigKeys.DFS_CLOUD_KEEPALIVE_TIME_DEFAULT);

    threadPoolExecutor = new ThreadPoolExecutor(
            maxThreads, Integer.MAX_VALUE,
            keepAliveTime, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            BlockingThreadPoolExecutorService.newDaemonThreadFactory(
                    "hopsfs-cloud-transfers-unbounded"));

    long partSize = conf.getLong(DFSConfigKeys.DFS_CLOUD_MULTIPART_SIZE,
            DFSConfigKeys.DFS_CLOUD_MULTIPART_SIZE_DEFAULT);

    if (partSize < 5 * 1024 * 1024) {
      LOG.error(DFSConfigKeys.DFS_CLOUD_MULTIPART_SIZE + " must be at least 5 MB");
      partSize = 5 * 1024 * 1024;
    }

    long multiPartThreshold = conf.getLong(DFSConfigKeys.DFS_CLOUD_MIN_MULTIPART_THRESHOLD,
            DFSConfigKeys.DFS_CLOUD_MIN_MULTIPART_THRESHOLD_DEFAULT);
    if (multiPartThreshold < 5 * 1024 * 1024) {
      LOG.error(DFSConfigKeys.DFS_CLOUD_MIN_MULTIPART_THRESHOLD + " must be at least 5 MB");
      multiPartThreshold = 5 * 1024 * 1024;
    }

    TransferManagerConfiguration transferConfiguration = new TransferManagerConfiguration();
    transferConfiguration.setMinimumUploadPartSize(partSize);
    transferConfiguration.setMultipartUploadThreshold(multiPartThreshold);
    transferConfiguration.setMultipartCopyPartSize(partSize);
    transferConfiguration.setMultipartCopyThreshold(multiPartThreshold);

    transfers = new TransferManager(s3Client, threadPoolExecutor);
    transfers.setConfiguration(transferConfiguration);
  }

  static long longOption(Configuration conf,
                         String key,
                         long defVal,
                         long min) {
    long v = conf.getLong(key, defVal);
    Preconditions.checkArgument(v >= min,
            String.format("Value of %s: %d is below the minimum value %d",
                    key, v, min));
    return v;
  }

  private void createS3Bucket(String bucketName) {
    if (!s3Client.doesBucketExist(bucketName)) {
      s3Client.createBucket(bucketName);
      // Verify that the bucket was created by retrieving it and checking its location.
      String bucketLocation = s3Client.getBucketLocation(new GetBucketLocationRequest(bucketName));
      LOG.info("HopsFS-Cloud. New bucket created. Name: " +
              bucketName + " Location: " + bucketLocation);
    } else {
      LOG.info("HopsFS-Cloud. Bucket already exists. Bucket Name: " + bucketName);
    }
  }

  /*
  deletes all the bucket belonging to this user.
  This is only used for testing.
   */
  public void deleteAllBuckets(String prefix) {
    ExecutorService tPool = Executors.newFixedThreadPool(bucketDeletionThreads);
    try {
      List<Bucket> buckets = s3Client.listBuckets();
      LOG.info("HopsFS-Cloud. Deleting all of the buckets for this user. Number of deletion " +
              "threads " + bucketDeletionThreads);
      for (Bucket b : buckets) {
        if (b.getName().startsWith(prefix)) {
          emptyAndDeleteS3Bucket(b.getName(), tPool);
        }
      }
    } finally {
      tPool.shutdown();
    }
  }

  /*
  Deletes all the buckets that are used by HopsFS
   */
  @Override
  public void format() {
    ExecutorService tPool = Executors.newFixedThreadPool(bucketDeletionThreads);
    try {
      System.out.println("HopsFS-Cloud. Deleting all of the buckets used by HopsFS. Number of " +
              "deletion " +
              "threads " + bucketDeletionThreads);
      for (int i = 0; i < numBuckets; i++) {
        emptyAndDeleteS3Bucket(getBucketDNSID(i), tPool);
      }

      createBuckets();
    } finally {
      tPool.shutdown();
    }
  }

  @Override
  public void checkAllBuckets() {

    final int retry = 300;  // keep trying until the newly created bucket is available
    for (int i = 0; i < numBuckets; i++) {
      String bucketID = getBucketDNSID(i);
      boolean exists = false;
      for (int j = 0; j < retry; j++) {
        if (!s3Client.doesBucketExistV2(bucketID)) {
          //wait for a sec and retry
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
          }
          continue;
        } else {
          exists = true;
          break;
        }
      }

      if (!exists) {
        throw new IllegalStateException("S3 Bucket " + bucketID + " needed for the file system " +
                "does not exists");
      } else {
        //check the bucket is writable
        UUID uuid = UUID.randomUUID();
        try {
          s3Client.putObject(bucketID, uuid.toString()/*key*/, "test");
          s3Client.deleteObject(bucketID, uuid.toString()/*key*/);
        } catch (Exception e) {
          throw new IllegalStateException("Write test for S3 bucket: " + bucketID + " failed. " + e);
        }
      }
    }
  }

  private void createBuckets() {
    for (int i = 0; i < numBuckets; i++) {
      createS3Bucket(getBucketDNSID(i));
    }
  }


  private void emptyAndDeleteS3Bucket(final String bucketName, ExecutorService tPool) {
    final AtomicInteger deletedBlocks = new AtomicInteger(0);
    try {
      if (!s3Client.doesBucketExistV2(bucketName)) {
        return;
      }

      System.out.println("HopsFS-Cloud. Deleting bucket: " + bucketName);

      ObjectListing objectListing = s3Client.listObjects(bucketName);
      while (true) {
        Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries().iterator();

        final List<Callable<Object>> addTasks = new ArrayList<>();
        while (objIter.hasNext()) {
          final String objectKey = objIter.next().getKey();

          Callable task = new Callable<Object>() {
            @Override
            public Object call() throws Exception {
              s3Client.deleteObject(bucketName, objectKey);
              String msg = "\rDeleted Blocks: " + (deletedBlocks.incrementAndGet());
              System.out.print(msg);
              return null;
            }
          };
          tPool.submit(task);
        }

        // If the bucket contains many objects, the listObjects() call
        // might not return all of the objects in the first listing. Check to
        // see whether the listing was truncated. If so, retrieve the next page of objects
        // and delete them.
        if (objectListing.isTruncated()) {
          objectListing = s3Client.listNextBatchOfObjects(objectListing);
        } else {
          break;
        }
      }

      System.out.println("");

      // Delete all object versions (required for versioned buckets).
      VersionListing versionList = s3Client.listVersions(
              new ListVersionsRequest().withBucketName(bucketName));
      while (true) {
        Iterator<S3VersionSummary> versionIter = versionList.getVersionSummaries().iterator();
        while (versionIter.hasNext()) {
          S3VersionSummary vs = versionIter.next();
          s3Client.deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
        }

        if (versionList.isTruncated()) {
          versionList = s3Client.listNextBatchOfVersions(versionList);
        } else {
          break;
        }
      }

      // After all objects and object versions are deleted, delete the bucket.
      s3Client.deleteBucket(bucketName);
    } catch (AmazonServiceException up) {
      // The call was transmitted successfully, but Amazon S3 couldn't process
      // it, so it returned an error response.
      up.printStackTrace();
      throw up;
    } catch (SdkClientException up) {
      // Amazon S3 couldn't be contacted for a response, or the client couldn't
      // parse the response from Amazon S3.
      up.printStackTrace();
      throw up;
    }
  }


  @Override
  public void uploadObject(short bucketID, String objectID, File object,
                           Map<String, String> metadata) throws IOException {
    try {
      LOG.debug("HopsFS-Cloud. Put Object. Bucket ID: " + bucketID + " Object ID: " + objectID);

      long startTime = System.currentTimeMillis();
      String bucket = getBucketDNSID(bucketID);
      PutObjectRequest putReq = new PutObjectRequest(bucket,
              objectID, object);

      // Upload a file as a new object with ContentType and title specified.
      ObjectMetadata objMetadata = new ObjectMetadata();
      objMetadata.setContentType("plain/text");
      //objMetadata.addUserMetadata(entry.getKey(), entry.getValue());
      objMetadata.setUserMetadata(metadata);
      putReq.setMetadata(objMetadata);

      Upload upload = transfers.upload(putReq);

      upload.waitForUploadResult();
      LOG.info("HopsFS-Cloud. Put Object. Bucket ID: " + bucketID + " Object ID: " + objectID
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.toString());
    } catch (AmazonServiceException e) {
      throw new IOException(e);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  public String getBucketDNSID(int ID) {
    return bucketPrefix + bucketIDSeparator + ID;
  }

  @Override
  public int getPrefixSize() {
    return prefixSize;
  }

  @Override
  public boolean objectExists(short bucketID, String objectID) throws IOException {
    try {
      long startTime = System.currentTimeMillis();
      boolean exists = s3Client.doesObjectExist(getBucketDNSID(bucketID), objectID);
      LOG.debug("HopsFS-Cloud. Object Exists?. Bucket ID: " + bucketID + " Object ID: " + objectID
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
      return exists;
    } catch (AmazonServiceException e) {
      throw new IOException(e); // throwing runtime exception will kill DN
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  private ObjectMetadata getS3ObjectMetadata(short bucketID, String objectID)
          throws IOException {
    try {
      GetObjectMetadataRequest req = new GetObjectMetadataRequest(getBucketDNSID(bucketID),
              objectID);
      ObjectMetadata s3metadata = s3Client.getObjectMetadata(req);
      return s3metadata;
    } catch (AmazonServiceException e) {
      throw new IOException(e); // throwing runtime exception will kill DN
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }


  @Override
  public Map<String, String> getUserMetaData(short bucketID, String objectID)
          throws IOException {
    long startTime = System.currentTimeMillis();
    ObjectMetadata s3metadata = getS3ObjectMetadata(bucketID, objectID);
    Map<String, String> metadata = s3metadata.getUserMetadata();
    LOG.info("HopsFS-Cloud. Get Object Metadata. Bucket ID: " + bucketID + " Object ID: " + objectID
            + " Time (ms): " + (System.currentTimeMillis() - startTime));
    return metadata;
  }

  @Override
  public long getObjectSize(short bucketID, String objectID) throws IOException {
    long startTime = System.currentTimeMillis();
    ObjectMetadata s3metadata = getS3ObjectMetadata(bucketID, objectID);
    long size = s3metadata.getContentLength();
    LOG.debug("HopsFS-Cloud. Get Object Size. Bucket ID: " + bucketID + " Object ID: " + objectID
            + " Time (ms): " + (System.currentTimeMillis() - startTime));
    return size;
  }

  @Override
  public void downloadObject(short bucketID, String objectID, File path) throws IOException {
    try {
      long startTime = System.currentTimeMillis();
      Download down = transfers.download(getBucketDNSID(bucketID), objectID, path);
      down.waitForCompletion();
      LOG.info("HopsFS-Cloud. Download Object. Bucket ID: " + bucketID + " Object ID: " + objectID
              + " Download Path: " + path
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
    } catch (AmazonServiceException e) {
      throw new IOException(e); // throwing runtime exception will kill DN
    } catch (SdkClientException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.toString());
    }
  }

  @Override
  public Map<Long, CloudBlock> getAll(String prefix) throws IOException {
    Map<Long, CloudBlock> blocks = new HashMap<>();
    for (int i = 0; i < numBuckets; i++) {
      listBucket(getBucketDNSID(i), prefix, blocks);
    }
    return blocks;
  }

  @Override
  public void deleteObject(short bucketID, String objectID) throws IOException {
    try {
      s3Client.deleteObject(getBucketDNSID(bucketID), objectID);
    } catch (AmazonServiceException up) {
      throw new IOException(up);
    } catch (SdkClientException up) {
      throw new IOException(up);
    }
  }

  @Override
  public void shutdown() {
    s3Client.shutdown();
    if (transfers != null) {
      transfers.shutdownNow(true);
      transfers = null;
    }
  }

  private void listBucket(String bucketName, String prefix, Map<Long, CloudBlock> result)
          throws IOException {
    Map<Long, S3ObjectSummary> blockObjs = new HashMap<>();
    Map<Long, S3ObjectSummary> metaObjs = new HashMap<>();

    try {
      if (!s3Client.doesBucketExist(bucketName)) {
        return;
      }

      assert prefix != null;

      ObjectListing objectListing = s3Client.listObjects(bucketName, prefix);
      while (true) {
        Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries().iterator();
        while (objIter.hasNext()) {
          S3ObjectSummary s3Object = objIter.next();
          String key = s3Object.getKey();

          if (CloudHelper.isBlockFilename(key)) {
            long blockID = CloudHelper.extractBlockIDFromBlockName(key);
            blockObjs.put(blockID, s3Object);
          } else if (CloudHelper.isMetaFilename(key)) {
            long blockID = CloudHelper.extractBlockIDFromMetaName(key);
            metaObjs.put(blockID, s3Object);
          } else {
            LOG.warn("HopsFS-Cloud. File system objects are tampered. The " + key + " is not HopsFS object.");
          }
        }

        if (objectListing.isTruncated()) {
          objectListing = s3Client.listNextBatchOfObjects(objectListing);
        } else {
          break;
        }
      }
    } catch (AmazonServiceException up) {
      throw new IOException(up);
    } catch (SdkClientException up) {
      throw new IOException(up);
    }

    mergeMetaAndBlockObjects(metaObjs, blockObjs, result);

    return;
  }

  private void mergeMetaAndBlockObjects(Map<Long, S3ObjectSummary> metaObjs,
                                        Map<Long, S3ObjectSummary> blockObjs,
                                        Map<Long, CloudBlock> res) {

    Set blockKeySet = blockObjs.keySet();
    Set metaKeySet = metaObjs.keySet();
    Sets.SetView<Long> symDiff = Sets.symmetricDifference(blockKeySet, metaKeySet);
    Sets.SetView<Long> intersection = Sets.intersection(blockKeySet, metaKeySet);

    for (Long blockID : intersection) {
      S3ObjectSummary blockObj = blockObjs.get(blockID);
      S3ObjectSummary metaObj = metaObjs.get(blockID);

      long blockSize = blockObj.getSize();
      short bucketID = CloudHelper.extractBucketID(blockObj.getBucketName());

      //Generation stamps of the meta file and block much match
      assert CloudHelper.extractGSFromBlockName(blockObj.getKey()) ==
              CloudHelper.extractGSFromMetaName(metaObj.getKey());
      long genStamp = CloudHelper.extractGSFromMetaName(metaObj.getKey());

      Block block = new Block(blockID, blockSize, genStamp, bucketID);

      CloudBlock cb = new CloudBlock(block, blockObj.getLastModified().getTime());
      res.put(blockID, cb);
    }

    for (Long id : symDiff) {
      String keyFound = "";
      String bucket = "";
      CloudBlock cb = new CloudBlock();

      S3ObjectSummary blockObj = blockObjs.get(id);
      S3ObjectSummary metaObj = metaObjs.get(id);

      if (blockObj != null) {
        cb.setBlockObjectFound(true);
        cb.setLastModified(blockObj.getLastModified().getTime());
        keyFound = blockObj.getKey();
        bucket = blockObj.getBucketName();
      } else if (metaObj != null) {
        cb.setMetaObjectFound(true);
        cb.setLastModified(metaObj.getLastModified().getTime());
        keyFound = metaObj.getKey();
        bucket = metaObj.getBucketName();
      }

      long blockID;
      long gs;

      if (CloudHelper.isMetaFilename(keyFound)) {
        blockID = CloudHelper.extractBlockIDFromMetaName(keyFound);
        gs = CloudHelper.extractGSFromMetaName(keyFound);
      } else if (CloudHelper.isBlockFilename(keyFound)) {
        blockID = CloudHelper.extractBlockIDFromBlockName(keyFound);
        gs = CloudHelper.extractGSFromBlockName(keyFound);
      } else {
        LOG.warn("HopsFS-Cloud. File system objects are tampered. The " + keyFound + " is not HopsFS " +
                "object.");
        continue;
      }

      Block block = new Block();
      block.setBlockIdNoPersistance(blockID);
      block.setGenerationStampNoPersistance(gs);
      block.setCloudBucketIDNoPersistance(CloudHelper.extractBucketID(bucket));
      cb.setBlock(block);
      res.put(id, cb);
    }
  }

  public void renameObject(short srcBucket, short dstBucket, String srcKey,
                           String dstKey) throws IOException {
    try {
      CopyObjectRequest req = new CopyObjectRequest(getBucketDNSID(srcBucket), srcKey,
              getBucketDNSID(dstBucket), dstKey);
      CopyObjectResult res = s3Client.copyObject(req);

      //delete the src
      deleteObject(srcBucket, srcKey);
    } catch (AmazonServiceException up) {
      throw new IOException(up);
    } catch (SdkClientException up) {
      throw new IOException(up);
    }


  }

}

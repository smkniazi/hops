package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.hops.metadata.hdfs.entity.CloudBucket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CloudBlock;
import org.apache.hadoop.hdfs.server.common.CloudHelper;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ActiveMultipartUploads;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.CloudPersistenceProvider;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class CloudPersistenceProviderS3Impl implements CloudPersistenceProvider {

  @VisibleForTesting
  public static final Log LOG = LogFactory.getLog(CloudPersistenceProviderS3Impl.class);

  private final Configuration conf;
  private final AmazonS3 s3Client;
  private final Regions region;
  private final int prefixSize;
  private TransferManager transfers;
  private final int bucketDeletionThreads;
  private long partSize;
  private int maxThreads;
  private long multiPartThreshold;

  CloudPersistenceProviderS3Impl(Configuration conf) {
    this.conf = conf;
    this.region = Regions.fromName(conf.get(DFSConfigKeys.DFS_CLOUD_AWS_S3_REGION,
            DFSConfigKeys.DFS_CLOUD_AWS_S3_REGION_DEFAULT));
    this.bucketDeletionThreads =
            conf.getInt(DFSConfigKeys.DFS_NN_MAX_THREADS_FOR_FORMATTING_CLOUD_BUCKETS_KEY,
                    DFSConfigKeys.DFS_NN_MAX_THREADS_FOR_FORMATTING_CLOUD_BUCKETS_DEFAULT);
    this.prefixSize = conf.getInt(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY,
            DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_DEFAULT);
    maxThreads = conf.getInt(DFSConfigKeys.DFS_DN_CLOUD_MAX_TRANSFER_THREADS,
            DFSConfigKeys.DFS_DN_CLOUD_MAX_TRANSFER_THREADS_DEFAULT);
    if (maxThreads < 2) {
      LOG.warn(DFSConfigKeys.DFS_DN_CLOUD_MAX_TRANSFER_THREADS +
              " must be at least 2: forcing to 2.");
      maxThreads = 2;
    }

    this.s3Client = connect();
    initTransferManager();
  }

  private AmazonS3 connect() {
    LOG.info("HopsFS-Cloud. Connecting to S3. Region " + region);
    ClientConfiguration s3conf = new ClientConfiguration();
    int retryCount = conf.getInt(DFSConfigKeys.DFS_CLOUD_FAILED_OPS_RETRY_COUNT_KEY,
            DFSConfigKeys.DFS_CLOUD_FAILED_OPS_RETRY_COUNT_DEFAULT);
    s3conf.withThrottledRetries(true);
    s3conf.setMaxErrorRetry(retryCount);
    s3conf.setMaxConnections(maxThreads);
    LOG.info("Max retry " + s3conf.getMaxErrorRetry());
    AmazonS3 s3client = AmazonS3ClientBuilder.standard()
            .withRegion(region)
            .build();

    return s3client;
  }

  public void initTransferManager() {
    partSize = conf.getLong(DFSConfigKeys.DFS_CLOUD_MULTIPART_SIZE,
            DFSConfigKeys.DFS_CLOUD_MULTIPART_SIZE_DEFAULT);

    if (partSize < 5 * 1024 * 1024) {
      LOG.error(DFSConfigKeys.DFS_CLOUD_MULTIPART_SIZE + " must be at least 5 MB");
      partSize = 5 * 1024 * 1024;
    }

    multiPartThreshold = conf.getLong(DFSConfigKeys.DFS_CLOUD_MIN_MULTIPART_THRESHOLD,
            DFSConfigKeys.DFS_CLOUD_MIN_MULTIPART_THRESHOLD_DEFAULT);
    if (multiPartThreshold < 5 * 1024 * 1024) {
      LOG.error(DFSConfigKeys.DFS_CLOUD_MIN_MULTIPART_THRESHOLD + " must be at least 5 MB");
      multiPartThreshold = 5 * 1024 * 1024;
    }

    transfers =
            TransferManagerBuilder.standard().withS3Client(s3Client).
                    withExecutorFactory(new ExecutorFactory() {
                      @Override
                      public ExecutorService newExecutor() {
                        return Executors.newFixedThreadPool(maxThreads);
                      }
                    }).
                    withMultipartUploadThreshold(multiPartThreshold).
                    withMinimumUploadPartSize(partSize).
                    withMultipartCopyThreshold(multiPartThreshold).
                    withMultipartCopyPartSize(partSize).build();
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
      LOG.info("HopsFS-Cloud. Deleting all of the buckets with prefix \""+prefix+
              "\" for this user. Number of deletion threads " + bucketDeletionThreads);
      for (Bucket b : buckets) {

        if (b.getName().startsWith(prefix.toLowerCase())) {
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
  public void format(List<String> buckets) {
    ExecutorService tPool = Executors.newFixedThreadPool(bucketDeletionThreads);
    try {
      System.out.println("HopsFS-Cloud. Deleting all of the buckets used by HopsFS. Number of " +
              "deletion " +
              "threads " + bucketDeletionThreads);
      for (String bucket : buckets) {
        emptyAndDeleteS3Bucket(bucket, tPool);
      }

      createBuckets(buckets);
    } finally {
      tPool.shutdown();
    }
  }

  @Override
  public void checkAllBuckets(List<String> buckets) {

    final int retry = 300;  // keep trying until the newly created bucket is available
    for (String bucket : buckets) {
      boolean exists = false;
      for (int j = 0; j < retry; j++) {
        if (!s3Client.doesBucketExistV2(bucket)) {
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
        throw new IllegalStateException("S3 Bucket " + bucket + " needed for the file system " +
                "does not exists");
      } else {
        //check the bucket is writable
        UUID uuid = UUID.randomUUID();
        try {
          s3Client.putObject(bucket, uuid.toString()/*key*/, "test");
          s3Client.deleteObject(bucket, uuid.toString()/*key*/);
        } catch (Exception e) {
          throw new IllegalStateException("Write test for S3 bucket: " + bucket + " failed. " + e);
        }
      }
    }
  }

  private void createBuckets(List<String> buckets) {
    for (String bucket : buckets) {
      createS3Bucket(bucket);
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
  public void uploadObject(String bucket, String objectKey, File object,
                           Map<String, String> metadata) throws IOException {
    try {
      LOG.debug("HopsFS-Cloud. Put Object. Bucket: " + bucket + " Object Key: " + objectKey);

      long startTime = System.currentTimeMillis();
      PutObjectRequest putReq = new PutObjectRequest(bucket,
              objectKey, object);

      // Upload a file as a new object with ContentType and title specified.
      ObjectMetadata objMetadata = new ObjectMetadata();
      objMetadata.setContentType("plain/text");
      //objMetadata.addUserMetadata(entry.getKey(), entry.getValue());
      objMetadata.setUserMetadata(metadata);
      putReq.setMetadata(objMetadata);

      Upload upload = transfers.upload(putReq);

      upload.waitForUploadResult();
      LOG.info("HopsFS-Cloud. Put Object. Bucket: " + bucket + " Object Key: " + objectKey
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.toString());
    } catch (AmazonServiceException e) {
      throw new IOException(e);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int getPrefixSize() {
    return prefixSize;
  }

  @Override
  public boolean objectExists(String bucket, String objectKey) throws IOException {
    try {
      long startTime = System.currentTimeMillis();
      boolean exists = s3Client.doesObjectExist(bucket, objectKey);
      LOG.debug("HopsFS-Cloud. Object Exists?. Bucket: " + bucket + " Object Key: " + objectKey
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
      return exists;
    } catch (AmazonServiceException e) {
      throw new IOException(e); // throwing runtime exception will kill DN
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  private ObjectMetadata getS3ObjectMetadata(String bucket, String objectKey)
          throws IOException {
    try {
      GetObjectMetadataRequest req = new GetObjectMetadataRequest(bucket,
              objectKey);
      ObjectMetadata s3metadata = s3Client.getObjectMetadata(req);
      return s3metadata;
    } catch (AmazonServiceException e) {
      throw new IOException(e); // throwing runtime exception will kill DN
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }


  @Override
  public Map<String, String> getUserMetaData(String bucket, String objectKey)
          throws IOException {
    long startTime = System.currentTimeMillis();
    ObjectMetadata s3metadata = getS3ObjectMetadata(bucket, objectKey);
    Map<String, String> metadata = s3metadata.getUserMetadata();
    LOG.info("HopsFS-Cloud. Get Object Metadata. Bucket: " + bucket + " Object Key: " + objectKey
            + " Time (ms): " + (System.currentTimeMillis() - startTime));
    return metadata;
  }

  @Override
  public long getObjectSize(String bucket, String objectKey) throws IOException {
    long startTime = System.currentTimeMillis();
    ObjectMetadata s3metadata = getS3ObjectMetadata(bucket, objectKey);
    long size = s3metadata.getContentLength();
    LOG.debug("HopsFS-Cloud. Get Object Size. Bucket: " + bucket + " Object Key: " + objectKey
            + " Time (ms): " + (System.currentTimeMillis() - startTime));
    return size;
  }

  @Override
  public void downloadObject(String bucket, String objectKey, File path) throws IOException {
    try {
      long startTime = System.currentTimeMillis();
      Download down = transfers.download(bucket, objectKey, path);
      down.waitForCompletion();
      LOG.info("HopsFS-Cloud. Download Object. Bucket: " + bucket + " Object Key: " + objectKey
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
  public Map<Long, CloudBlock> getAll(String prefix, List<String> buckets) throws IOException {
    Map<Long, CloudBlock> blocks = new HashMap<>();
    for (String bucket : buckets) {
      listBucket(bucket, prefix, blocks);
    }
    return blocks;
  }

  @Override
  public void deleteObject(String bucket, String objectKey) throws IOException {
    try {
      long startTime = System.currentTimeMillis();
      s3Client.deleteObject(bucket, objectKey);
      LOG.info("HopsFS-Cloud. Delete object. Bucket: " + bucket + " Object Key: " + objectKey
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
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

      //Generation stamps of the meta file and block much match
      assert CloudHelper.extractGSFromBlockName(blockObj.getKey()) ==
              CloudHelper.extractGSFromMetaName(metaObj.getKey());
      long genStamp = CloudHelper.extractGSFromMetaName(metaObj.getKey());

      Block block = new Block(blockID, blockSize, genStamp, blockObj.getBucketName());

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
      block.setCloudBucketNoPersistance(bucket);
      cb.setBlock(block);
      res.put(id, cb);
    }
  }

  public void renameObject(String srcBucket, String dstBucket, String srcKey,
                           String dstKey) throws IOException {
    try {
      long startTime = System.currentTimeMillis();
      CopyObjectRequest req = new CopyObjectRequest(srcBucket, srcKey,
              dstBucket, dstKey);
      CopyObjectResult res = s3Client.copyObject(req);
      LOG.info("HopsFS-Cloud. Rename object. Src Bucket: " + srcBucket +
              " Dst Bucket: " + dstBucket +
              " Src Object Key: " + srcKey +
              " Dst Object Key: " + dstKey +
              " Time (ms): " + (System.currentTimeMillis() - startTime));
      //delete the src
      deleteObject(srcBucket, srcKey);
    } catch (AmazonServiceException up) {
      throw new IOException(up);
    } catch (SdkClientException up) {
      throw new IOException(up);
    }


  }

  @Override
  public long getPartSize() {
    return partSize;
  }

  @Override
  public int getXferThreads(){
    return maxThreads;
  }

  @Override
  public String startMultipartUpload(String bucket, String objectKey, Map<String, String> metadata) throws IOException {
    try {
      long startTime = System.currentTimeMillis();
      InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket,
              objectKey);

      ObjectMetadata objMetadata = new ObjectMetadata();
      objMetadata.setContentType("plain/text");
      //objMetadata.addUserMetadata(entry.getKey(), entry.getValue());
      objMetadata.setUserMetadata(metadata);
      initRequest.setObjectMetadata(objMetadata);

      InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

      LOG.info("HopsFS-Cloud. Start multipart upload. Bucket: " + bucket + " Object Key: " + objectKey
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
      return initResponse.getUploadId();
    } catch (AmazonServiceException up) {
      throw new IOException(up);
    } catch (SdkClientException up) {
      throw new IOException(up);
    }
  }

  @Override
  public PartETag uploadPart(String bucket, String objectKey, String uploadID, int partNo,
                             File file, long startPos, long endPos) throws IOException {
    try {
      long startTime = System.currentTimeMillis();
      UploadPartRequest uploadRequest = new UploadPartRequest()
              .withBucketName(bucket)
              .withKey(objectKey)
              .withUploadId(uploadID)
              .withPartNumber(partNo)
              .withFileOffset(startPos)
              .withFile(file)
              .withPartSize(endPos - startPos);

      UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
      LOG.info("HopsFS-Cloud. Upload part. Bucket: " + bucket + " Object Key: " + objectKey + " " +
              "PartNo: " + partNo + " Time (ms): " + (System.currentTimeMillis() - startTime));
      return uploadResult.getPartETag();
    } catch (AmazonServiceException up) {
      throw new IOException(up);
    } catch (SdkClientException up) {
      throw new IOException(up);
    }
  }

  @Override
  public void finalizeMultipartUpload(String bucket, String objectKey, String uploadID,
                                      List<PartETag> partETags) throws IOException {
    try {
      long startTime = System.currentTimeMillis();
      CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucket, objectKey,
              uploadID, partETags);
      s3Client.completeMultipartUpload(compRequest);
      LOG.info("HopsFS-Cloud. Finalize multipart upload. Bucket: " + bucket +
              " Object Key: " + objectKey + " " + "Total Parts: " + partETags.size() +
              " Time (ms): " + (System.currentTimeMillis() - startTime));
    } catch (AmazonServiceException up) {
      throw new IOException(up);
    } catch (SdkClientException up) {
      throw new IOException(up);
    }
  }

  @Override
  public void abortMultipartUpload(String bucket, String objectKey, String uploadID)
          throws IOException {
    try {
      long startTime = System.currentTimeMillis();
      AbortMultipartUploadRequest req = new AbortMultipartUploadRequest(bucket, objectKey, uploadID);
      s3Client.abortMultipartUpload(req);
      LOG.info("HopsFS-Cloud. Aborted multipart upload. Bucket: " + bucket +
              " Object Key: " + objectKey + " Time (ms): " + (System.currentTimeMillis() - startTime));
    } catch (AmazonServiceException up) {
      throw new IOException(up);
    } catch (SdkClientException up) {
      throw new IOException(up);
    }
  }

  @Override
  public List<ActiveMultipartUploads> listMultipartUploads(List<String> buckets) throws IOException {
    List<ActiveMultipartUploads> uploads = new ArrayList();
    long startTime = System.currentTimeMillis();
    for (String bucket : buckets) {
      uploads.addAll(listMultipartUploadsForBucket(bucket));
    }
    LOG.info("HopsFS-Cloud. List multipart. Active Uploads " + uploads.size() +
            " Time (ms): " + (System.currentTimeMillis() - startTime));
    return uploads;
  }

  private List<ActiveMultipartUploads> listMultipartUploadsForBucket(String bucket)
          throws IOException {
    List<ActiveMultipartUploads> uploads = new ArrayList();
    try {
      MultipartUploadListing uploadListing =
              s3Client.listMultipartUploads(new ListMultipartUploadsRequest(bucket));
      do {
        for (MultipartUpload upload : uploadListing.getMultipartUploads()) {
          uploads.add(new ActiveMultipartUploads(bucket, upload.getKey(),
                  upload.getInitiated().getTime(), upload.getUploadId()));
        }
        ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(bucket)
                .withUploadIdMarker(uploadListing.getNextUploadIdMarker())
                .withKeyMarker(uploadListing.getNextKeyMarker());
        uploadListing = s3Client.listMultipartUploads(request);
      } while (uploadListing.isTruncated());
    } catch (AmazonServiceException up) {
      throw new IOException(up);
    } catch (SdkClientException up) {
      throw new IOException(up);
    }
    return uploads;
  }
}

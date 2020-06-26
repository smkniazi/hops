/*
 * Copyright (C) 2020 Logical Clocks AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.cloud;

import com.azure.core.exception.AzureException;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.*;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.CloudBlock;
import org.apache.hadoop.hdfs.server.common.CloudHelper;

import java.io.*;
import java.security.MessageDigest;
import java.util.*;


public class CloudPersistenceProviderAzureImpl implements CloudPersistenceProvider {

  public static final Log LOG = LogFactory.getLog(CloudPersistenceProviderAzureImpl.class);

  BlobServiceClient blobClient;
  private final Configuration conf;
  private final int prefixSize;
  private int maxThreads;
  private long partSize;

  public CloudPersistenceProviderAzureImpl(Configuration conf) throws IOException {
    this.conf = conf;

    String storageConnectionString = null;

    if (System.getenv("AZURE_STORAGE_CONNECTION_STRING") != null) {
      storageConnectionString = System.getenv("AZURE_STORAGE_CONNECTION_STRING");
    }

    if(storageConnectionString == null) {
      throw new IllegalArgumentException("AZURE_STORAGE_CONNECTION_STRING evn variable is not set");
    }

    this.prefixSize = conf.getInt(DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_KEY,
            DFSConfigKeys.DFS_CLOUD_PREFIX_SIZE_DEFAULT);
    maxThreads = conf.getInt(DFSConfigKeys.DFS_DN_CLOUD_MAX_TRANSFER_THREADS,
            DFSConfigKeys.DFS_DN_CLOUD_MAX_TRANSFER_THREADS_DEFAULT);
    if (maxThreads < 2) {
      LOG.warn(DFSConfigKeys.DFS_DN_CLOUD_MAX_TRANSFER_THREADS +
              " must be at least 2: forcing to 2.");
      maxThreads = 2;
    }

    partSize = conf.getLong(DFSConfigKeys.DFS_CLOUD_MULTIPART_SIZE,
            DFSConfigKeys.DFS_CLOUD_MULTIPART_SIZE_DEFAULT);

    if (partSize < 5 * 1024 * 1024) {
      LOG.error(DFSConfigKeys.DFS_CLOUD_MULTIPART_SIZE + " must be at least 5 MB");
      partSize = 5 * 1024 * 1024;
    }

    int retryCount = conf.getInt(DFSConfigKeys.DFS_CLOUD_FAILED_OPS_RETRY_COUNT_KEY,
            DFSConfigKeys.DFS_CLOUD_FAILED_OPS_RETRY_COUNT_DEFAULT);

    try {
      RequestRetryOptions retryOptions = new RequestRetryOptions(RetryPolicyType.EXPONENTIAL,
              retryCount, null /*timeout*/, null/*retryDelayInMs*/,
              null/*maxRetryDelayInMs*/, null/*secondaryHost*/);
      blobClient = new BlobServiceClientBuilder().
              connectionString(storageConnectionString).
              retryOptions(retryOptions).buildClient();

      LOG.info("Azure Connected ");
    } catch (AzureException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deleteAllBuckets(String prefix) throws IOException {
    try {
      long startTime = System.currentTimeMillis();

      ListBlobContainersOptions options = new ListBlobContainersOptions();
      options.setPrefix(prefix.toLowerCase());
      for (BlobContainerItem cont : blobClient.listBlobContainers(options, null)) {
        LOG.info("Deleting container: " + cont.getName());
        BlobContainerClient bcc = blobClient.getBlobContainerClient(cont.getName());
        if (bcc.exists()) {
          bcc.delete();
        }
      }
      LOG.debug("HopsFS-Cloud. Delete all containers. Prefix: " + prefix
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
    } catch (AzureException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void format(List<String> containers) throws IOException {
    try {
      long startTime = System.currentTimeMillis();

      //delete containers
      for (String containerStr : containers) {
        new CloudActionHandler() {
          @Override
          public Object task() throws IOException {
            LOG.info("Deleting container: " + containerStr);
            BlobContainerClient bcc = blobClient.getBlobContainerClient(containerStr);
            if (bcc.exists()) {
              bcc.delete();
            }
            return null;
          }
        }.performTask();
      }

      //recreate containers
      for (String constainerStr : containers) {
        new CloudActionHandler() {
          @Override
          public Object task() throws IOException {
            LOG.info("Creating container: " + constainerStr);
            BlobContainerClient bcc = blobClient.getBlobContainerClient(constainerStr);
            bcc.create();
            return null;
          }
        }.performTask();
      }
      LOG.debug("HopsFS-Cloud. Format containers: " + Arrays.toString(containers.toArray())
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
    } catch (AzureException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void checkAllBuckets(List<String> containers) throws IOException {
    try {
      long startTime = System.currentTimeMillis();

      final int retry = 300;  // keep trying until the newly created container is available
      for (String contStr : containers) {
        LOG.debug("Checking container: " + contStr);
        boolean exists = false;
        BlobContainerClient bcc = null;
        for (int j = 0; j < retry; j++) {
          bcc = blobClient.getBlobContainerClient(contStr);
          if (!bcc.exists()) {
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
          throw new IllegalStateException("Azure Container " + contStr + " needed for the " +
                  "file system does not exists");
        } else {
          //check the container is writable
          UUID uuid = UUID.randomUUID();
          try {
            BlobClient bc = bcc.getBlobClient(uuid.toString()/*key*/);
            String message = "hello! hello! testing! testing! testing 1 2  3!";
            File file1 = new File("/tmp/" + uuid);
            File file2 = new File("/tmp/" + uuid + ".downloaded");
            FileWriter fw = new FileWriter(file1);
            fw.write(message);
            fw.close();
            bc.uploadFromFile(file1.getAbsolutePath());
            bc.downloadToFile(file2.getAbsolutePath());
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash1 = com.google.common.io.Files.getDigest(file1, md);
            byte[] hash2 = com.google.common.io.Files.getDigest(file2, md);
            file1.delete();
            file2.delete();
            bc.delete();
            assert Arrays.equals(hash1, hash2) == true;
          } catch (Exception e) {
            throw new IllegalStateException("Write test for Azure container: " + contStr +
                    " failed. " + e);
          }
        }
      }

      LOG.info("HopsFS-Cloud. Check all containers: " + Arrays.toString(containers.toArray())
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
    } catch (AzureException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int getPrefixSize() {
    return prefixSize;
  }

  @Override
  public void uploadObject(String container, String objectKey, File file,
                           HashMap<String, String> metadata) throws IOException {
    try {
      long startTime = System.currentTimeMillis();

      BlobContainerClient bcc = blobClient.getBlobContainerClient(container);
      BlobClient bc = bcc.getBlobClient(objectKey);
      bc.uploadFromFile(file.getAbsolutePath());
      bc.setMetadata(metadata);

      LOG.debug("HopsFS-Cloud. Put Object. Container: " + container + " Object Key: " + objectKey
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
    } catch (AzureException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean objectExists(String container, String objectKey) throws IOException {
    try {
      long startTime = System.currentTimeMillis();

      BlobContainerClient bcc = blobClient.getBlobContainerClient(container);
      BlobClient bc = bcc.getBlobClient(objectKey);

      LOG.debug("HopsFS-Cloud. Obj Exists? Container: " + container + " Object Key: " + objectKey
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
      return bc.exists();
    } catch (AzureException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Map<String, String> getUserMetaData(String container, String objectKey)
          throws IOException {
    try {
      long startTime = System.currentTimeMillis();

      BlobContainerClient bcc = blobClient.getBlobContainerClient(container);
      BlobClient bc = bcc.getBlobClient(objectKey);

      LOG.debug("HopsFS-Cloud. Get metadata. Container: " + container + " Object Key: " + objectKey
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
      return bc.getProperties().getMetadata();
    } catch (AzureException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getObjectSize(String container, String objectKey) throws IOException {
    try {
      long startTime = System.currentTimeMillis();

      BlobContainerClient bcc = blobClient.getBlobContainerClient(container);
      BlobClient bc = bcc.getBlobClient(objectKey);

      LOG.debug("HopsFS-Cloud. Get obj size. Container: " + container + " Object Key: " + objectKey
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
      return bc.getProperties().getBlobSize();
    } catch (AzureException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void downloadObject(String container, String objectKey, File path) throws IOException {
    try {
      long startTime = System.currentTimeMillis();
      if (path.exists()) {
        path.delete();
      } else {
        //make sure that all parent dirs exists
        if (!path.getParentFile().exists()) {
          path.getParentFile().mkdirs();
        }
      }

      Random rand = new Random(System.currentTimeMillis());
      File tmpFile = new File(path.getAbsolutePath() + "." + rand.nextLong() + ".downloading");
      if (tmpFile.exists()) {
        tmpFile.delete();
      }

      BlobContainerClient bcc = blobClient.getBlobContainerClient(container);
      BlobClient bc = bcc.getBlobClient(objectKey);
      BlobProperties props = bc.downloadToFile(tmpFile.getAbsolutePath());

      tmpFile.renameTo(path);

      LOG.debug("HopsFS-Cloud. Download obj. Container: " + container + " Object Key: " + objectKey
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
    } catch (AzureException e) {
      throw new IOException(e);
    }
  }

  /*
   * only for testing
   */
  @VisibleForTesting
  @Override
  public Map<Long, CloudBlock> getAll(String prefix, List<String> containers) throws IOException {
    long startTime = System.currentTimeMillis();
    Map<Long, CloudBlock> blocks = new HashMap<>();
    for (String contStr : containers) {
      listContainer(contStr, prefix, blocks);
    }
    LOG.debug("HopsFS-Cloud. Get all blocks. Containers: " + Arrays.toString(containers.toArray()) +
            " Total Blocks: " + blocks.size() +
            " Time (ms): " + (System.currentTimeMillis() - startTime));
    return blocks;
  }

  private void listContainer(String container, String prefix, Map<Long, CloudBlock> result)
          throws IOException {
    try {
      Map<Long, CloudObject> blockObjs = new HashMap<>();
      Map<Long, CloudObject> metaObjs = new HashMap<>();

      BlobContainerClient bcc = blobClient.getBlobContainerClient(container);
      ListBlobsOptions lbo = new ListBlobsOptions();
      lbo.setPrefix(prefix);
      Iterator<BlobItem> itr = bcc.listBlobs(lbo, null).iterator();

      while (itr.hasNext()) {
        BlobItem item = itr.next();
        String key = item.getName();

        CloudObject co = new CloudObject();
        co.setBucket(container);
        co.setKey(item.getName());
        co.setSize(item.getProperties().getContentLength());
        co.setLastModifiedTime(item.getProperties().getLastModified().toEpochSecond());

        if (CloudHelper.isBlockFilename(key)) {
          long blockID = CloudHelper.extractBlockIDFromBlockName(key);
          blockObjs.put(blockID, co);
        } else if (CloudHelper.isMetaFilename(key)) {
          long blockID = CloudHelper.extractBlockIDFromMetaName(key);
          metaObjs.put(blockID, co);
        } else {
          LOG.warn("HopsFS-Cloud. File system objects are tampered. The " + key + " is not HopsFS object.");
        }
      }

      CloudPersistenceProviderS3Impl.mergeMetaAndBlockObjects(metaObjs, blockObjs, result);
    } catch (AzureException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deleteObject(String container, String objectKey) throws IOException {
    try {
      long startTime = System.currentTimeMillis();

      BlobContainerClient bcc = blobClient.getBlobContainerClient(container);
      BlobClient bc = bcc.getBlobClient(objectKey);
      bc.delete();
      LOG.debug("HopsFS-Cloud. Delete Object. Container: " + container + " Object Key: " + objectKey
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
    } catch (AzureException e) {
      throw new IOException(e);
    }
  }

  /*
  only for testing
   */
  @VisibleForTesting
  @Override
  public void renameObject(String srcContainer, String dstContainer, String srcKey, String dstKey)
          throws IOException {
    try {
      long startTime = System.currentTimeMillis();

      BlobContainerClient srcBcc = blobClient.getBlobContainerClient(srcContainer);
      BlobClient srcBc = srcBcc.getBlobClient(srcKey);
      UUID uuid = UUID.randomUUID();
      File file = new File("/tmp/" + uuid);
      srcBc.downloadToFile(file.getAbsolutePath());
      srcBc.delete();

      BlobContainerClient dstBcc = blobClient.getBlobContainerClient(dstContainer);
      BlobClient dstBc = dstBcc.getBlobClient(dstKey);
      dstBc.uploadFromFile(file.getAbsolutePath());
      file.delete();
      LOG.debug("HopsFS-Cloud. Rename Object. Src Container: " + srcContainer + " Src Object Key: " + srcKey +
              " Dst Container: " + dstContainer + " Dst Object Key: " + dstKey
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
    } catch (AzureException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getPartSize() {
    return partSize;
  }

  @Override
  public int getXferThreads() {
    return maxThreads;
  }

  @Override
  public UploadID startMultipartUpload(String container, String objectKey,
                                       Map<String, String> metadata)
          throws IOException {
    LOG.debug("HopsFS-Cloud. Starting Multipart Upload.");
    return null;
  }

  @Override
  public PartRef uploadPart(String container, String objectKey, UploadID uploadID, int partNo,
                            File file, long startPos, long endPos) throws IOException {
    long startTime = System.currentTimeMillis();
    try {
      BlockBlobClient bbc = blobClient.getBlobContainerClient(container).
              getBlobClient(objectKey).getBlockBlobClient();

      String id64 = base64ID(partNo);
      FileInputStream fis = new FileInputStream(file);
      fis.skip(startPos);
      int len = (int) (endPos - startPos);
      byte[] bytes = new byte[len];
      int read = fis.read(bytes, 0, len);
      LOG.debug("HopsFS-Cloud. Uploading part: " + partNo + "  base64: " + id64 + " len: " + len);
      bbc.stageBlock(id64, new ByteArrayInputStream(bytes), read);
      LOG.debug("HopsFS-Cloud. Upload Part.  Container: " + container + " Object Key: " + objectKey
              + " PartID: " + id64 + " Part Size: " + len
              + " Time (ms): " + (System.currentTimeMillis() - startTime));
      return new AzurePartRef(id64);

    } catch (AzureException e) {
      throw new IOException(e);
    }
  }

  private String base64ID(int id) {
    String id64 = new String(Base64.getEncoder().encode(String.format("%09d", id).getBytes()));
    return id64;
  }

  @Override
  public void finalizeMultipartUpload(String container, String objectKey, UploadID uploadID,
                                      List<PartRef> refs) throws IOException {
    long startTime = System.currentTimeMillis();
    try {
      List<String> ids = new ArrayList<String>();
      for (PartRef ref : refs) {
        ids.add(((AzurePartRef) ref).getId64());
      }

      BlockBlobClient bbc = blobClient.getBlobContainerClient(container).
              getBlobClient(objectKey).getBlockBlobClient();
      bbc.commitBlockList(ids);
      LOG.debug("HopsFS-Cloud. Finalize Multipart Upload. Container: " + container +
              " Object Key: " + objectKey + " Parts: " + ids.size() +
              " Time (ms): " + (System.currentTimeMillis() - startTime));
    } catch (AzureException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void abortMultipartUpload(String container, String objectKey, UploadID uploadID)
          throws IOException {
    throw new UnsupportedOperationException("");
  }

  @Override
  public List<ActiveMultipartUploads> listMultipartUploads(List<String> container)
          throws IOException {
    throw new UnsupportedOperationException("Operation not supported for azure");
  }

  @Override
  public void shutdown() {
  }

  public BlobServiceClient getBlobClient() {
    return blobClient;
  }

  private abstract class CloudActionHandler {
    public abstract Object task() throws IOException;

    public Object performTask() throws IOException {
      List<Exception> exceptions = new ArrayList<>();
      long sleepTime = 500;
      for (int i = 0; i < 10; i++) {

        try {
          if (i != 0) {
            sleepTime = sleepTime * 2;
            LOG.info("HopsFS-Cloud. Operation Failed. Cause: " + exceptions.get(exceptions.size() - 1)
                    + " Retrying operation after " + sleepTime + " ms. Retry Count: "+i);
            Thread.sleep(sleepTime);
          }
        } catch (InterruptedException e) {
        }

        try {
          Object obj = task();
          return obj;
        } catch (AzureException e) {
          exceptions.add(e);
        }
      }

      for (Exception e : exceptions) {
        LOG.info("Supressed Exception", e);
      }
      throw new IOException(exceptions.get(0));
    }
  }

}

package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import com.amazonaws.services.s3.model.PartETag;
import org.apache.hadoop.hdfs.protocol.CloudBlock;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface CloudPersistenceProvider {
  /*
  deletes all the bucket belonging to the user.
  This is only used for testing.
   */
  public void deleteAllBuckets(String prefix);

  /*
  Deletes all the buckets that are used by HopsFS
   */
  public void format(List<String> buckets);

  /*
  Check that all the buckets needed exist
  throws runtime exception if the buckets dont exists or not writable.
   */
  public void checkAllBuckets(List<String> buckets);

  public int getPrefixSize();

  public void uploadObject(String bucket, String objectKey, File object,
                           Map<String, String> metadata) throws IOException;

  public boolean objectExists(String bucket, String objectKey)
          throws IOException;

  public Map<String, String> getUserMetaData(String bucket, String objectKey)
          throws IOException;

  public long getObjectSize(String bucket, String objectKey)
          throws IOException;

  public void downloadObject(String bucket, String objectKey, File path)
          throws IOException;

  public Map<Long, CloudBlock> getAll(String prefix, List<String> buckets) throws IOException;

  public void deleteObject(String bucket, String objectKey) throws IOException;

  public void renameObject(String srcBucket, String dstBucket, String srcKey,
                           String dstKey) throws IOException ;

  public long getPartSize();

  public int getXferThreads();

  public String startMultipartUpload(String bucket, String objectKey,
                                     Map<String, String> metadata)
          throws IOException;

  public PartETag uploadPart(String bucket, String objectKey, String uploadID,
                             int partNo, File file, long startPos, long endPos)
          throws IOException;

  public void finalizeMultipartUpload(String bucket, String objectKey,
                                      String uploadID, List<PartETag> partETags)
          throws IOException;

  public void abortMultipartUpload(String bucket, String objectKey, String uploadID)
          throws IOException;

  public List<ActiveMultipartUploads> listMultipartUploads(List<String> buckets) throws IOException;

  public void shutdown();
}

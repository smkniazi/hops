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
  public void format();

  /*
  Check that all the buckets needed exist
  throws runtime exception if the buckets dont exists or not writable.
   */
  public void checkAllBuckets();

  public String getBucketDNSID(int ID);

  public int getPrefixSize();

  public void uploadObject(short bucketID, String objectID, File object,
                           Map<String, String> metadata) throws IOException;

  public boolean objectExists(short bucketID, String objectID)
          throws IOException;

  public Map<String, String> getUserMetaData(short bucketID, String objectID)
          throws IOException;

  public long getObjectSize(short bucketID, String objectID)
          throws IOException;

  public void downloadObject(short bucketID, String objectID, File path)
          throws IOException;

  public Map<Long, CloudBlock> getAll(String prefix) throws IOException;

  public void deleteObject(short bucketID, String objectID) throws IOException;

  public void renameObject(short srcBucket, short dstBucket, String srcKey,
                           String dstKey) throws IOException ;

  public long getPartSize();

  public int getXferThreads();

  public String startMultipartUpload(short bucketID, String objectID,
                                     Map<String, String> metadata)
          throws IOException;

  public PartETag uploadPart(short bucketID, String objectID, String uploadID,
                             int partNo, File file, long startPos, long endPos)
          throws IOException;

  public void finalizeMultipartUpload(short bucketID, String objectID,
                                      String uploadID, List<PartETag> partETags)
          throws IOException;

  public void abortMultipartUpload(short bucketID, String objectID, String uploadID)
          throws IOException;

  public List<ActiveMultipartUploads> listMultipartUploads() throws IOException;

  public void shutdown();
}

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

import com.amazonaws.services.s3.model.PartETag;
import org.apache.hadoop.hdfs.protocol.CloudBlock;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface CloudPersistenceProvider {
  /*
  deletes all the bucket belonging to the user.
  This is only used for testing.
   */
  public void deleteAllBuckets(String prefix) throws IOException;

  /*
  Deletes all the buckets that are used by HopsFS
   */
  public void format(List<String> buckets) throws IOException;

  /*
  Check that all the buckets needed exist
  throws runtime exception if the buckets dont exists or not writable.
   */
  public void checkAllBuckets(List<String> buckets) throws IOException;

  public int getPrefixSize();

  public void uploadObject(String bucket, String objectKey, File object,
                           HashMap<String, String> metadata) throws IOException;

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

  public UploadID startMultipartUpload(String bucket, String objectKey,
                                     Map<String, String> metadata)
          throws IOException;

  public PartRef uploadPart(String bucket, String objectKey, UploadID uploadID,
                             int partNo, File file, long startPos, long endPos)
          throws IOException;

  public void finalizeMultipartUpload(String bucket, String objectKey,
                                      UploadID uploadID, List<PartRef> partETags)
          throws IOException;

  public void abortMultipartUpload(String bucket, String objectKey, UploadID uploadID)
          throws IOException;

  public List<ActiveMultipartUploads> listMultipartUploads(List<String> buckets) throws IOException;

  public void shutdown();
}

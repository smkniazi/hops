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

public class ActiveMultipartUploads {
  private final String bucket;
  private final String objectID;
  private final long startTime;
  private final UploadID uploadID;

  public ActiveMultipartUploads(String bucket, String objectID, long startTime,
                                UploadID uploadID) {
    this.bucket = bucket;
    this.objectID = objectID;
    this.startTime = startTime;
    this.uploadID = uploadID;
  }

  public String getObjectID() {
    return objectID;
  }

  public long getStartTime() {
    return startTime;
  }

  public UploadID getUploadID() {
    return uploadID;
  }

  public String getBucket() {
    return bucket;
  }
}

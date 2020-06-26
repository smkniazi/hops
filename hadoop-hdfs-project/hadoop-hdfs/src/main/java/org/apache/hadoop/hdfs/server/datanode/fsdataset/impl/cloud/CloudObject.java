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

public class CloudObject {
  private long size;
  private String key;
  private String bucket;
  private long lastModifiedTime;

  public void setSize(long size) {
    this.size = size;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public void setLastModifiedTime(long lastModifiedTime) {
    this.lastModifiedTime = lastModifiedTime;
  }

  public long getSize() {
    return size;
  }

  public String getKey() {
    return key;
  }

  public String getBucketName() {
    return bucket;

  }

  public long getLastModified() {
    return lastModifiedTime;
  }
}

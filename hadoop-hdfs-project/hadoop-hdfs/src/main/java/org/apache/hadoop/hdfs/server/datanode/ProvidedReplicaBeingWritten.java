/*
 * Copyright (C) 2020 LogicalClocks.
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

package org.apache.hadoop.hdfs.server.datanode;

import com.amazonaws.services.s3.model.PartETag;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class ProvidedReplicaBeingWritten extends ReplicaBeingWritten {

  private boolean isMultipart = false;
  private boolean isMultipartComplete = false;
  private int currentPart = 0;
  private final long partSize;
  List<PartETag> partETags = new ArrayList<PartETag>();
  List<Future> uploadTasks = new ArrayList<Future>();
  private String uploadID = null;

  public ProvidedReplicaBeingWritten(ReplicaBeingWritten from, long partSize) {
    super(from);
    this.partSize = partSize;
  }

  public boolean isPartAvailable() {
    if (getBytesOnDisk() >= ((currentPart + 1) * partSize)) {
      return true;
    }
    return false;
  }

  public int incrementAndGetNextPart() {
    return ++currentPart;
  }

  public int getCurrentPart() {
    return currentPart;
  }

  public long getPartSize() {
    return partSize;
  }

  public String getUploadID() {
    return uploadID;
  }

  public void setUploadID(String uploadID) {
    this.uploadID = uploadID;
  }

  public void addEtag(PartETag etag){
    partETags.add(etag);
  }

  public List<PartETag> getPartETags(){
    return partETags;
  }

  public boolean isMultipart() {
    return isMultipart;
  }

  public void setMultipart(boolean multipart) {
    isMultipart = multipart;
  }

  public boolean isMultipartComplete() {
    return isMultipartComplete;
  }

  public void setMultipartComplete(boolean multipartComplete) {
    isMultipartComplete = multipartComplete;
  }

  public List<Future> getAllUploadTasks(){
    return uploadTasks;
  }

  public void addUploadTask(Future future){
    uploadTasks.add(future);
  }
}

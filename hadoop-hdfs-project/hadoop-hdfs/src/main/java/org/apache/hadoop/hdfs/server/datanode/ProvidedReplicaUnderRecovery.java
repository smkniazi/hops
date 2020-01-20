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

import org.apache.hadoop.hdfs.protocol.Block;

public class ProvidedReplicaUnderRecovery extends ReplicaUnderRecovery {

  private final boolean partiallyUploaded;
  private final String uploadID;
  private final Block block;

  public ProvidedReplicaUnderRecovery(ReplicaInfo replica, long recoveryId
          , boolean partiallyUploaded, String uploadID, Block block) {
    super(replica, recoveryId);
    this.partiallyUploaded = partiallyUploaded;
    this.uploadID = uploadID;
    this.block = block;
  }

  public boolean isPartiallyUploaded() {
    return partiallyUploaded;
  }

  public String getUploadID() {
    return uploadID;
  }

  public Block getBlock(){
    return block;
  }
}

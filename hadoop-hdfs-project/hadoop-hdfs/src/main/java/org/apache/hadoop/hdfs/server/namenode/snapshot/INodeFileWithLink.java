/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import java.io.IOException;

/**
 * INodeFile with a link to the next element.
 * This class is used to represent the original file that is snapshotted.
 * The snapshot files are represented by {@link INodeFileSnapshot}.
 * The link of all the snapshot files and the original file form a circular
 * linked list so that all elements are accessible by any of the elements.
 */
@InterfaceAudience.Private
public class INodeFileWithLink extends INodeFile {
  private INodeFileWithLink next;

  public INodeFileWithLink(INodeFile f) throws IOException {
    super(f);
    next = this;
  }

  void setNext(INodeFileWithLink next) {
    this.next = next;
  }

  public INodeFileWithLink getNext() {
    return next;
  }

  /** Insert inode to the circular linked list. */
  public void insert(INodeFileWithLink inode) {
    inode.setNext(this.getNext());
    this.setNext(inode);
  }

  /**
   * @return the max file replication of the elements
   *         in the circular linked list.
   */
  public short getBlockReplication() {
    short max = getFileReplication();
    for(INodeFileWithLink i = next; i != this; i = i.getNext()) {
      final short replication = i.getFileReplication();
      if (replication > max) {
        max = replication;
      }
    }
    return max;
  }
}

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.util.Time;

import java.io.IOException;

/** Directories where taking snapshots is allowed. */
@InterfaceAudience.Private
public class INodeDirectorySnapshottable extends INodeDirectory {
  static public INodeDirectorySnapshottable newInstance(final INodeDirectory dir) throws IOException {
    return new INodeDirectorySnapshottable(dir);
  }

  private INodeDirectorySnapshottable(INodeDirectory dir) throws IOException {
    super(dir,true);
  }

  /** Cast INode to INodeDirectorySnapshottable. */
  static public INodeDirectorySnapshottable valueOf(
      INode inode, String src) throws IOException {
    final INodeDirectory dir = INodeDirectory.valueOf(inode, src);
    if (!dir.isSnapshottable()) {
      throw new SnapshotException(src + " is not a snapshottable directory.");
    }
    return (INodeDirectorySnapshottable)dir;
  }

  /** A list of snapshots of this directory. */
  private final List<INodeDirectorySnapshotRoot> snapshots
      = new ArrayList<INodeDirectorySnapshotRoot>();

  @Override
  public boolean isSnapshottable() {
    return true;
  }

  /** Add a snapshot root under this directory. */
  INodeDirectorySnapshotRoot addSnapshotRoot(final String name) throws IOException {
    final INodeDirectorySnapshotRoot r = new INodeDirectorySnapshotRoot(name, this);
    snapshots.add(r);

    //set modification time
    final long timestamp = Time.now();
    r.setModificationTime(timestamp);
    setModificationTime(timestamp);
    return r;
  }
}

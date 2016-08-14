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
package org.apache.hadoop.hdfs.server.namenode;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.MetadataLogDataAccess;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * I-node for closed file.
 */
@InterfaceAudience.Private
public class INodeFile extends INode implements BlockCollection {
  /**
   * Cast INode to INodeFile.
   */
  public static INodeFile valueOf(INode inode, String path) throws IOException {
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + path);
    }
    if (!(inode instanceof INodeFile)) {
      throw new FileNotFoundException("Path is not a file: " + path);
    }
    return (INodeFile) inode;
  }

  //Number of bits for Block size
  final static short BLOCK_BITS = 48;
  final static short REPLICATION_BITS = 8;
  final static short BOOLEAN_BITS = 8;
  final static short HAS_BLKS_BITS = 1; // this is out of the 8 bits for the storing booleans
  //Header mask 64-bit representation
  //Format:[8 bits for flags][8 bits for replication degree][48 bits for PreferredBlockSize]
  final static long BLOCK_SIZE_MASK =  0x0000FFFFFFFFFFFFL;
  final static long REPLICATION_MASK = 0x00FF000000000000L;
  final static long FLAGS_MASK =       0xFF00000000000000L;
  final static long HAS_BLKS_MASK =    0x0100000000000000L;
  //[8 bits for flags]
  //0 bit : 1 if the file has blocks. 0 blocks
  //remaining bits are not yet used
  long header;

  private int generationStamp = (int) GenerationStamp.FIRST_VALID_STAMP;
  private long size;
  

  public INodeFile(PermissionStatus permissions, BlockInfo[] blklist,
      short replication, long modificationTime, long atime,
      long preferredBlockSize) throws IOException {
    super(permissions, modificationTime, atime);
    this.setReplicationNoPersistance(replication);
    this.setPreferredBlockSizeNoPersistance(preferredBlockSize);
  }

  public INodeFile(PermissionStatus permissions, long header,
      long modificationTime, long atime) throws IOException {
    super(permissions, modificationTime, atime);
    this.setHeader(header);
  }

  //HOP:
  public INodeFile(INodeFile other)
      throws IOException {
    super(other);
    setReplicationNoPersistance(other.getBlockReplication());
    setPreferredBlockSizeNoPersistance(other.getPreferredBlockSize());
    setGenerationStampNoPersistence(other.getGenerationStamp());
    setSizeNoPersistence(other.getSize());
    setHasBlocksNoPersistance(other.hasBlocks());
  }

  /**
   * @return the replication factor of the file.
   */
  @Override
  public short getBlockReplication() {
    return getBlockReplication(header);
  }

  public static short getBlockReplication(long header) {
    long val = (header & REPLICATION_MASK);
    long val2 = val >> BLOCK_BITS;
    return (short) val2;
  }

  void setReplicationNoPersistance(short replication) {
    if (replication <= 0 || replication > (Math.pow(2, REPLICATION_BITS) - 1)) {
      throw new IllegalArgumentException("Unexpected value for the " +
          "replication [" + replication + "]. Expected [1:" + (Math.pow(2, REPLICATION_BITS) - 1) + "]");
    }
    header = ((long) replication << BLOCK_BITS) | (header & ~REPLICATION_MASK);
  }

  /**
   * @return preferred block size (in bytes) of the file.
   */
  @Override
  public long getPreferredBlockSize() {
    return getPreferredBlockSize(header);
  }

  public static long getPreferredBlockSize(long header) {
    return header & BLOCK_SIZE_MASK;
  }

  private void setPreferredBlockSizeNoPersistance(long preferredBlkSize) {
    if ((preferredBlkSize < 0) || (preferredBlkSize > (Math.pow(2, BLOCK_BITS) - 1))) {
      throw new IllegalArgumentException("Unexpected value for the block " +
          "size [" + preferredBlkSize + "]. Expected [1:" + (Math.pow(2, BLOCK_BITS) - 1) + "]");
    }
    header = (header & ~BLOCK_SIZE_MASK) | (preferredBlkSize & BLOCK_SIZE_MASK);
  }

  public void setHeader(long header) {
    if (header <= 0) {
      throw new IllegalArgumentException("Unexpected value for the " +
          "header [" + header + "]");
    }
    long preferecBlkSize = getPreferredBlockSize(header);
    short replication = getBlockReplication(header);
    if (preferecBlkSize < 0) {
      throw new IllegalArgumentException("Unexpected value for the " +
          "block size [" + preferecBlkSize + "]");
    }

    if (replication < 0) {
      throw new IllegalArgumentException("Unexpected value for the " +
          "replication [" + replication + "]");
    }


    this.header = header;
  }

  public void setHasBlocks(boolean hasBlocks) throws TransactionContextException, StorageException {
    setHasBlocksNoPersistance(hasBlocks);
    save();
  }

  public void setHasBlocksNoPersistance(boolean hasBlocks) {
    long val = (hasBlocks) ? 1 : 0;
    header = ((long) val << (BLOCK_BITS + REPLICATION_BITS)) | (header & ~HAS_BLKS_MASK);
  }

  public boolean hasBlocks() {
    long val = (header & HAS_BLKS_MASK);
    long val2 = val >> (BLOCK_BITS + REPLICATION_BITS);
    if (val2 == 1) {
      return true;
    } else if (val2 == 0) {
      return false;
    } else {
      throw new IllegalStateException("Flags in the inode header are messed up");
    }
  }

  public static boolean hasBlocks(long header){
   return hasBlocks(header);
  }
  /**
   * @return the blocks of the file.
   */
  @Override
  public BlockInfo[] getBlocks()
      throws StorageException, TransactionContextException {
    if (getId() == INode.NON_EXISTING_ID) {
      return BlockInfo.EMPTY_ARRAY;
    }
    List<BlockInfo> blocks = (List<BlockInfo>) EntityManager
        .findList(BlockInfo.Finder.ByINodeId, id);
    if (blocks != null) {
      Collections.sort(blocks, BlockInfo.Order.ByBlockIndex);
      BlockInfo[] blks = new BlockInfo[blocks.size()];
      return blocks.toArray(blks);
    } else {
      return BlockInfo.EMPTY_ARRAY;
    }
  }

  /**
   * append array of blocks to this.blocks
   */
  List<BlockInfo> appendBlocks(INodeFile[] inodes, int totalAddedBlocks /*HOP not used*/)
      throws StorageException, TransactionContextException {
    List<BlockInfo> oldBlks = new ArrayList<BlockInfo>();
    for (INodeFile srcInode : inodes) {
      for (BlockInfo block : srcInode.getBlocks()) {
        BlockInfo copy = BlockInfo.cloneBlock(block);
        oldBlks.add(copy);
        addBlock(block);
        block.setBlockCollection(this);
      }
    }
    return oldBlks;
  }
  
  /**
   * add a block to the block list
   */
  void addBlock(BlockInfo newblock)
      throws StorageException, TransactionContextException {
    BlockInfo maxBlk = findMaxBlk();
    newblock.setBlockIndex(maxBlk.getBlockIndex() + 1);
  }

  /**
   * Set the block of the file at the given index.
   */
  public void setBlock(int idx, BlockInfo blk)
      throws StorageException, TransactionContextException {
    blk.setBlockIndex(idx);
  }

  @Override
  int collectSubtreeBlocksAndClear(List<Block> v)
      throws StorageException, TransactionContextException {
    parent = null;
    BlockInfo[] blocks = getBlocks();
    if (blocks != null && v != null) {
      for (BlockInfo blk : blocks) {
        blk.setBlockCollection(null);
        v.add(blk);
      }
    }
    return 1;
  }
  
  @Override
  public String getName() throws StorageException, TransactionContextException {
    // Get the full path name of this inode.
    return getFullPathName();
  }


  @Override
  long[] computeContentSummary(long[] summary)
      throws StorageException, TransactionContextException {
    summary[0] += computeFileSize(true);
    summary[1]++;
    summary[3] += diskspaceConsumed();
    return summary;
  }

  /**
   * Compute file size.
   * May or may not include BlockInfoUnderConstruction.
   */
  public long computeFileSize(boolean includesBlockInfoUnderConstruction)
      throws StorageException, TransactionContextException {
    return computeFileSize(includesBlockInfoUnderConstruction, getBlocks());
  }

  static long computeFileSize(boolean includesBlockInfoUnderConstruction,
      BlockInfo[] blocks) throws StorageException {
    if (blocks == null || blocks.length == 0) {
      return 0;
    }
    final int last = blocks.length - 1;
    //check if the last block is BlockInfoUnderConstruction
    long bytes = 0;
    
    if(blocks[last] instanceof BlockInfoUnderConstruction){
        if(includesBlockInfoUnderConstruction){
            bytes = blocks[last].getNumBytes();
        }
    }else{
        bytes = blocks[last].getNumBytes();
    }
    for (int i = 0; i < last; i++) {
      bytes += blocks[i].getNumBytes();
    }
    return bytes;
  }

  @Override
  DirCounts spaceConsumedInTree(DirCounts counts)
      throws StorageException, TransactionContextException {
    counts.nsCount += 1;
    counts.dsCount += diskspaceConsumed();
    return counts;
  }

  long diskspaceConsumed()
      throws StorageException, TransactionContextException {
    return diskspaceConsumed(getBlocks());
  }
  
  long diskspaceConsumed(Block[] blkArr) {
    return diskspaceConsumed(blkArr, isUnderConstruction(),
        getPreferredBlockSize(), getBlockReplication());
  }

  static long diskspaceConsumed(Block[] blkArr, boolean underConstruction,
      long preferredBlockSize, short blockReplication) {
    long size = 0;
    if (blkArr == null) {
      return 0;
    }

    for (Block blk : blkArr) {
      if (blk != null) {
        size += blk.getNumBytes();
      }
    }
    /* If the last block is being written to, use prefferedBlockSize
     * rather than the actual block size.
     */
    if (blkArr.length > 0 && blkArr[blkArr.length - 1] != null &&
        underConstruction) {
      size += preferredBlockSize - blkArr[blkArr.length - 1].getNumBytes();
    }
    return size * blockReplication;
  }
  
  /**
   * Return the penultimate allocated block for this file.
   */
  BlockInfo getPenultimateBlock()
      throws StorageException, TransactionContextException {
    BlockInfo[] blocks = getBlocks();
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
    return blocks[blocks.length - 2];
  }

  @Override
  public BlockInfo getLastBlock() throws IOException, StorageException {
    BlockInfo[] blocks = getBlocks();
    return blocks == null || blocks.length == 0 ? null :
        blocks[blocks.length - 1];
  }

  @Override
  public int numBlocks() throws StorageException, TransactionContextException {
    BlockInfo[] blocks = getBlocks();
    return blocks == null ? 0 : blocks.length;
  }
  

  public long getHeader() {
    return header;
  }

  void setReplication(short replication)
      throws StorageException, TransactionContextException {
    setReplicationNoPersistance(replication);
    save();
  }

  public INodeFileUnderConstruction convertToUnderConstruction(
      String clientName, String clientMachine, DatanodeID clientNode)
      throws IOException {
    INodeFileUnderConstruction ucfile =
        new INodeFileUnderConstruction(this, clientName, clientMachine,
            clientNode);
    save(ucfile);
    return ucfile;
  }
  
  public BlockInfo findMaxBlk()
      throws StorageException, TransactionContextException {
    BlockInfo maxBlk = (BlockInfo) EntityManager
        .find(BlockInfo.Finder.ByMaxBlockIndexForINode, this.getId());
    return maxBlk;
  }
  
  public int getGenerationStamp() {
    return generationStamp;
  }

  public void setGenerationStampNoPersistence(int generationStamp) {
    this.generationStamp = generationStamp;
  }
  
  public int nextGenerationStamp()
      throws StorageException, TransactionContextException {
    generationStamp++;
    save();
    return generationStamp;
  }

  public long getSize() {
    return size;
  }

  public void setSizeNoPersistence(long size) {
    this.size = size;
  }
  
  public void recomputeFileSize() throws StorageException, TransactionContextException {
    setSizeNoPersistence(this.computeFileSize(true));
    save();
  }
}

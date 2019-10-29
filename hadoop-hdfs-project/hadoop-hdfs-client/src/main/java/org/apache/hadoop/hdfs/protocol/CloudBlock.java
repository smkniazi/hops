package org.apache.hadoop.hdfs.protocol;

import com.google.common.base.Preconditions;

public class CloudBlock {

  private boolean metaObjectFound;
  private boolean blockObjectFound;
  private long lastModified;
  private Block block;

  public CloudBlock(){
    this.block = null;
    this.metaObjectFound = false;
    this.blockObjectFound = false;
    this.lastModified = -1;
  }

  public CloudBlock(Block block, long lastModified){
    Preconditions.checkNotNull(block);
    this.metaObjectFound = true;
    this.blockObjectFound = true;
    this.block = block;
    this.lastModified = lastModified;
  }

  public void setLastModified(long lastModified) {
    this.lastModified = lastModified;
  }

  public long getLastModified() {
    return lastModified;
  }

  public void setMetaObjectFound(boolean metaObjectFound) {
    this.metaObjectFound = metaObjectFound;
  }

  public void setBlockObjectFound(boolean blockObjectFound) {
    this.blockObjectFound = blockObjectFound;
  }

  public void setBlock(Block block) {
    this.block = block;
  }

  public Block getBlock() {
    return block;
  }

  public boolean isPartiallyListed() {
    return !metaObjectFound || !blockObjectFound;
  }
}

/*
 * Copyright (C) 2019 Logical Clocks AB.
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

package org.apache.hadoop.hdfs.server.common;

import com.google.common.collect.Lists;
import io.hops.exception.StorageException;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.CloudBucketDataAccess;
import io.hops.metadata.hdfs.entity.CloudBucket;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.CloudBucketAlreadyExistsException;
import org.apache.hadoop.hdfs.server.blockmanagement.CloudBucketNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.omg.PortableInterceptor.NON_EXISTENT;

public class CloudHelper {

  public static final Log LOG = LogFactory.getLog(CloudHelper.class);
  public final static String BLOCKFILE_EXTENSION = ".data";
  public final static String PREFIX_STR = "hopsfs-blocks-set-";

  public static final Pattern blockFilePattern = Pattern.compile(
          PREFIX_STR + "(-??\\d++)" + "/" + Block.BLOCK_FILE_PREFIX +
                  "(-??\\d++)_(\\d++)\\" + BLOCKFILE_EXTENSION + "$");
  public static final Pattern metaFilePattern = Pattern.compile(
          PREFIX_STR + "(-??\\d++)" + "/" + Block.BLOCK_FILE_PREFIX +
                  "(-??\\d++)_(\\d++)\\" + Block.METADATA_EXTENSION + "$");

  //Block key prefix/blk_id_gs.data
  public static String getBlockKey(final int prefixSize, Block b) {
    return getPrefix(prefixSize, b.getBlockId()) + b.getBlockName() + "_" + b.getGenerationStamp()
            + BLOCKFILE_EXTENSION;
  }

  //Meta key prefix/blk_id_gs.meta
  public static String getMetaFileKey(final int prefixSize, Block b) {
    String metaFileID = DatanodeUtil.getMetaName(b.getBlockName(), b.getGenerationStamp());
    return getPrefix(prefixSize, b.getBlockId()) + metaFileID;
  }

  public static String getPrefix(final int prefixSize, long blockID) {
    long prefixNo = blockID / prefixSize;
    return PREFIX_STR + prefixNo + "/";
  }

  public static boolean isBlockFilename(String name) {
    return blockFilePattern.matcher(name).matches();
  }

  public static boolean isMetaFilename(String name) {
    return metaFilePattern.matcher(name).matches();
  }

  public static long extractBlockIDFromBlockName(String name) {
    List<Long> numbers = extractNumbers(name);
    return numbers.get(numbers.size() - 2);
  }

  public static long extractGSFromBlockName(String name) {
    List<Long> numbers = extractNumbers(name);
    return numbers.get(numbers.size() - 1);
  }

  public static long extractBlockIDFromMetaName(String name) {
    List<Long> numbers = extractNumbers(name);
    return numbers.get(numbers.size() - 2);
  }

  public static long extractGSFromMetaName(String name) {
    List<Long> numbers = extractNumbers(name);
    return numbers.get(numbers.size() - 1);
  }

  public static List<Long> extractNumbers(String key) {
    String str = key.replaceAll("[^?0-9]+", " ");
    String[] numbersStr = str.trim().split(" ");
    List<Long> numbers = new ArrayList();
    for (String num : numbersStr) {
      numbers.add(Long.parseLong(num));
    }
    return numbers;
  }

  private static Map<String, CloudBucket> BUCKETS = null;

  /**
    * Load the buckets from the database the first time it runs, then use the local bucket map.
    * This is ok because buckets in the database are only modified during the format phase.
    */
  public static Map<String, CloudBucket> getAllBuckets() throws StorageException {
    if (BUCKETS != null && BUCKETS.size() != 0) {
      return BUCKETS;
    } else {
      BUCKETS = getAllBucketsFromDB();
      return BUCKETS;
    }
  }

  private static Map<String, CloudBucket> getAllBucketsFromDB()
          throws StorageException {
    try {
      LightWeightRequestHandler h =
              new LightWeightRequestHandler(HDFSOperationType.GET_ALL_CLOUD_BUCKETS) {
                @Override
                public Object performTask() throws IOException {
                  CloudBucketDataAccess da = (CloudBucketDataAccess)
                          HdfsStorageFactory.getDataAccess(CloudBucketDataAccess.class);
                  return da.getAll();
                }
              };
      return (Map<String, CloudBucket>) h.handle();
    } catch (IOException e) {
      LOG.error(e, e);
      throw new StorageException(e);
    }
  }

  /**
    * This function should only be called during the format phase. Calling it while namenodes are running won't work
    * because namenodes are caching the list of buckets.
    */

  public static int addBucket(final String name) throws StorageException {
    try {
      if (getAllBuckets().keySet().contains(name)) {
        throw new CloudBucketAlreadyExistsException("Bucket with name: " + name + " already added");
      }

      LightWeightRequestHandler h =
              new LightWeightRequestHandler(HDFSOperationType.ADD_CLOUD_BUCKET) {
                @Override
                public Object performTask() throws IOException {
                  CloudBucketDataAccess da = (CloudBucketDataAccess)
                          HdfsStorageFactory.getDataAccess(CloudBucketDataAccess.class);
                  return da.addBucket(name);
                }
              };
      return (int) h.handle();
    } catch (IOException e) {
      LOG.error(e, e);
      throw new StorageException(e);
    }
  }

  public static String getCloudBucketName(final short ID) throws StorageException,
          CloudBucketNotFoundException {
    if(ID == CloudBucket.NON_EXISTENT_BUCKET_ID){
      return CloudBucket.NON_EXISTENT_BUCKET_NAME;
    }

    for (CloudBucket bucket : getAllBuckets().values()) {
      if (bucket.getID() == ID) {
        return bucket.getName();
      }
    }
    throw new CloudBucketNotFoundException("Bucket with ID: " + ID + " is not found");
  }

  public static short getCloudBucketID(final String name) throws StorageException {
    if(name.compareToIgnoreCase(CloudBucket.NON_EXISTENT_BUCKET_NAME) == 0){
      return CloudBucket.NON_EXISTENT_BUCKET_ID;
    }

    Map<String, CloudBucket> buckets = getAllBuckets();
    if (!buckets.keySet().contains(name)) {
      throw new CloudBucketNotFoundException("Bucket with name: " + name + " is not added");
    } else {
      return buckets.get(name).getID();
    }
  }

  public static List<String> getBucketsFromConf(Configuration conf) {
    String bucket = conf.get(DFSConfigKeys.S3_BUCKET_KEY, DFSConfigKeys.S3_BUCKET_DEFAULT);
    List<String> buckets = new ArrayList();
    buckets.add(bucket);
    return buckets;
  }

  static Random rand = new Random(System.currentTimeMillis());

  public static String getRandomCloudBucket() throws StorageException {
    //for now select one bucket randomly
    Map<String, CloudBucket> buckets = getAllBuckets();
    if (buckets == null || buckets.isEmpty()) {
      throw new CloudBucketNotFoundException("No cloud buckets found");
    }

    return Lists.newArrayList(buckets.keySet()).get(rand.nextInt(buckets.size()));
  }

  public static void clearCache(){
    if(BUCKETS!=null) {
      BUCKETS.clear();
    }
  }
}

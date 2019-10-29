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

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class CloudHelper {

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

  public static long extractBlockIDFromBlockName(String name){
    List<Long> numbers = extractNumbers(name);
    return numbers.get(numbers.size()-2);
  }

  public static long extractGSFromBlockName(String name){
    List<Long> numbers = extractNumbers(name);
    return numbers.get(numbers.size()-1);
  }

  public static long extractBlockIDFromMetaName(String name){
    List<Long> numbers = extractNumbers(name);
    return numbers.get(numbers.size()-2);
  }

  public static long extractGSFromMetaName(String name){
    List<Long> numbers = extractNumbers(name);
    return numbers.get(numbers.size()-1);
  }
  public static short extractBucketID(String name){
    List<Long> bucketNums = CloudHelper.extractNumbers(name);
    long bucketID = bucketNums.get(bucketNums.size() - 1);
    assert bucketID <= Short.MAX_VALUE;
    return (short)bucketID;
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
}

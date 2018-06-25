
/*
 * Copyright (C) 2015 hops.io.
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
package org.apache.hadoop.hdfs;

import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.SubTreeOperation;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestNNRestartNPE extends TestCase {

  static boolean fail=false;
  static String failMessage="";

  @Before
  public void init(){
   fail = false;
   failMessage = "";
  }



  @Test
  public void testNNRestartNPE() throws IOException, InterruptedException {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(1).build();
      cluster.waitActive();


      DistributedFileSystem dfs = cluster.getFileSystem();

      dfs.mkdir(new Path("/A"), FsPermission.getDefault());
      TestFileCreation.create(dfs, new Path("/A/File1"),1 );
      TestFileCreation.create(dfs, new Path("/A/File2"),1 );

      deleteINode(conf, "File2");
      cluster.setLeasePeriod(1000,1000);
//      cluster.restartNameNode();
      Thread.sleep(5000);

    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public static void deleteINode(final Configuration conf, final String inodeName) throws IOException {
    LightWeightRequestHandler subTreeLockChecker =
            new LightWeightRequestHandler(HDFSOperationType.TEST) {
              @Override
              public Object performTask() throws IOException {
                String query = "delete from "+TablesDef.INodeTableDef.TABLE_NAME+" where "+
                        TablesDef.INodeTableDef.NAME +" = \""+inodeName+"\"";
                MySQLQueryHelper.execute(query);
                LOG.debug("Testing STO Deleted inode "+inodeName);
                return null;
              }
            };
    subTreeLockChecker.handle();
  }
}

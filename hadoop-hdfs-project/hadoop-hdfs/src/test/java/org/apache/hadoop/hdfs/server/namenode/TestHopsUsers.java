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
package org.apache.hadoop.hdfs.server.namenode;

import io.hops.common.INodeUtil;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import javax.swing.*;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class TestHopsUsers extends TestCase {
  static final Log LOG = LogFactory.getLog(TestHopsUsers.class);
  boolean fail = false;

  @Test
  public void testMultiUserMultiGrp() throws Exception {
    Configuration conf = new HdfsConfiguration();

    String userName = UserGroupInformation.getCurrentUser().getShortUserName();
    conf.set(String.format("hadoop.proxyuser.%s.hosts", userName), "*");
    conf.set(String.format("hadoop.proxyuser.%s.users", userName), "*");
    conf.set(String.format("hadoop.proxyuser.%s.groups", userName), "*");
//    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);

    final MiniDFSCluster cluster =
            new MiniDFSCluster.Builder(conf).format(true).build();
    cluster.waitActive();

    try {

      DistributedFileSystem superFS = cluster.getFileSystem();

      int numUsers = 2; //>=1
      DistributedFileSystem []fss  = new DistributedFileSystem[numUsers];
      Path[] files = new Path[numUsers];

      for(int i = 0; i < numUsers; i++){
        superFS.addUser("user"+i);
//        superFS.addGroup("group"+i);
      }

      if(true) return;

      //add all users to all groups
      for(int i = 0; i < numUsers; i++) {
        for(int j = 0; j < numUsers; j++) {
          superFS.addUserToGroup("user"+i, "group"+j);
        }
      }

      // create file system objects
      for(int i = 0; i < numUsers; i++){
        UserGroupInformation ugi =
                UserGroupInformation.createUserForTesting("user"+i, new String[]{"group"+i});

        DistributedFileSystem fs = (DistributedFileSystem) ugi
                .doAs(new PrivilegedExceptionAction<FileSystem>() {
                  @Override
                  public FileSystem run() throws Exception {
                    return cluster.getFileSystem();
                  }
                });
        fss[i] = fs;
      }

      Path path = new Path("/Projects");
      superFS.mkdirs(path);
      superFS.setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

      path = new Path("/Projects/dataset");
      fss[0].mkdirs(path);
      fss[0].setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
      fss[0].setOwner(path, "user0", "group0");


      for(int i = 0; i < numUsers; i++){
        path = new Path("/Projects/dataset/user"+i);
        fss[i].mkdirs(path);
        fss[i].setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
        fss[i].setOwner(path, "user"+i, "group"+i);
        path = new Path("/Projects/dataset/user"+i+"/file"+i);
        fss[i].create(path).close();
        fss[i].setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
        fss[i].setOwner(path, "user"+i, "group"+i);
        files[i] = path;
      }


      Thread[] threads = new Thread[numUsers];
      for (int i = 0; i < threads.length; i++) {
        threads[i] = new Thread(new Worker(fss[i], files));
        threads[i].start();
      }

      for (int i = 0; i < threads.length; i++) {
        threads[i].join();
      }


      if(fail){
        fail();
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      cluster.shutdown();
    }
  }

  Random rand = new Random(System.currentTimeMillis());
  class Worker implements Runnable {

    DistributedFileSystem fs;
    Path[] paths;
    Worker(DistributedFileSystem fs, Path[] paths) {
      this.fs = fs;
      this.paths = paths;
    }

    public void run() {

      try {

        for(int i = 0; i < 1000; i++){
          FileStatus status = fs.getFileStatus(paths[rand.nextInt(paths.length)]);
          Thread.sleep(50);
          if (status == null) {
            fail = true;
            return;
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}



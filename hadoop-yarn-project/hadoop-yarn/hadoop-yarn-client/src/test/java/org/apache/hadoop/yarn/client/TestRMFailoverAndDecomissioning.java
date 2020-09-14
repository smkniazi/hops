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

package org.apache.hadoop.yarn.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.cli.RMAdminCLI;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.AdminService;
import org.apache.hadoop.yarn.server.resourcemanager.HATestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRMFailoverAndDecomissioning extends ClientBaseWithFixes {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRMFailoverAndDecomissioning.class.getName());
  private static final HAServiceProtocol.StateChangeRequestInfo req =
      new HAServiceProtocol.StateChangeRequestInfo(
          HAServiceProtocol.RequestSource.REQUEST_BY_USER);

  private static final String RM1_NODE_ID = "rm1";
  private static final int RM1_PORT_BASE = 10000;
  private static final String RM2_NODE_ID = "rm2";
  private static final int RM2_PORT_BASE = 20000;

  private Configuration conf;
  private MiniYARNCluster cluster;

  File hostFile;
  File confFile;


  @Before
  public void setup() throws IOException {
    conf = new YarnConfiguration();

    Path workingPath = new Path(new File("target").getAbsolutePath());
    conf.set(YarnConfiguration.FS_BASED_RM_CONF_STORE, workingPath.toString());
    conf.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
            "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");
    hostFile = new File(workingPath + File.separator + "hostFile.txt");
    confFile = new File(workingPath + File.separator + YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);

    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, true);
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    conf.setClass(
            CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
            DominantResourceCalculator.class, ResourceCalculator.class);
    HATestUtil.setRpcAddressForRM(RM1_NODE_ID, RM1_PORT_BASE, conf);
    HATestUtil.setRpcAddressForRM(RM2_NODE_ID, RM2_PORT_BASE, conf);

    conf.setLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS, 100L);

    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
    conf.setBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC, true);

    cluster = new MiniYARNCluster(TestRMFailoverAndDecomissioning.class.getName(), 2, 1, 1, 1);
  }

  @After
  public void teardown() {
    cluster.stop();
  }

  private void verifyClientConnection() {
    int numRetries = 3;
    while(numRetries-- > 0) {
      Configuration conf = new YarnConfiguration(this.conf);
      YarnClient client = YarnClient.createYarnClient();
      client.init(conf);
      client.start();
      try {
        client.getApplications();
        return;
      } catch (Exception e) {
        LOG.error(e.toString());
      } finally {
        client.stop();
      }
    }
    fail("Client couldn't connect to the Active RM");
  }

  private void verifyConnections() throws InterruptedException, YarnException {
    assertTrue("NMs failed to connect to the RM",
        cluster.waitForNodeManagersToConnect(20000));
    verifyClientConnection();
  }

  private AdminService getAdminService(int index) {
    return cluster.getResourceManager(index).getRMContext().getRMAdminService();
  }

  private void explicitFailover() throws IOException {
    int activeRMIndex = cluster.getActiveRMIndex();
    int newActiveRMIndex = (activeRMIndex + 1) % 2;
    getAdminService(activeRMIndex).transitionToStandby(req);
    getAdminService(newActiveRMIndex).transitionToActive(req);
    assertEquals("Failover failed", newActiveRMIndex, cluster.getActiveRMIndex());
  }

  @Test
  public void testExplicitFailover()
          throws Exception {
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    conf.set(YarnConfiguration.RM_NODES_EXCLUDE_FILE_PATH,
            hostFile.getAbsolutePath());
    writeConfigurationXML(conf,confFile);

    if(hostFile.exists()){
      hostFile.delete();
    }
    hostFile.createNewFile();

    cluster.init(conf);
    cluster.start();


    assertFalse("RM never turned active", -1 == cluster.getActiveRMIndex());
    verifyConnections();

    GetClusterMetricsResponse resp =
            cluster.getResourceManager(0).getClientRMService().getClusterMetrics(GetClusterMetricsRequest.newInstance());
    assert resp.getClusterMetrics().getNumNodeManagers()==1;

    int result = ToolRunner.run(new RMAdminCLI(conf), new String[]{
            "-updateExcludeList",
            cluster.getNodeManager(0).getNMContext().getNodeId().getHost()});
    assert result == 0;


    result = ToolRunner.run(new RMAdminCLI(conf), new String[]{
            "-refreshNodes"});
    assert result == 0;
    Thread.sleep(10000);
    assertTrue(cluster.getNodeManager(0).getNMContext().getDecommissioned());
    assertEquals(0,
            cluster.getResourceManager(cluster.getActiveRMIndex()).getRMContext().
                    getClientRMService().getClusterMetrics(GetClusterMetricsRequest.newInstance()).
                    getClusterMetrics().getNumNodeManagers());

    explicitFailover();

    assertEquals(0,
            cluster.getResourceManager(cluster.getActiveRMIndex()).getRMContext().
                    getClientRMService().getClusterMetrics(GetClusterMetricsRequest.newInstance()).
                    getClusterMetrics().getNumNodeManagers());
  }

  private String writeConfigurationXML(Configuration conf, File confFile)
          throws IOException {
    DataOutputStream output = null;
    try {
      if (confFile.exists()) {
        confFile.delete();
      }
      if (!confFile.createNewFile()) {
        Assert.fail("Can not create " + confFile);
      }
      output = new DataOutputStream(
              new FileOutputStream(confFile));
      conf.writeXml(output);
      return confFile.getAbsolutePath();
    } finally {
      if (output != null) {
        output.close();
      }
    }
  }
}

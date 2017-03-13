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
package org.apache.hadoop.yarn.client;

import io.hops.leader_election.node.ActiveNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConfiguredLeaderFailoverHAProxyProvider<T>
    extends ConfiguredRMFailoverHAProxyProvider<Object> {
  private static final Log LOG = LogFactory.getLog(
          ConfiguredLeaderFailoverHAProxyProvider.class);

  protected ActiveNode getActiveNode() {
    ActiveNode node = groupMembership.getLeader();
    if (node != null) {
      LOG.info("geting leader : " + node.getHostname());
    } else {
      LOG.info("geting leader : " + null);
    }
    return node;
  }
}

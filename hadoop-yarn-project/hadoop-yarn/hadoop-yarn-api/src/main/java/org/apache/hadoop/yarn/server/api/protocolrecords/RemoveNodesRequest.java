/*
 * Copyright 2020 Apache Software Foundation.
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
package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.util.Records;

public abstract class RemoveNodesRequest {

  @InterfaceAudience.Private
  @InterfaceStability.Stable
  public static RemoveNodesRequest newInstance() {
    RemoveNodesRequest request = Records.newRecord(RemoveNodesRequest.class);
    return request;
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static RemoveNodesRequest newInstance(List<String> nodes) {
    RemoveNodesRequest request = Records.newRecord(RemoveNodesRequest.class);
    request.setNodes(nodes);
    return request;
  }

  /**
   * Set node
   *
   * @param nodes
   */
  public abstract void setNodes(List<String> nodes);

  /**
   * Get the decommissioned node
   *
   * @return node
   */
  public abstract List<String> getNodes();

}

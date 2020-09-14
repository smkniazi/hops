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

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateExcludeListRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateExcludeListRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateExcludeListRequest;

@Private
@Unstable
public class UpdateExcludeListRequestPBImpl extends UpdateExcludeListRequest {
  UpdateExcludeListRequestProto proto =
          UpdateExcludeListRequestProto.getDefaultInstance();
  UpdateExcludeListRequestProto.Builder builder = null;
  boolean viaProto = false;
  private String nodes;

  public UpdateExcludeListRequestPBImpl() {
    builder = UpdateExcludeListRequestProto.newBuilder();
  }

  public UpdateExcludeListRequestPBImpl(UpdateExcludeListRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized UpdateExcludeListRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.nodes != null) {
      builder.setNodes(this.nodes);
    }
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = UpdateExcludeListRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public synchronized void setNodes(String nodes) {
    maybeInitBuilder();
    this.nodes = nodes;
    mergeLocalToBuilder();
  }

  @Override
  public synchronized String getNodes() {
    UpdateExcludeListRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getNodes();
  }
}

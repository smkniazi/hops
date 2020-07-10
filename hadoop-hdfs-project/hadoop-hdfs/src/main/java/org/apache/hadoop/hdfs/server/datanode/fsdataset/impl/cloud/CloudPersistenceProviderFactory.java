/*
 * Copyright (C) 2020 Logical Clocks AB.
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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.cloud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CloudProvider;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import java.io.IOException;

public class CloudPersistenceProviderFactory {

  public static CloudPersistenceProvider getCloudClient(Configuration conf) throws IOException {
    String cloudProvider = conf.get(DFSConfigKeys.DFS_CLOUD_PROVIDER,
            DFSConfigKeys.DFS_CLOUD_PROVIDER_DEFAULT);
    if (cloudProvider.compareToIgnoreCase(CloudProvider.AWS.name()) == 0) {
      return new CloudPersistenceProviderS3Impl(conf);
    } else if (cloudProvider.compareToIgnoreCase(CloudProvider.AZURE.name()) == 0) {
      return new CloudPersistenceProviderAzureImpl(conf);
    } else {
      throw new UnsupportedOperationException("Cloud provider '" + cloudProvider +
              "' is not supported");
    }
  }
}

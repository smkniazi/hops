/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.util.resource;

import org.apache.hadoop.yarn.api.records.Resource;

public class DominantResourceCalculatorGPU extends DominantResourceCalculator {
  
  @Override
  public long computeAvailableContainers(Resource available, Resource required) {
    long vCoreMemoryContainers = Math.min(
        available.getMemorySize() / required.getMemorySize(),
        available.getVirtualCores() / required.getVirtualCores());
    //If request needs GPUs, we need to account for it too
    if(required.getGPUs() > 0) {
      long vCoreMemoryGPUContainers = Math.min(vCoreMemoryContainers,
          available.getGPUs() / required.getGPUs());
      return vCoreMemoryGPUContainers;
    } else {
      return vCoreMemoryContainers;
    }
  }
  
  @Override
  public Resource divideAndCeil(Resource numerator, int denominator) {
    return Resources.createResource(
        divideAndCeil(numerator.getMemorySize(), denominator),
        divideAndCeil(numerator.getVirtualCores(), denominator),
        numerator.getGPUs()
    );
  }
  
  @Override
  public Resource normalize(Resource r, Resource minimumResource,
      Resource maximumResource, Resource stepFactor) {
    long normalizedMemory = Math.min(
            roundUp(
            Math.max(r.getMemorySize(), minimumResource.getMemorySize()),
            stepFactor.getMemorySize()),
            maximumResource.getMemorySize());
    int normalizedCores = Math.min(
            roundUp(
            Math.max(r.getVirtualCores(), minimumResource.getVirtualCores()),
            stepFactor.getVirtualCores()),
            maximumResource.getVirtualCores());
    int normalizedGPUs = Math.min(
        roundUpWithZero(
            Math.max(r.getGPUs(), minimumResource.getGPUs()),
            stepFactor.getGPUs()),
        maximumResource.getGPUs());
    return Resources.createResource(normalizedMemory,
        normalizedCores, normalizedGPUs);
  }
  
  @Override
  public Resource roundUp(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundUp(r.getMemorySize(), stepFactor.getMemorySize()),
        roundUp(r.getVirtualCores(), stepFactor.getVirtualCores()),
        roundUpWithZero(r.getGPUs(), stepFactor.getGPUs())
    );
  }
  
  @Override
  public Resource roundDown(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundDown(r.getMemorySize(), stepFactor.getMemorySize()),
        roundDown(r.getVirtualCores(), stepFactor.getVirtualCores()),
        roundDownWithZero(r.getGPUs(), stepFactor.getGPUs())
    );
  }
  
  @Override
  public Resource multiplyAndNormalizeUp(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundUp(
            (int)Math.ceil(r.getMemorySize() * by), stepFactor.getMemorySize()),
        roundUp(
            (int)Math.ceil(r.getVirtualCores() * by),
            stepFactor.getVirtualCores()),
        roundUpWithZero(
            (int)Math.ceil(r.getGPUs() * by),
            stepFactor.getGPUs())
    );
  }
  
  @Override
  public Resource multiplyAndNormalizeDown(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundDown(
            (int)(r.getMemorySize() * by),
            stepFactor.getMemorySize()
        ),
        roundDown(
            (int)(r.getVirtualCores() * by),
            stepFactor.getVirtualCores()
        ),
        roundDownWithZero(
            (int)(r.getGPUs() * by),
            stepFactor.getGPUs()
        )
    );
  }

  @Override
  public boolean fitsIn(Resource cluster,
                        Resource smaller, Resource bigger) {
    return smaller.getMemorySize() <= bigger.getMemorySize()
            && smaller.getVirtualCores() <= bigger.getVirtualCores()
            && smaller.getGPUs() <= bigger.getGPUs();
  }
  
}
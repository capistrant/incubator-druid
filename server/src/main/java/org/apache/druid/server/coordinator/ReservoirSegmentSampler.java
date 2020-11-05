/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.coordinator;

import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

final class ReservoirSegmentSampler
{

  /**
   * Use psuedo-random strategy to select a segment from the list of ServerHolder objects provided. Always skips
   * broadcast segments as they do not require moving. It is possible that no segment is selected. In this case,
   * null is returned to caller.
   *
   * @param serverHolders List of {@link ServerHolder} objects that are holding candidate SegmentHolder objects.
   * @param broadcastDatasources Datasources that are broadcast only and should not have segments checked
   * @return null or {@link BalancerSegmentHolder} that holds a segment chosen to be moved.
   */
  @Nullable
  static BalancerSegmentHolder getRandomBalancerSegmentHolder(
      final List<ServerHolder> serverHolders,
      Set<String> broadcastDatasources
  )
  {
    ServerHolder fromServerHolder = null;
    DataSegment proposalSegment = null;
    int numSoFar = 0;

    for (ServerHolder server : serverHolders) {
      if (!server.getServer().getType().isSegmentReplicationTarget()) {
        // if the server only handles broadcast segments (which don't need to be rebalanced), we have nothing to do
        continue;
      }

      for (DataSegment segment : server.getServer().iterateAllSegments()) {
        if (broadcastDatasources.contains(segment.getDataSource())) {
          // we don't need to rebalance segments that were assigned via broadcast rules
          continue;
        }

        int randNum = ThreadLocalRandom.current().nextInt(numSoFar + 1);
        // w.p. 1 / (numSoFar+1), swap out the server and segment
        if (randNum == numSoFar) {
          fromServerHolder = server;
          proposalSegment = segment;
        }
        numSoFar++;
      }
    }
    if (fromServerHolder != null) {
      return new BalancerSegmentHolder(fromServerHolder.getServer(), proposalSegment);
    } else {
      return null;
    }
  }

  /**
   * Iterates over segments on each ServerHolder in serverHolders looking for a segment
   * who lives on less than two guilds. If one is found, that segment is chosen to be moved.
   * If no segment is found, null is returned.
   *
   * @param serverHolders List of {@link ServerHolder} objects that are holding candidate SegmentHolder objects.
   * @param broadcastDatasources Datasources that are broadcast only and should not have segments checked
   * @param params {@link DruidCoordinatorRuntimeParams} used to access {@link ReservoirSegmentSampler}
   * @return null or {@link BalancerSegmentHolder} that holds a segment chosen to be moved.
   */
  @Nullable
  static BalancerSegmentHolder getGuildReplicationViolatorSegmentHolder(
      final List<ServerHolder> serverHolders,
      Set<String> broadcastDatasources,
      DruidCoordinatorRuntimeParams params
  )
  {
    ServerHolder fromServerHolder = null;
    DataSegment proposalSegment = null;

    for (ServerHolder server : serverHolders) {
      if (!server.getServer().getType().isSegmentReplicationTarget()) {
        // if the server only handles broadcast segments (which don't need to be rebalanced), we have nothing to do
        continue;
      }

      for (DataSegment segment : server.getServer().iterateAllSegments()) {
        if (broadcastDatasources.contains(segment.getDataSource())) {
          // we don't need to rebalance segments that were assigned via broadcast rules
          continue;
        }

        if (params.getSegmentReplicantLookup().getGuildSetForSegment(segment.getId()).size() > 1) {
          // we are only looking for segments who are living on less than two guilds
          continue;
        }
        // We found a ServerHolder with a segment that lives on less than two guilds. We will select it and return.
        fromServerHolder = server;
        proposalSegment = segment;
        break;
      }
    }
    if (fromServerHolder != null) {
      return new BalancerSegmentHolder(fromServerHolder.getServer(), proposalSegment);
    } else {
      return null;
    }
  }

  private ReservoirSegmentSampler()
  {
  }
}

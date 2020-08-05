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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

/**
 * A lookup for the number of replicants of a given segment for a certain tier.
 * A lookup for the guildReplicaiton information of a given segment.
 */
public class SegmentReplicantLookup
{
  public static SegmentReplicantLookup make(DruidCluster cluster, boolean makeGuildReplicationDataStructures)
  {
    final Table<SegmentId, String, Integer> segmentsInCluster = HashBasedTable.create();
    final Table<SegmentId, String, Integer> loadingSegments = HashBasedTable.create();
    final Table<SegmentId, String, Integer> historicalGuildDistribution = (makeGuildReplicationDataStructures) ? HashBasedTable.create() : null;

    for (SortedSet<ServerHolder> serversByType : cluster.getSortedHistoricalsByTier()) {
      for (ServerHolder serverHolder : serversByType) {
        ImmutableDruidServer server = serverHolder.getServer();

        for (DataSegment segment : server.iterateAllSegments()) {
          if (makeGuildReplicationDataStructures) {
            Integer numReplicantsOnGuild = historicalGuildDistribution.get(segment.getId(), server.getGuild());
            if (numReplicantsOnGuild == null) {
              numReplicantsOnGuild = 0;
            }
            historicalGuildDistribution.put(segment.getId(), server.getGuild(), numReplicantsOnGuild + 1);
          }

          Integer numReplicants = segmentsInCluster.get(segment.getId(), server.getTier());
          if (numReplicants == null) {
            numReplicants = 0;
          }
          segmentsInCluster.put(segment.getId(), server.getTier(), numReplicants + 1);
        }

        // Also account for queued segments
        for (DataSegment segment : serverHolder.getPeon().getSegmentsToLoad()) {
          if (makeGuildReplicationDataStructures) {
            Integer numReplicantsOnGuild = historicalGuildDistribution.get(segment.getId(), server.getGuild());
            if (numReplicantsOnGuild == null) {
              numReplicantsOnGuild = 0;
            }
            historicalGuildDistribution.put(segment.getId(), server.getGuild(), numReplicantsOnGuild + 1);
          }
          Integer numReplicants = loadingSegments.get(segment.getId(), server.getTier());
          if (numReplicants == null) {
            numReplicants = 0;
          }
          loadingSegments.put(segment.getId(), server.getTier(), numReplicants + 1);
        }
      }
    }

    return new SegmentReplicantLookup(segmentsInCluster, loadingSegments, cluster, historicalGuildDistribution);
  }

  private final Table<SegmentId, String, Integer> segmentsInCluster;
  private final Table<SegmentId, String, Integer> loadingSegments;
  private final Table<SegmentId, String, Integer> historicalGuildDistribution;
  private final DruidCluster cluster;

  private SegmentReplicantLookup(
      Table<SegmentId, String, Integer> segmentsInCluster,
      Table<SegmentId, String, Integer> loadingSegments,
      DruidCluster cluster,
      @Nullable
      Table<SegmentId, String, Integer> historicalGuildDistribution
  )
  {
    this.segmentsInCluster = segmentsInCluster;
    this.loadingSegments = loadingSegments;
    this.cluster = cluster;
    this.historicalGuildDistribution = historicalGuildDistribution;
  }

  public Map<String, Integer> getClusterTiers(SegmentId segmentId)
  {
    Map<String, Integer> retVal = segmentsInCluster.row(segmentId);
    return (retVal == null) ? new HashMap<>() : retVal;
  }

  int getLoadedReplicants(SegmentId segmentId)
  {
    Map<String, Integer> allTiers = segmentsInCluster.row(segmentId);
    int retVal = 0;
    for (Integer replicants : allTiers.values()) {
      retVal += replicants;
    }
    return retVal;
  }

  public int getLoadedReplicants(SegmentId segmentId, String tier)
  {
    Integer retVal = segmentsInCluster.get(segmentId, tier);
    return (retVal == null) ? 0 : retVal;
  }

  private int getLoadingReplicants(SegmentId segmentId, String tier)
  {
    Integer retVal = loadingSegments.get(segmentId, tier);
    return (retVal == null) ? 0 : retVal;
  }

  private int getLoadingReplicants(SegmentId segmentId)
  {
    Map<String, Integer> allTiers = loadingSegments.row(segmentId);
    int retVal = 0;
    for (Integer replicants : allTiers.values()) {
      retVal += replicants;
    }
    return retVal;
  }

  public int getTotalReplicants(SegmentId segmentId)
  {
    return getLoadedReplicants(segmentId) + getLoadingReplicants(segmentId);
  }

  public int getTotalReplicants(SegmentId segmentId, String tier)
  {
    return getLoadedReplicants(segmentId, tier) + getLoadingReplicants(segmentId, tier);
  }

  public Object2LongMap<String> getBroadcastUnderReplication(SegmentId segmentId)
  {
    Object2LongOpenHashMap<String> perTier = new Object2LongOpenHashMap<>();
    for (ServerHolder holder : cluster.getAllServers()) {
      // Only record tier entry for server that is segment broadcast target
      if (holder.getServer().getType().isSegmentBroadcastTarget()) {
        // Every broadcast target server should be serving 1 replica of the segment
        if (!holder.isServingSegment(segmentId)) {
          perTier.addTo(holder.getServer().getTier(), 1L);
        } else {
          perTier.putIfAbsent(holder.getServer().getTier(), 0);
        }
      }
    }
    return perTier;
  }

  /**
   * Return {@link Map} with K:V Pairs of type String:Integer that represent
   * a guild name and the number of replicas on that guild as it pertains to
   * the provided segmentId. If Map for this segment does not exist, return
   * an empty Map.
   *
   * @param segmentId ID of segment to lookup
   * @return {@link Map} that maps gulid name to the number of replicas on that guild.
   */
  public Map<String, Integer> getGuildMapForSegment(SegmentId segmentId)
  {
    if (historicalGuildDistribution == null) {
      return new HashMap<>();
    }
    Map<String, Integer> retVal = historicalGuildDistribution.row(segmentId);
    return (retVal == null) ? new HashMap<>() : retVal;
  }

  /**
   * Return {@link Set} of guild names serving segment. If no guild set exists,
   * return an empty Set.
   *
   * @param segmentId ID of segment we are looking up.
   * @return {@link Set} containing names of guilds serving specified segment.
   */
  public Set<String> getGuildSetForSegment(SegmentId segmentId)
  {
    Map<String, Integer> map = getGuildMapForSegment(segmentId);
    return (map.isEmpty()) ? new HashSet<>() : map.keySet();
  }

}

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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.timeline.DataSegment;

public class LogUsedSegments implements CoordinatorDuty
{
  private static final EmittingLogger log = new EmittingLogger(LogUsedSegments.class);

  public LogUsedSegments()
  {
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    log.debug("Starting coordination. Getting used segments.");

    DataSourcesSnapshot dataSourcesSnapshot = params.getDataSourcesSnapshot();
    for (DataSegment segment : dataSourcesSnapshot.iterateAllUsedSegmentsInSnapshot()) {
      if (segment.getSize() < 0) {
        log.makeAlert("No size on a segment")
           .addData("segment", segment)
           .emit();
      }
    }

    // Log info about all used segments
    if (log.isDebugEnabled()) {
      log.debug("Used Segments");
      for (DataSegment dataSegment : dataSourcesSnapshot.iterateAllUsedSegmentsInSnapshot()) {
        log.debug("  %s", dataSegment);
        // Log the guilds used for each used segment
        if (params.isGuildReplicationEnabled() && params.getSegmentReplicantLookup() != null) {
          log.debug("GuildReplication{"
                    + "segmentId=" + dataSegment.getId()
                    + ", guildSet=" + params.getSegmentReplicantLookup().getGuildSetForSegment(dataSegment.getId())
                    + "}"
          );
        }
      }
    }

    log.info("Found [%,d] used segments.", params.getUsedSegments().size());

    return params;
  }
}

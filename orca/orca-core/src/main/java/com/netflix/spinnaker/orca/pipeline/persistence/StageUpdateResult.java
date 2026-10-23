/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.orca.pipeline.persistence;

import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus;
import javax.annotation.Nullable;

/** Outcome of {@link ExecutionRepository#updateStageIfStatus}. */
public final class StageUpdateResult {
  private final boolean updated;
  @Nullable private final ExecutionStatus storedStatus;

  public StageUpdateResult(boolean updated, @Nullable ExecutionStatus storedStatus) {
    this.updated = updated;
    this.storedStatus = storedStatus;
  }

  /** Whether the updater was applied and the stage stored. */
  public boolean isUpdated() {
    return updated;
  }

  /** The status of the stage as read from the repository, before any update. */
  @Nullable
  public ExecutionStatus getStoredStatus() {
    return storedStatus;
  }

  @Override
  public String toString() {
    return "StageUpdateResult{updated=" + updated + ", storedStatus=" + storedStatus + "}";
  }
}

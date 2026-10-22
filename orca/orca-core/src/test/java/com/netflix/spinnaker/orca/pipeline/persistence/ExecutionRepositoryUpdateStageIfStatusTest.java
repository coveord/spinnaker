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

import static com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.NOT_STARTED;
import static com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.RUNNING;
import static com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.SUCCEEDED;
import static com.netflix.spinnaker.orca.api.pipeline.models.ExecutionType.PIPELINE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus;
import com.netflix.spinnaker.orca.api.pipeline.models.StageExecution;
import com.netflix.spinnaker.orca.pipeline.model.PipelineExecutionImpl;
import com.netflix.spinnaker.orca.pipeline.model.StageExecutionImpl;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class ExecutionRepositoryUpdateStageIfStatusTest {
  private static final Set<ExecutionStatus> NOT_COMPLETE = EnumSet.of(NOT_STARTED, RUNNING);

  private ExecutionRepository repository;
  private PipelineExecutionImpl execution;
  private StageExecutionImpl stage;

  @BeforeEach
  void setUp() {
    repository = spy(new InMemoryExecutionRepository());
    execution = new PipelineExecutionImpl(PIPELINE, "myapp");
    stage = new StageExecutionImpl(execution, "manualJudgment", new HashMap<>());
    execution.getStages().add(stage);
    repository.store(execution);
  }

  @Test
  void appliesTheUpdaterAndStoresTheStageWhenItsStatusIsExpected() {
    stage.setStatus(RUNNING);

    StageUpdateResult result =
        repository.updateStageIfStatus(
            PIPELINE,
            execution.getId(),
            stage.getId(),
            NOT_COMPLETE,
            fresh -> fresh.getContext().put("judgmentStatus", "continue"));

    assertThat(result.isUpdated()).isTrue();
    assertThat(result).extracting(StageUpdateResult::getStoredStatus).isEqualTo(RUNNING);
    assertThat(stage.getContext()).containsEntry("judgmentStatus", "continue");
    verify(repository).storeStage(stage);
  }

  @Test
  void refusesWithoutStoringWhenTheStatusIsNotExpected() {
    stage.setStatus(SUCCEEDED);

    StageUpdateResult result =
        repository.updateStageIfStatus(
            PIPELINE,
            execution.getId(),
            stage.getId(),
            NOT_COMPLETE,
            fresh -> fresh.getContext().put("judgmentStatus", "continue"));

    assertThat(result.isUpdated()).isFalse();
    assertThat(result).extracting(StageUpdateResult::getStoredStatus).isEqualTo(SUCCEEDED);
    assertThat(stage.getContext()).doesNotContainKey("judgmentStatus");
    verify(repository, never()).storeStage(any(StageExecution.class));
  }

  @Test
  void throwsStageNotFoundForAnUnknownStage() {
    assertThatThrownBy(
            () ->
                repository.updateStageIfStatus(
                    PIPELINE, execution.getId(), "unknown-stage", NOT_COMPLETE, fresh -> {}))
        .isInstanceOf(StageNotFoundException.class)
        .hasMessageContaining("unknown-stage");
    verify(repository, never()).storeStage(any(StageExecution.class));
  }

  @Test
  void stageNotFoundIsNotRetryable() {
    assertThat(new StageNotFoundException("execution-id", "stage-id").getRetryable()).isFalse();
  }

  @Test
  void stageNotUpdatableExplainsWhyTheStageCannotBeUpdated() {
    assertThat(new StageNotUpdatableException("stage-id", SUCCEEDED))
        .hasMessage("Stage stage-id is SUCCEEDED; completed stages cannot be updated");
  }
}

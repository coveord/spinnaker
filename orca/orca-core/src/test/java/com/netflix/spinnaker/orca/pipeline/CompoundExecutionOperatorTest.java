/*
 * Copyright 2020 Google, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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

package com.netflix.spinnaker.orca.pipeline;

import static com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.NOT_STARTED;
import static com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.RUNNING;
import static com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.spinnaker.kork.core.RetrySupport;
import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus;
import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionType;
import com.netflix.spinnaker.orca.api.pipeline.models.PipelineExecution;
import com.netflix.spinnaker.orca.api.pipeline.models.StageExecution;
import com.netflix.spinnaker.orca.lock.NoOpRunOnLockAcquired;
import com.netflix.spinnaker.orca.lock.RetriableLock;
import com.netflix.spinnaker.orca.lock.RunOnLockAcquired;
import com.netflix.spinnaker.orca.lock.RunOnLockResult;
import com.netflix.spinnaker.orca.pipeline.model.PipelineExecutionImpl;
import com.netflix.spinnaker.orca.pipeline.model.StageExecutionImpl;
import com.netflix.spinnaker.orca.pipeline.persistence.ExecutionRepository;
import com.netflix.spinnaker.orca.pipeline.persistence.StageNotUpdatableException;
import com.netflix.spinnaker.orca.pipeline.persistence.StageUpdateResult;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

final class CompoundExecutionOperatorTest {
  private static final String APPLICATION = "myapp";
  private static final String PIPELINE = "mypipeline";
  private static final String EXECUTION_ID = "EXECUTION_ID";
  private static final String STAGE_ID = "STAGE_ID";
  private static final String UPSTREAM_STAGE = "UPSTREAM_STAGE";
  private static final String UPSTREAM_STAGE_ID = "UPSTREAM_STAGE_ID";
  private static final int PRECONDITION_STAGE_LIST_SIZE = 2;
  private final ExecutionRepository repository = mock(ExecutionRepository.class);
  private final ExecutionRunner runner = mock(ExecutionRunner.class);
  private final RetrySupport retrySupport = mock(RetrySupport.class);

  private final RetriableLock retriableLock = mock(RetriableLock.class);
  private CompoundExecutionOperator executionOperator =
      new CompoundExecutionOperator(repository, runner, retrySupport, retriableLock);
  private PipelineExecution execution =
      new PipelineExecutionImpl(ExecutionType.PIPELINE, EXECUTION_ID, APPLICATION);

  @Test
  void restartStageWithValidExpression() {

    execution = buildExpectedExecution(execution, true);
    String expression = "'${( #stage(\"Jenkins1\")[\"status\"] matches ''SUCCEEDED|SKIPPED'')";
    Map expectedRestartDetails = buildExpectedRestartDetailsMap(expression);

    List<Map> expectedStageList = (List<Map>) expectedRestartDetails.get("stages");
    Map expectedStageMap = (Map) expectedStageList.get(0);

    when(repository.retrieve(any(), anyString())).thenReturn(execution);
    when(repository.handlesPartition(execution.getPartition())).thenReturn(true);

    PipelineExecution updatedExecution =
        executionOperator.restartStage(EXECUTION_ID, STAGE_ID, expectedRestartDetails);

    List<Map> updatedPreconditionsList =
        (List<Map>) updatedExecution.getStages().get(0).getContext().get("preconditions");
    Map updatedContextMap = (Map) updatedPreconditionsList.get(0).get("context");

    assertEquals(expression, updatedContextMap.get("expression"));
    assertEquals(expectedStageMap.get("type"), updatedExecution.getStages().get(0).getType());
    assertEquals(APPLICATION, updatedExecution.getApplication());
  }

  @Test
  void restartUpstreamStageWithMultiplePreconditionStages() {
    // ARRANGE
    StageExecution upstreamStage = buildUpstreamStage();
    List<StageExecution> preconditionStages = new ArrayList<>(PRECONDITION_STAGE_LIST_SIZE);
    List<StageExecution> executionStages = execution.getStages();
    executionStages.add(upstreamStage);

    for (int i = 0; i < PRECONDITION_STAGE_LIST_SIZE; i++) {
      StageExecution preconditionStage =
          buildPreconditionStage("expression_" + i, "precondition_" + i, "precondition_stage_" + i);
      preconditionStages.add(preconditionStage);
      executionStages.add(preconditionStage);
    }

    Map<String, Object> restartDetails =
        Map.of(
            "application", APPLICATION,
            "name", PIPELINE,
            "executionId", EXECUTION_ID,
            "stages", buildExpectedRestartStageList(preconditionStages, upstreamStage));

    when(repository.retrieve(any(), anyString())).thenReturn(execution);
    when(repository.handlesPartition(execution.getPartition())).thenReturn(true);

    // ACT
    PipelineExecution updatedExecution =
        executionOperator.restartStage(EXECUTION_ID, STAGE_ID, restartDetails);

    // ASSERT
    assertEquals(APPLICATION, updatedExecution.getApplication());
    for (int i = 0, size = preconditionStages.size(); i < size; i++) {
      StageExecution originalPreconditionStage = preconditionStages.get(i);
      StageExecution updatedPreconditionStage = updatedExecution.getStages().get(i + 1);
      assertEquals(originalPreconditionStage.getContext(), updatedPreconditionStage.getContext());
      assertEquals(originalPreconditionStage.getType(), updatedPreconditionStage.getType());
      assertEquals(originalPreconditionStage.getName(), updatedPreconditionStage.getName());
    }
  }

  @Test
  void restartStageWithNoPreconditions() {

    execution = buildExpectedExecution(execution, false);
    Map expectedRestartDetails = buildExpectedRestartDetailsMap(null);

    List<Map> expectedStageList = (List<Map>) expectedRestartDetails.get("stages");
    Map expectedStageMap = (Map) expectedStageList.get(0);

    when(repository.retrieve(any(), anyString())).thenReturn(execution);
    when(repository.handlesPartition(execution.getPartition())).thenReturn(true);

    PipelineExecution updatedExecution =
        executionOperator.restartStage(EXECUTION_ID, STAGE_ID, expectedRestartDetails);

    Map<String, Object> updatedPreconditionsList = updatedExecution.getStages().get(0).getContext();

    assertEquals(null, updatedPreconditionsList.get("preconditions"));
    assertEquals(expectedStageMap.get("type"), updatedExecution.getStages().get(0).getType());
    assertEquals(APPLICATION, updatedExecution.getApplication());
  }

  @Nested
  class UpdateStageIfStatus {
    private final Set<ExecutionStatus> notComplete = EnumSet.of(NOT_STARTED, RUNNING);
    private final Consumer<StageExecution> updater =
        stage -> stage.getContext().put("judgmentStatus", "continue");
    private final RetrySupport realRetrySupport = new RetrySupport();
    private final RetriableLock lockThatRunsTheAction = mock(RetriableLock.class);

    @BeforeEach
    void setUp() {
      when(lockThatRunsTheAction.lock(any(), any()))
          .thenAnswer(
              invocation -> {
                invocation.<Runnable>getArgument(1).run();
                return true;
              });
      when(repository.retrieve(ExecutionType.PIPELINE, EXECUTION_ID)).thenReturn(execution);
    }

    private CompoundExecutionOperator operatorWithLock(RetriableLock lock) {
      return new CompoundExecutionOperator(repository, runner, realRetrySupport, lock);
    }

    private PipelineExecution updateStage(CompoundExecutionOperator operator) {
      return operator.updateStageIfStatus(
          ExecutionType.PIPELINE, EXECUTION_ID, STAGE_ID, notComplete, updater);
    }

    @Test
    void refusedUpdateThrowsStageNotUpdatableWithoutRescheduling() {
      when(repository.updateStageIfStatus(
              ExecutionType.PIPELINE, EXECUTION_ID, STAGE_ID, notComplete, updater))
          .thenReturn(new StageUpdateResult(false, SUCCEEDED));

      assertThatThrownBy(() -> updateStage(operatorWithLock(lockThatRunsTheAction)))
          .isInstanceOf(StageNotUpdatableException.class)
          .hasMessage("Stage STAGE_ID is SUCCEEDED; completed stages cannot be updated");
      verify(runner, never()).reschedule(any());
    }

    @Test
    void acceptedUpdateReschedulesOnceAndReturnsTheExecution() {
      when(repository.updateStageIfStatus(
              ExecutionType.PIPELINE, EXECUTION_ID, STAGE_ID, notComplete, updater))
          .thenReturn(new StageUpdateResult(true, RUNNING));
      when(repository.handlesPartition(execution.getPartition())).thenReturn(true);

      PipelineExecution updated = updateStage(operatorWithLock(lockThatRunsTheAction));

      assertThat(updated).isSameAs(execution);
      verify(runner, times(1)).reschedule(execution);
    }

    @Test
    void acceptedUpdateOfAForeignExecutionDoesNotReschedule() {
      when(repository.updateStageIfStatus(
              ExecutionType.PIPELINE, EXECUTION_ID, STAGE_ID, notComplete, updater))
          .thenReturn(new StageUpdateResult(true, RUNNING));
      when(repository.handlesPartition(execution.getPartition())).thenReturn(false);

      PipelineExecution updated = updateStage(operatorWithLock(lockThatRunsTheAction));

      assertThat(updated).isSameAs(execution);
      verify(runner, never()).reschedule(any());
    }

    @Test
    void repositoryFailurePropagatesThroughTheNoOpLock() {
      RuntimeException failure = new CannotUpdateStage("Cannot manually skip stage.");
      when(repository.updateStageIfStatus(
              ExecutionType.PIPELINE, EXECUTION_ID, STAGE_ID, notComplete, updater))
          .thenThrow(failure);
      RetriableLock noOpLock = new RetriableLock(new NoOpRunOnLockAcquired(), realRetrySupport);

      assertThatThrownBy(() -> updateStage(operatorWithLock(noOpLock))).isSameAs(failure);
      verify(repository, times(1))
          .updateStageIfStatus(
              ExecutionType.PIPELINE, EXECUTION_ID, STAGE_ID, notComplete, updater);
      verify(runner, never()).reschedule(any());
    }

    @Test
    void repositoryFailurePropagatesThroughALockThatReportsTheException() {
      RuntimeException failure = new CannotUpdateStage("Cannot manually skip stage.");
      when(repository.updateStageIfStatus(
              ExecutionType.PIPELINE, EXECUTION_ID, STAGE_ID, notComplete, updater))
          .thenThrow(failure);
      RetriableLock relayingLock =
          new RetriableLock(new LockThatReportsActionFailures(), realRetrySupport);

      assertThatThrownBy(() -> updateStage(operatorWithLock(relayingLock))).isSameAs(failure);
      verify(repository, times(1))
          .updateStageIfStatus(
              ExecutionType.PIPELINE, EXECUTION_ID, STAGE_ID, notComplete, updater);
      verify(runner, never()).reschedule(any());
    }

    @Test
    void lockReportingAcquiredWithoutRunningTheUpdateFailsWithIllegalState() {
      RetriableLock lockThatSkipsTheAction =
          new RetriableLock(new LockThatSkipsTheAction(), realRetrySupport);

      assertThatThrownBy(() -> updateStage(operatorWithLock(lockThatSkipsTheAction)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Stage update for STAGE_ID did not run under the lock");
      verify(runner, never()).reschedule(any());
    }

    @Test
    void updateSucceedsWhenTheLockRunsItAgainAfterAFailure() {
      when(repository.updateStageIfStatus(
              ExecutionType.PIPELINE, EXECUTION_ID, STAGE_ID, notComplete, updater))
          .thenThrow(new CannotUpdateStage("Transient failure."))
          .thenReturn(new StageUpdateResult(true, RUNNING));
      when(repository.handlesPartition(execution.getPartition())).thenReturn(true);
      RetriableLock lockThatRunsTheActionTwice =
          new RetriableLock(new LockThatRunsTheActionTwice(), realRetrySupport);

      PipelineExecution updated = updateStage(operatorWithLock(lockThatRunsTheActionTwice));

      assertThat(updated).isSameAs(execution);
      verify(runner, times(1)).reschedule(execution);
    }

    @Test
    void acceptedUpdateReturnsTheExecutionWhenReschedulingFails() {
      when(repository.updateStageIfStatus(
              ExecutionType.PIPELINE, EXECUTION_ID, STAGE_ID, notComplete, updater))
          .thenReturn(new StageUpdateResult(true, RUNNING));
      when(repository.handlesPartition(execution.getPartition())).thenReturn(true);
      doThrow(new IllegalStateException("Queue unavailable")).when(runner).reschedule(execution);

      PipelineExecution updated = updateStage(operatorWithLock(lockThatRunsTheAction));

      assertThat(updated).isSameAs(execution);
    }

    @Test
    void failureToAcquireTheLockPropagates() {
      RetriableLock unavailableLock = mock(RetriableLock.class);
      when(unavailableLock.lock(any(), any())).thenReturn(false);

      assertThatThrownBy(() -> updateStage(operatorWithLock(unavailableLock)))
          .hasMessageContaining("Failed to acquire lock");
      verify(repository, never())
          .updateStageIfStatus(any(), anyString(), anyString(), any(), any());
      verify(runner, never()).reschedule(any());
    }
  }

  private static final class CannotUpdateStage extends RuntimeException {
    CannotUpdateStage(String message) {
      super(message);
    }
  }

  /** Mirrors a lock that reports acquisition even though it never ran the action. */
  private static final class LockThatSkipsTheAction implements RunOnLockAcquired {
    @Override
    public RunOnLockResult<Void> execute(Runnable action, String keyName) {
      return new RunOnLockResult<>(true, false, null, null);
    }

    @Override
    public <R> RunOnLockResult<R> execute(Callable<R> action, String keyName) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class LockThatRunsTheActionTwice implements RunOnLockAcquired {
    @Override
    public RunOnLockResult<Void> execute(Runnable action, String keyName) {
      action.run();
      action.run();
      return new RunOnLockResult<>(true, true, null, null);
    }

    @Override
    public <R> RunOnLockResult<R> execute(Callable<R> action, String keyName) {
      throw new UnsupportedOperationException();
    }
  }

  /** Mirrors the Redis lock, which reports an exception thrown by the action as lock acquired. */
  private static final class LockThatReportsActionFailures implements RunOnLockAcquired {
    @Override
    public RunOnLockResult<Void> execute(Runnable action, String keyName) {
      try {
        action.run();
        return new RunOnLockResult<>(true, true, null, null);
      } catch (Exception actionFailure) {
        return new RunOnLockResult<>(true, false, actionFailure, null);
      }
    }

    @Override
    public <R> RunOnLockResult<R> execute(Callable<R> action, String keyName) {
      throw new UnsupportedOperationException();
    }
  }

  private PipelineExecution buildExpectedExecution(
      PipelineExecution execution, boolean expression) {
    if (expression) {
      Map<String, Object> contextMap = new HashMap<>();
      List<Map> preconditionList = new ArrayList();
      Map preconditionMap = new HashMap<>();
      Map expressionContextMap = new HashMap<>();

      expressionContextMap.put("expression", expression);
      expressionContextMap.put("failureMessage", "Precondition failed");

      preconditionMap.put("context", expressionContextMap);
      preconditionMap.put("failPipeline", true);
      preconditionMap.put("type", "expression");

      preconditionList.add(preconditionMap);

      contextMap.put("preconditions", preconditionList);

      StageExecution preconditionStage = new StageExecutionImpl();
      preconditionStage.setId(STAGE_ID);
      preconditionStage.setType("checkPreconditions");
      preconditionStage.setName("Check Preconditions");
      preconditionStage.setContext(contextMap);

      execution.getStages().add(preconditionStage);
    } else {
      StageExecution jenkinsStage = new StageExecutionImpl();
      jenkinsStage.setId(STAGE_ID);
      jenkinsStage.setType("jenkins");
      jenkinsStage.setName("Jenkins1");

      execution.getStages().add(jenkinsStage);
    }
    return execution;
  }

  private List<Map<String, Object>> buildExpectedRestartStageList(
      List<StageExecution> preconditionStages, StageExecution upstreamStage) {
    List<Map<String, Object>> restartStageList = new ArrayList<>(preconditionStages.size() + 1);

    Map<String, Object> upstreamStageMap =
        Map.of(
            "name", upstreamStage.getName(),
            "type", upstreamStage.getType());
    restartStageList.add(upstreamStageMap);

    for (StageExecution stage : preconditionStages) {
      Map<String, Object> stageMap =
          Map.of(
              "name", stage.getName(),
              "type", stage.getType(),
              "preconditions", stage.getContext().get("preconditions"));
      restartStageList.add(stageMap);
    }
    return restartStageList;
  }

  private StageExecution buildPreconditionStage(
      String stageStatus, String stageId, String preConditionStageName) {
    StageExecution preconditionStage = new StageExecutionImpl();
    preconditionStage.setId(stageId);
    preconditionStage.setType("checkPreconditions");
    preconditionStage.setName(preConditionStageName);
    Map<String, Object> contextMap = new HashMap<>();
    contextMap.put("stageName", UPSTREAM_STAGE);
    contextMap.put("stageStatus", stageStatus);
    List<Map<String, Object>> preconditionList = new ArrayList<>();
    Map<String, Object> preconditionMap = new HashMap<>();
    preconditionMap.put("context", contextMap);
    preconditionMap.put("failPipeline", true);
    preconditionMap.put("type", "stageStatus");
    preconditionList.add(preconditionMap);
    contextMap.put("preconditions", preconditionList);
    preconditionStage.setContext(contextMap);
    return preconditionStage;
  }

  private StageExecution buildUpstreamStage() {
    StageExecution upstreamStage = new StageExecutionImpl();
    upstreamStage.setId(UPSTREAM_STAGE_ID);
    upstreamStage.setType("manualJudgment");
    upstreamStage.setName(UPSTREAM_STAGE);
    return upstreamStage;
  }

  private Map buildExpectedRestartDetailsMap(String expression) {
    Map restartDetails = new HashMap<>();
    List<Map> pipelineStageList = new ArrayList();

    if (expression != null) {
      Map preconditionStageMap = new HashMap<>();
      List<Map> preconditionList = new ArrayList();
      Map preconditionMap = new HashMap<>();
      Map contextMap = new HashMap<>();

      contextMap.put("expression", expression);
      contextMap.put("failureMessage", "Precondition failed");

      preconditionMap.put("context", contextMap);
      preconditionMap.put("failPipeline", true);
      preconditionMap.put("type", "expression");

      preconditionList.add(preconditionMap);

      preconditionStageMap.put("name", "Check Preconditions");
      preconditionStageMap.put("type", "checkPreconditions");
      preconditionStageMap.put("preconditions", preconditionList);

      pipelineStageList.add(preconditionStageMap);
    } else {
      Map jenkinsStageMap = new HashMap<>();
      jenkinsStageMap.put("name", "Jenkins");
      jenkinsStageMap.put("job", "Jenkins_Job");
      jenkinsStageMap.put("type", "jenkins");

      pipelineStageList.add(jenkinsStageMap);
    }

    restartDetails.put("application", APPLICATION);
    restartDetails.put("name", PIPELINE);
    restartDetails.put("executionId", EXECUTION_ID);
    restartDetails.put("stages", pipelineStageList);

    return restartDetails;
  }
}

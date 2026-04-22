/*
 * Copyright 2017 Netflix, Inc.
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

package com.netflix.spinnaker.orca.q.handler

import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.CANCELED
import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.NOT_STARTED
import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.RUNNING
import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.TERMINAL
import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionType.PIPELINE
import com.netflix.spinnaker.orca.api.test.pipeline
import com.netflix.spinnaker.orca.api.test.stage
import com.netflix.spinnaker.orca.events.ExecutionComplete
import com.netflix.spinnaker.orca.events.ExecutionStarted
import com.netflix.spinnaker.orca.lock.RetriableLock
import com.netflix.spinnaker.orca.pipeline.persistence.ExecutionRepository
import com.netflix.spinnaker.orca.q.CancelExecution
import com.netflix.spinnaker.orca.q.StartExecution
import com.netflix.spinnaker.orca.q.StartStage
import com.netflix.spinnaker.orca.q.StartWaitingExecutions
import com.netflix.spinnaker.orca.q.pending.PendingExecutionService
import com.netflix.spinnaker.orca.q.singleTaskStage
import com.netflix.spinnaker.q.Queue
import com.netflix.spinnaker.spek.and
import com.netflix.spinnaker.time.fixedClock
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.argumentCaptor
import com.nhaarman.mockito_kotlin.check
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.isA
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.never
import com.nhaarman.mockito_kotlin.reset
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.verifyNoMoreInteractions
import com.nhaarman.mockito_kotlin.whenever
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import org.jetbrains.spek.api.lifecycle.CachingMode.GROUP
import org.jetbrains.spek.subject.SubjectSpek
import org.springframework.context.ApplicationEventPublisher
import io.reactivex.rxjava3.core.Observable.just

object StartExecutionHandlerTest : SubjectSpek<StartExecutionHandler>({

  val queue: Queue = mock()
  val repository: ExecutionRepository = mock()
  val pendingExecutionService: PendingExecutionService = mock()
  val publisher: ApplicationEventPublisher = mock()
  val clock = fixedClock()
  val retriableLock: RetriableLock = mock()

  subject(GROUP) {
    StartExecutionHandler(queue, repository, pendingExecutionService, publisher, clock, retriableLock)
  }

  fun resetMocks() = reset(queue, repository, publisher, retriableLock, pendingExecutionService)

  describe("starting an execution") {
    given("a pipeline with a single initial stage") {
      val pipeline = pipeline {
        stage {
          type = singleTaskStage.type
        }
      }
      val message = StartExecution(pipeline)

      beforeGroup {
        whenever(repository.retrieve(message.executionType, message.executionId)) doReturn pipeline
      }

      afterGroup(::resetMocks)

      on("receiving a message") {
        subject.handle(message)
      }

      it("marks the execution as running") {
        assertThat(pipeline.status).isEqualTo(RUNNING)
        verify(repository).updateStatus(pipeline)
      }

      it("starts the first stage") {
        verify(queue).push(StartStage(pipeline.stages.first()))
      }

      it("does not attempt to acquire a lock (no pipelineConfigId)") {
        verify(retriableLock, never()).lock(any(), any())
      }

      it("publishes an event") {
        verify(publisher).publishEvent(
          check<ExecutionStarted> {
            assertThat(it.executionType).isEqualTo(message.executionType)
            assertThat(it.executionId).isEqualTo(message.executionId)
            assertThat(it.execution.startTime).isNotNull()
          }
        )
      }
    }

    given("a pipeline that was previously canceled and status is CANCELED") {
      val pipeline = pipeline {
        stage {
          type = singleTaskStage.type
        }
        status = CANCELED
      }

      val message = StartExecution(pipeline)

      beforeGroup {
        whenever(repository.retrieve(message.executionType, message.executionId)) doReturn pipeline
      }

      afterGroup(::resetMocks)

      on("receiving a message") {
        subject.handle(message)
      }

      it("publishes an event") {
        verify(publisher).publishEvent(
          check<ExecutionComplete> {
            assertThat(it.executionType).isEqualTo(message.executionType)
            assertThat(it.executionId).isEqualTo(message.executionId)
            assertThat(it.status).isEqualTo(CANCELED)
          }
        )
      }

      it("pushes no messages to the queue") {
        verifyNoMoreInteractions(queue)
      }
    }

    given("a pipeline with no pipelineConfigId that was previously canceled and status is NOT_STARTED") {
      val pipeline = pipeline {
        stage {
          type = singleTaskStage.type
        }
        isCanceled = true
      }

      val message = StartExecution(pipeline)

      beforeGroup {
        whenever(repository.retrieve(message.executionType, message.executionId)) doReturn pipeline
      }

      afterGroup(::resetMocks)

      on("receiving a message") {
        subject.handle(message)
      }

      it("publishes an event") {
        verify(publisher).publishEvent(
          check<ExecutionComplete> {
            assertThat(it.executionType).isEqualTo(message.executionType)
            assertThat(it.executionId).isEqualTo(message.executionId)
            assertThat(it.status).isEqualTo(NOT_STARTED)
          }
        )
      }

      it("pushes no messages to the queue") {
        verifyNoMoreInteractions(queue)
      }
    }

    given("a pipeline with a pipelineConfigId that was previously canceled and status is NOT_STARTED") {
      val pipeline = pipeline {
        pipelineConfigId = "aaaaa-12345-bbbbb-67890"
        isKeepWaitingPipelines = false
        stage {
          type = singleTaskStage.type
        }
        isCanceled = true
      }

      val message = StartExecution(pipeline)

      beforeGroup {
        whenever(repository.retrieve(message.executionType, message.executionId)) doReturn pipeline
      }

      afterGroup(::resetMocks)

      on("receiving a message") {
        subject.handle(message)
      }

      it("starts waiting executions for the pipelineConfigId") {
        verify(queue).push(StartWaitingExecutions("aaaaa-12345-bbbbb-67890", true))
      }
    }

    given("a pipeline with multiple initial stages") {
      val pipeline = pipeline {
        stage {
          type = singleTaskStage.type
        }
        stage {
          type = singleTaskStage.type
        }
      }
      val message = StartExecution(pipeline)

      beforeGroup {
        whenever(repository.retrieve(message.executionType, message.executionId)) doReturn pipeline
      }

      afterGroup(::resetMocks)

      on("receiving a message") {
        subject.handle(message)
      }

      it("starts all the initial stages") {
        argumentCaptor<StartStage>().apply {
          verify(queue, times(2)).push(capture())
          assertThat(allValues)
            .extracting("stageId")
            .containsExactlyInAnyOrderElementsOf(pipeline.stages.map { it.id })
        }
      }
    }

    given("a pipeline with no initial stages") {
      val pipeline = pipeline {
        stage {
          type = singleTaskStage.type
          requisiteStageRefIds = listOf("1")
        }
        stage {
          type = singleTaskStage.type
          requisiteStageRefIds = listOf("1")
        }
      }
      val message = StartExecution(pipeline)

      beforeGroup {
        whenever(repository.retrieve(message.executionType, message.executionId)) doReturn pipeline
      }

      afterGroup(::resetMocks)

      on("receiving a message") {
        subject.handle(message)
      }

      it("marks the execution as TERMINAL") {
        assertThat(pipeline.status).isEqualTo(TERMINAL)
        verify(repository, times(1)).updateStatus(pipeline)
      }

      it("publishes an event with TERMINAL status") {
        verify(publisher).publishEvent(
          check<ExecutionComplete> {
            assertThat(it.executionType).isEqualTo(message.executionType)
            assertThat(it.executionId).isEqualTo(message.executionId)
            assertThat(it.status).isEqualTo(TERMINAL)
          }
        )
      }
    }

    given("a start time after ttl") {
      val pipeline = pipeline {
        stage {
          type = singleTaskStage.type
        }
        startTimeExpiry = clock.instant().minusSeconds(30).toEpochMilli()
      }
      val message = StartExecution(pipeline)

      beforeGroup {
        whenever(repository.retrieve(message.executionType, message.executionId)) doReturn pipeline
      }

      afterGroup(::resetMocks)

      action("the handler receives the message") {
        subject.handle(message)
      }

      it("cancels the execution") {
        verify(queue).push(
          CancelExecution(
            pipeline,
            "spinnaker",
            "Could not begin execution before start time expiry"
          )
        )
      }
    }

    given("a pipeline with another instance already running") {
      val configId = UUID.randomUUID().toString()
      val runningPipeline = pipeline {
        pipelineConfigId = configId
        isLimitConcurrent = true
        status = RUNNING
        stage {
          type = singleTaskStage.type
          status = RUNNING
        }
      }
      val pipeline = pipeline {
        pipelineConfigId = configId
        isLimitConcurrent = true
        stage {
          type = singleTaskStage.type
        }
      }
      val message = StartExecution(pipeline.type, pipeline.id, pipeline.application)

      and("the pipeline should not run multiple executions concurrently") {
        beforeGroup {
          setupRetriableLock(true, retriableLock)
          pipeline.isLimitConcurrent = true
          runningPipeline.isLimitConcurrent = true

          whenever(
            repository.retrievePipelinesForPipelineConfigId(eq(configId), any())
          ) doReturn just(runningPipeline)

          whenever(
            repository.retrieve(message.executionType, message.executionId)
          ) doReturn pipeline
        }

        afterGroup(::resetMocks)

        on("receiving a message") {
          subject.handle(message)
        }

        it("does not start the new pipeline") {
          assertThat(pipeline.status).isNotEqualTo(RUNNING)
          verify(repository, never()).updateStatus(pipeline)
          verify(queue, never()).push(isA<StartStage>())
        }

        it("does not push any messages to the queue") {
          verifyNoMoreInteractions(queue)
        }

        it("does not publish any events") {
          verifyNoMoreInteractions(publisher)
        }
      }

      and("the pipeline is allowed to run multiple executions concurrently") {
        beforeGroup {
          setupRetriableLock(true, retriableLock)
          pipeline.isLimitConcurrent = false
          runningPipeline.isLimitConcurrent = false

          whenever(
            repository.retrievePipelinesForPipelineConfigId(eq(configId), any())
          ) doReturn just(runningPipeline)

          whenever(
            repository.retrieve(message.executionType, message.executionId)
          ) doReturn pipeline
        }

        afterGroup(::resetMocks)

        on("receiving a message") {
          subject.handle(message)
        }

        it("starts the new pipeline") {
          assertThat(pipeline.status).isEqualTo(RUNNING)
          verify(repository).updateStatus(pipeline)
          verify(queue).push(isA<StartStage>())
        }
      }

      and("the pipeline is not allowed to run concurrently but the only pipeline already running is the same one") {
        beforeGroup {
          setupRetriableLock(true, retriableLock)
          pipeline.isLimitConcurrent = true
          runningPipeline.isLimitConcurrent = true
          pipeline.status = NOT_STARTED

          whenever(
            repository.retrievePipelinesForPipelineConfigId(eq(configId), any())
          ) doReturn just(pipeline)

          whenever(
            repository.retrieve(message.executionType, message.executionId)
          ) doReturn pipeline
        }

        afterGroup(::resetMocks)

        on("receiving a message") {
          subject.handle(message)
        }

        it("starts the new pipeline") {
          assertThat(pipeline.status).isEqualTo(RUNNING)
          verify(repository).updateStatus(pipeline)
          verify(queue).push(isA<StartStage>())
        }
      }
    }

    given("a pipeline with maxConcurrentExecutions set to 3") {
      val configId = UUID.randomUUID().toString()
      val runningPipeline1 = pipeline {
        pipelineConfigId = configId
        isLimitConcurrent = false
        maxConcurrentExecutions = 3
        status = RUNNING
        stage {
          type = singleTaskStage.type
          status = RUNNING
        }
      }
      val runningPipeline2 = pipeline {
        pipelineConfigId = configId
        isLimitConcurrent = false
        maxConcurrentExecutions = 3
        status = RUNNING
        stage {
          type = singleTaskStage.type
          status = RUNNING
        }
      }
      val runningPipeline3 = pipeline {
        pipelineConfigId = configId
        isLimitConcurrent = false
        maxConcurrentExecutions = 3
        status = RUNNING
        stage {
          type = singleTaskStage.type
          status = RUNNING
        }
      }
      val pipeline = pipeline {
        pipelineConfigId = configId
        isLimitConcurrent = false
        maxConcurrentExecutions = 3
        status = NOT_STARTED
        stage {
          type = singleTaskStage.type
          status = NOT_STARTED
        }
      }
      val message = StartExecution(pipeline.type, pipeline.id, pipeline.application)

      and("the pipeline is allowed to run if no executions are running") {
        beforeGroup {
          setupRetriableLock(true, retriableLock)
          pipeline.status = NOT_STARTED

          whenever(
            repository.retrievePipelinesForPipelineConfigId(eq(configId), any())
          ) doReturn just(pipeline)

          whenever(
            repository.retrieve(message.executionType, message.executionId)
          ) doReturn pipeline
        }

        afterGroup(::resetMocks)

        on("receiving a message") {
          subject.handle(message)
        }

        it("starts the new pipeline") {
          assertThat(pipeline.status).isEqualTo(RUNNING)
          verify(repository).updateStatus(pipeline)
          verify(queue).push(isA<StartStage>())
        }
      }

      and("the pipeline is allowed to run if 1 execution is running") {
        beforeGroup {
          setupRetriableLock(true, retriableLock)
          pipeline.status = NOT_STARTED

          whenever(
            repository.retrievePipelinesForPipelineConfigId(eq(configId), any())
          ) doReturn just(runningPipeline1)

          whenever(
            repository.retrieve(message.executionType, message.executionId)
          ) doReturn pipeline
        }

        afterGroup(::resetMocks)

        on("receiving a message") {
          subject.handle(message)
        }

        it("starts the new pipeline") {
          assertThat(pipeline.status).isEqualTo(RUNNING)
          verify(repository).updateStatus(pipeline)
          verify(queue).push(isA<StartStage>())
        }
      }

      and("the pipeline is allowed to run if 2 executions are running") {
        beforeGroup {
          setupRetriableLock(true, retriableLock)
          pipeline.status = NOT_STARTED

          whenever(
            repository.retrievePipelinesForPipelineConfigId(eq(configId), any())
          ) doReturn just(runningPipeline1,runningPipeline2)

          whenever(
            repository.retrieve(message.executionType, message.executionId)
          ) doReturn pipeline
        }

        afterGroup(::resetMocks)

        on("receiving a message") {
          subject.handle(message)
        }

        it("starts the new pipeline") {
          assertThat(pipeline.status).isEqualTo(RUNNING)
          verify(repository).updateStatus(pipeline)
          verify(queue).push(isA<StartStage>())
        }
      }

      and("the pipeline should not run if 3 executions are running") {
        beforeGroup {
          setupRetriableLock(true, retriableLock)
          pipeline.status = NOT_STARTED

          whenever(
            repository.retrievePipelinesForPipelineConfigId(eq(configId), any())
          ) doReturn just(runningPipeline1,runningPipeline2,runningPipeline3)

          whenever(
            repository.retrieve(message.executionType, message.executionId)
          ) doReturn pipeline
        }

        afterGroup(::resetMocks)

        on("receiving a message") {
          subject.handle(message)
        }

        it("does not start the new pipeline") {
          assertThat(pipeline.status).isNotEqualTo(RUNNING)
          verify(repository, never()).updateStatus(pipeline)
          verify(queue, never()).push(isA<StartStage>())
        }

        it("does not push any messages to the queue") {
          verifyNoMoreInteractions(queue)
        }

        it("does not publish any events") {
          verifyNoMoreInteractions(publisher)
        }
      }

    }

    given("a pipeline with a pipelineConfigId when the lock cannot be acquired") {
      val configId = UUID.randomUUID().toString()
      val pipeline = pipeline {
        pipelineConfigId = configId
        isLimitConcurrent = true
        stage {
          type = singleTaskStage.type
        }
      }
      val message = StartExecution(pipeline)

      beforeGroup {
        setupRetriableLock(false, retriableLock)
        whenever(repository.retrieve(message.executionType, message.executionId)) doReturn pipeline
      }

      afterGroup(::resetMocks)

      on("receiving a message") {
        subject.handle(message)
      }

      it("does not start the pipeline") {
        assertThat(pipeline.status).isNotEqualTo(RUNNING)
        verify(repository, never()).updateStatus(pipeline)
      }

      it("re-pushes the message to the queue") {
        verify(queue).push(isA<StartExecution>())
      }

      it("does not publish any events") {
        verifyNoMoreInteractions(publisher)
      }
    }

    given("a pipeline with a pipelineConfigId acquires lock using the pipelineConfigId as lock name") {
      val configId = UUID.randomUUID().toString()
      val pipeline = pipeline {
        pipelineConfigId = configId
        isLimitConcurrent = true
        stage {
          type = singleTaskStage.type
        }
      }
      val message = StartExecution(pipeline)

      beforeGroup {
        setupRetriableLock(true, retriableLock)
        whenever(repository.retrieve(message.executionType, message.executionId)) doReturn pipeline
        whenever(
          repository.retrievePipelinesForPipelineConfigId(eq(configId), any())
        ) doReturn just(pipeline)
      }

      afterGroup(::resetMocks)

      on("receiving a message") {
        subject.handle(message)
      }

      it("acquires the lock using pipelineConfigId as the lock name") {
        argumentCaptor<RetriableLock.RetriableLockOptions>().apply {
          verify(retriableLock).lock(capture(), any())
          assertThat(firstValue.lockName).isEqualTo(configId)
        }
      }
    }

    given("a pipeline with a pipelineConfigId and shouldQueue true enqueues under lock") {
      val configId = UUID.randomUUID().toString()
      val runningPipeline = pipeline {
        pipelineConfigId = configId
        isLimitConcurrent = true
        status = RUNNING
        stage {
          type = singleTaskStage.type
          status = RUNNING
        }
      }
      val pipeline = pipeline {
        pipelineConfigId = configId
        isLimitConcurrent = true
        stage {
          type = singleTaskStage.type
        }
      }
      val message = StartExecution(pipeline.type, pipeline.id, pipeline.application)

      beforeGroup {
        setupRetriableLock(true, retriableLock)

        whenever(
          repository.retrievePipelinesForPipelineConfigId(eq(configId), any())
        ) doReturn just(runningPipeline)

        whenever(
          repository.retrieve(message.executionType, message.executionId)
        ) doReturn pipeline
      }

      afterGroup(::resetMocks)

      on("receiving a message") {
        subject.handle(message)
      }

      it("enqueues the execution in the pending queue") {
        verify(pendingExecutionService).enqueue(eq(configId), any())
      }

      it("does not start the pipeline") {
        assertThat(pipeline.status).isNotEqualTo(RUNNING)
        verify(repository, never()).updateStatus(pipeline)
      }

      it("does not push any start stage messages") {
        verify(queue, never()).push(isA<StartStage>())
      }
    }
  }
})

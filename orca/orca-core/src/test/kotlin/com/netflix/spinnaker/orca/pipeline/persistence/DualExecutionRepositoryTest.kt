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

package com.netflix.spinnaker.orca.pipeline.persistence

import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.NOT_STARTED
import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionStatus.RUNNING
import com.netflix.spinnaker.orca.api.pipeline.models.ExecutionType.PIPELINE
import com.netflix.spinnaker.orca.api.pipeline.models.StageExecution
import java.util.EnumSet
import java.util.function.Consumer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.never
import org.mockito.Mockito.`when`
import org.springframework.context.ApplicationContext

class DualExecutionRepositoryTest {

  private val primary = mock(ExecutionRepository::class.java)
  private val previous = mock(ExecutionRepository::class.java)
  private val expected = EnumSet.of(NOT_STARTED, RUNNING)
  private val updater = Consumer<StageExecution> { }

  private lateinit var repository: DualExecutionRepository

  @BeforeEach
  fun setUp() {
    val applicationContext = mock(ApplicationContext::class.java)
    `when`(applicationContext.getBean("primary")).thenReturn(primary)
    `when`(applicationContext.getBean("previous")).thenReturn(previous)
    repository = DualExecutionRepository("", "", "primary", "previous", listOf(primary, previous), applicationContext)
  }

  @Test
  fun `updateStageIfStatus delegates to the previous repository when it has the execution`() {
    `when`(previous.hasExecution(PIPELINE, "execution-id")).thenReturn(true)
    `when`(previous.updateStageIfStatus(eq(PIPELINE), eq("execution-id"), eq("stage-id"), eq(expected), any()))
      .thenReturn(StageUpdateResult(true, RUNNING))

    val result = repository.updateStageIfStatus(PIPELINE, "execution-id", "stage-id", expected, updater)

    assertThat(result.isUpdated).isTrue()
    assertThat(result.storedStatus).isEqualTo(RUNNING)
    verify(previous).updateStageIfStatus(PIPELINE, "execution-id", "stage-id", expected, updater)
  }

  @Test
  fun `updateStageIfStatus delegates to the primary repository when it has the execution`() {
    `when`(primary.hasExecution(PIPELINE, "execution-id")).thenReturn(true)
    `when`(primary.updateStageIfStatus(eq(PIPELINE), eq("execution-id"), eq("stage-id"), eq(expected), any()))
      .thenReturn(StageUpdateResult(false, RUNNING))

    val result = repository.updateStageIfStatus(PIPELINE, "execution-id", "stage-id", expected, updater)

    assertThat(result.isUpdated).isFalse()
    verify(primary).updateStageIfStatus(PIPELINE, "execution-id", "stage-id", expected, updater)
    verify(previous, never()).updateStageIfStatus(any(), anyString(), anyString(), any(), any())
  }
}
